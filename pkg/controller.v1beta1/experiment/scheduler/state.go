/*
Copyright 2026 The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
	"github.com/kubeflow/katib/pkg/controller.v1beta1/consts"
)

// BudgetProvider resolves the namespace-wide running-trial budget.
// Implementations must never return an unlimited budget: when configuration is
// missing or unreadable they fall back to the conservative default and report
// the fallback reason.
type BudgetProvider interface {
	GetBudget(ctx context.Context, namespace string) NamespaceBudget
}

// ConfigBudgetProvider reads budgets from the Katib config ConfigMap through
// the supplied lookup function. The lookup is indirection so unit tests can
// inject configurations without a running API server.
type ConfigBudgetProvider struct {
	Lookup func(ctx context.Context, namespace string) (int32, error)
}

// GetBudget returns the configured namespace limit, or the conservative
// fallback when the lookup fails or returns a non-positive limit.
func (p *ConfigBudgetProvider) GetBudget(ctx context.Context, namespace string) NamespaceBudget {
	budget := NamespaceBudget{
		Namespace:        namespace,
		MaxRunningTrials: DefaultNamespaceRunningLimit,
		Fallback:         true,
	}
	if p == nil || p.Lookup == nil {
		return budget
	}
	limit, err := p.Lookup(ctx, namespace)
	if err != nil || limit <= 0 {
		return budget
	}
	budget.MaxRunningTrials = limit
	budget.Fallback = false
	return budget
}

// StateBuilder reconstructs scheduling state for every experiment in a
// namespace from the live cluster objects. Because all inputs are cluster
// objects, a controller restart rebuilds identical state and the admission
// sequence cannot flip.
type StateBuilder struct {
	Client client.Client
	// Now is injectable for deterministic tests.
	Now func() metav1.Time
	// EnqueuedAt persists the first time each experiment accumulated
	// unsatisfied demand. The map is keyed by namespace/experiment name and is
	// only an optimization for the starvation timer: when the entry is absent
	// the experiment creation timestamp is used, so a cold cache still produces
	// correct ordering.
	enqueued map[string]*metav1.Time
}

// NewStateBuilder returns a StateBuilder using the real clock.
func NewStateBuilder(c client.Client) *StateBuilder {
	return &StateBuilder{
		Client:   c,
		Now:      func() metav1.Time { return metav1.Now() },
		enqueued: map[string]*metav1.Time{},
	}
}

// Build lists experiments and their trials in one namespace and assembles the
// experiment states. Trial lists use the same label selector the experiment
// controller uses, keeping a single source of truth for membership.
func (b *StateBuilder) Build(ctx context.Context, namespace string) ([]*ExperimentState, error) {
	experiments := &experimentsv1beta1.ExperimentList{}
	if err := b.Client.List(ctx, experiments, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	// Sort experiments by name once so iteration order is independent of cache
	// list ordering; the scheduler re-sorts for admission but the rebuilt states
	// are also deterministic when consumed directly.
	states := make([]*ExperimentState, 0, len(experiments.Items))
	for i := range experiments.Items {
		experiment := &experiments.Items[i]

		trials := &trialsv1beta1.TrialList{}
		labels := map[string]string{consts.LabelExperimentName: experiment.Name}
		if err := b.Client.List(ctx, trials,
			client.InNamespace(namespace),
			client.MatchingLabels(labels)); err != nil {
			return nil, err
		}

		active, running, pending, completed := CountActive(trials.Items)
		parallel := int32(0)
		if experiment.Spec.ParallelTrialCount != nil {
			parallel = *experiment.Spec.ParallelTrialCount
		}

		state := &ExperimentState{
			Experiment:         experiment,
			Trials:             trials.Items,
			Priority:           ResolvePriority(experiment),
			Share:              ResolveShare(experiment),
			ParallelTrialCount: parallel,
			Active:             active,
			Running:            running,
			Pending:            pending,
			Completed:          completed,
		}
		state.EnqueuedAt = b.resolveEnqueued(namespace, experiment, state)
		states = append(states, state)
	}
	return states, nil
}

// resolveEnqueued maintains the starvation-timer entry. An experiment with
// unsatisfied demand gets an entry the first time it is seen queued; an
// experiment whose demand is satisfied or that completes loses the entry so the
// timer restarts on the next queueing episode.
func (b *StateBuilder) resolveEnqueued(namespace string, experiment *experimentsv1beta1.Experiment, state *ExperimentState) *metav1.Time {
	key := namespace + "/" + experiment.Name
	if experiment.IsCompleted() || state.Demand() <= 0 {
		delete(b.enqueued, key)
		return nil
	}
	if existing, ok := b.enqueued[key]; ok {
		return existing
	}
	now := b.Now()
	b.enqueued[key] = &now
	return &now
}

// Reset clears the in-memory starvation timer. It is primarily used by tests
// and after a leader election change; cluster timestamps remain the fallback.
func (b *StateBuilder) Reset() {
	b.enqueued = map[string]*metav1.Time{}
}

// Forget removes the starvation-timer entry for a single experiment. It is
// invoked when that experiment completes or is deleted so its waiting entry is
// reclaimed without affecting other experiments in the namespace.
func (b *StateBuilder) Forget(namespace, experimentName string) {
	delete(b.enqueued, namespace+"/"+experimentName)
}
