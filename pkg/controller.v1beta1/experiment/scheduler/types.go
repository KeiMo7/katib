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

// Package scheduler implements the namespace-wide concurrency budget and the
// deterministic admission order for experiment trials.
//
// All scheduling state is derived from the live Experiment and Trial objects
// in the cluster cache: the package keeps no persistent queue of its own, so a
// controller restart reconstructs exactly the same state from cluster objects.
package scheduler

import (
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
)

const (
	// PriorityAnnotation carries an integer admission priority on an Experiment.
	// Higher values are admitted first. Absent or unparsable values default to 0,
	// preserving the legacy ordering for experiments without the annotation.
	PriorityAnnotation = "scheduler.katib.kubeflow.org/priority"

	// ShareAnnotation overrides the per-experiment share of the namespace
	// running-trial budget. It never raises the effective limit above the
	// experiment's own ParallelTrialCount. Absent values fall back to the
	// ParallelTrialCount, which keeps the single-experiment behavior unchanged.
	ShareAnnotation = "scheduler.katib.kubeflow.org/share"

	// DedupAnnotation opts into duplicate parameter-assignment merging. When
	// enabled, trials in the same experiment carrying the same parameter values
	// (regardless of key order) count as one effective trial and later ones
	// neither consume scheduling slots nor inflate the completion count. It
	// defaults to disabled so existing experiments keep their legacy behavior.
	DedupAnnotation = "scheduler.katib.kubeflow.org/deduplicate-trials"

	// DefaultNamespaceRunningLimit is the conservative upper bound applied when
	// the namespace budget configuration is missing or cannot be read. It must
	// never be interpreted as unlimited concurrency.
	DefaultNamespaceRunningLimit int32 = 3
)

// Reason explains why an admission decision or a budget fallback was taken.
// Reasons are stable identifiers intended for events and status messages; they
// never contain workload data.
type Reason string

const (
	ReasonAdmitted               Reason = "Admitted"
	ReasonNamespaceBudgetBlocked Reason = "NamespaceBudgetExhausted"
	ReasonShareBlocked           Reason = "ExperimentShareExhausted"
	ReasonBorrowedSlot           Reason = "BorrowedIdleShare"
	ReasonYieldedBorrowedSlot    Reason = "YieldedBorrowedShare"
	ReasonStarvationGrant        Reason = "StarvationProtectionGrant"
	ReasonBudgetFallback         Reason = "BudgetConfigUnavailableFallback"
	ReasonWaitingTimeout         Reason = "WaitingTimeout"
	ReasonExperimentGone         Reason = "ExperimentDeleted"
	ReasonExperimentCompleted    Reason = "ExperimentCompleted"
	ReasonDuplicateTrial         Reason = "DuplicateParameterAssignment"
)

// NamespaceBudget is the resolved concurrency budget for one namespace.
type NamespaceBudget struct {
	// Namespace the budget applies to.
	Namespace string
	// MaxRunningTrials is the namespace-wide upper bound on running trials.
	MaxRunningTrials int32
	// Fallback is true when the budget could not be read from configuration and
	// the conservative default was applied instead.
	Fallback bool
}

// ExperimentState is the scheduling-relevant snapshot of one Experiment and its
// current trials. It is rebuilt from cluster objects on every scheduling pass,
// which makes every decision reproducible after a restart.
type ExperimentState struct {
	Experiment *experimentsv1beta1.Experiment
	// Trials currently owned by the experiment, in the order returned by the
	// cache list call.
	Trials []trialsv1beta1.Trial

	// Priority is the resolved admission priority (higher wins).
	Priority int32
	// Share is the resolved per-experiment share of the namespace budget.
	Share int32
	// ParallelTrialCount is the experiment's own parallelism cap.
	ParallelTrialCount int32

	// Active is the number of pending plus running trials.
	Active int32
	// Running is the number of running trials.
	Running int32
	// Pending is the number of pending (created, not yet running) trials.
	Pending int32
	// Completed is the number of terminal trials.
	Completed int32

	// EnqueuedAt is the time the experiment first had an unsatisfied admission
	// demand, used for starvation protection and waiting timeouts.
	EnqueuedAt *metav1.Time
}

// Demand is the number of additional trials the experiment would like to start
// during this scheduling pass, before any budget is applied.
func (s *ExperimentState) Demand() int32 {
	if s.Experiment == nil {
		return 0
	}
	desired := s.ParallelTrialCount
	if s.Experiment.Spec.MaxTrialCount != nil {
		remaining := *s.Experiment.Spec.MaxTrialCount - s.Completed
		if remaining < desired {
			desired = remaining
		}
	}
	if desired <= s.Active {
		return 0
	}
	return desired - s.Active
}

// ResolvePriority extracts the admission priority from experiment annotations,
// defaulting to zero when the annotation is absent or invalid.
func ResolvePriority(instance *experimentsv1beta1.Experiment) int32 {
	if instance == nil || instance.Annotations == nil {
		return 0
	}
	raw, ok := instance.Annotations[PriorityAnnotation]
	if !ok {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0
	}
	return int32(value)
}

// ResolveShare returns the per-experiment share capped by the experiment's own
// ParallelTrialCount. A missing or invalid share annotation falls back to the
// ParallelTrialCount so that experiments without scheduling annotations keep
// the legacy behavior.
func ResolveShare(instance *experimentsv1beta1.Experiment) int32 {
	parallel := int32(0)
	if instance != nil && instance.Spec.ParallelTrialCount != nil {
		parallel = *instance.Spec.ParallelTrialCount
	}
	if instance == nil || instance.Annotations == nil {
		return parallel
	}
	raw, ok := instance.Annotations[ShareAnnotation]
	if !ok {
		return parallel
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || value <= 0 {
		return parallel
	}
	share := int32(value)
	if share > parallel {
		return parallel
	}
	return share
}

// ResolveDedup reports whether duplicate parameter-assignment merging is
// enabled for the experiment. It is opt-in and off by default to preserve the
// legacy admission and completion-count behavior.
func ResolveDedup(instance *experimentsv1beta1.Experiment) bool {
	if instance == nil || instance.Annotations == nil {
		return false
	}
	enabled, err := strconv.ParseBool(instance.Annotations[DedupAnnotation])
	if err != nil {
		return false
	}
	return enabled
}
