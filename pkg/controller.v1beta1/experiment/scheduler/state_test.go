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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	configv1beta1 "github.com/kubeflow/katib/pkg/apis/config/v1beta1"
	apis "github.com/kubeflow/katib/pkg/apis/controller"
	commonv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/common/v1beta1"
	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
	"github.com/kubeflow/katib/pkg/controller.v1beta1/consts"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := apis.AddToScheme(scheme); err != nil {
		t.Fatalf("add controller scheme: %v", err)
	}
	if err := configv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add config scheme: %v", err)
	}
	return scheme
}

func fakeExperiment(name, namespace string, parallel int32) *experimentsv1beta1.Experiment {
	return &experimentsv1beta1.Experiment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			CreationTimestamp: metav1.Now(),
		},
		Spec: experimentsv1beta1.ExperimentSpec{
			ParallelTrialCount: &parallel,
			MaxTrialCount:      int32Ptr(10),
			Objective: &commonv1beta1.ObjectiveSpec{
				Type:                commonv1beta1.ObjectiveTypeMinimize,
				ObjectiveMetricName: "loss",
			},
		},
	}
}

func fakeTrial(name, namespace, experimentName string, condition trialsv1beta1.TrialConditionType) trialsv1beta1.Trial {
	trial := trialsv1beta1.Trial{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{consts.LabelExperimentName: experimentName},
		},
		Spec: trialsv1beta1.TrialSpec{
			Objective: &commonv1beta1.ObjectiveSpec{ObjectiveMetricName: "loss"},
		},
	}
	switch condition {
	case trialsv1beta1.TrialRunning:
		trial.MarkTrialStatusRunning("TrialRunning", "running")
	case trialsv1beta1.TrialSucceeded:
		trial.MarkTrialStatusSucceeded("True", "TrialSucceeded", "done")
	case trialsv1beta1.TrialCreated:
		trial.MarkTrialStatusCreated("TrialCreated", "created")
	}
	return trial
}

func TestStateBuilderRebuildsFromClusterObjects(t *testing.T) {
	scheme := testScheme(t)
	namespace := "team-a"
	experiment := fakeExperiment("exp", namespace, 3)
	experiment.Annotations = map[string]string{
		PriorityAnnotation: "7",
		ShareAnnotation:    "2",
	}

	trials := []runtime.Object{
		ptrRuntime(fakeTrial("t-run", namespace, "exp", trialsv1beta1.TrialRunning)),
		ptrRuntime(fakeTrial("t-pending", namespace, "exp", trialsv1beta1.TrialCreated)),
		ptrRuntime(fakeTrial("t-done", namespace, "exp", trialsv1beta1.TrialSucceeded)),
	}
	objects := append([]runtime.Object{experiment}, trials...)
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()

	builder := NewStateBuilder(client)
	states, err := builder.Build(context.Background(), namespace)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("states = %d, want 1", len(states))
	}
	state := states[0]
	if state.Priority != 7 {
		t.Fatalf("priority = %d, want 7", state.Priority)
	}
	if state.Share != 2 {
		t.Fatalf("share = %d, want 2", state.Share)
	}
	if state.Active != 2 || state.Running != 1 || state.Pending != 1 || state.Completed != 1 {
		t.Fatalf("counts active=%d running=%d pending=%d completed=%d, want 2/1/1/1",
			state.Active, state.Running, state.Pending, state.Completed)
	}
}

func TestStateBuilderNamespaceIsolation(t *testing.T) {
	scheme := testScheme(t)
	expA := fakeExperiment("exp-a", "ns-a", 2)
	expB := fakeExperiment("exp-b", "ns-b", 2)
	trialA := ptrRuntime(fakeTrial("ta", "ns-a", "exp-a", trialsv1beta1.TrialRunning))
	trialB := ptrRuntime(fakeTrial("tb", "ns-b", "exp-b", trialsv1beta1.TrialRunning))
	client := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(expA, expB, trialA, trialB).Build()

	builder := NewStateBuilder(client)
	statesA, err := builder.Build(context.Background(), "ns-a")
	if err != nil {
		t.Fatalf("Build ns-a: %v", err)
	}
	if len(statesA) != 1 || statesA[0].Experiment.Name != "exp-a" {
		t.Fatalf("namespace isolation broken: %+v", statesA)
	}
}

func TestLookupNamespaceBudgetFromConfigMap(t *testing.T) {
	scheme := testScheme(t)
	cfg := &configv1beta1.KatibConfig{
		InitConfig: configv1beta1.InitConfig{
			ControllerConfig: configv1beta1.ControllerConfig{
				NamespaceScheduling: []configv1beta1.NamespaceSchedulingConfig{
					{Namespace: "team-a", MaxRunningTrials: 9},
					{Namespace: "", MaxRunningTrials: 5},
				},
			},
		},
	}
	encoded, err := runtimeEncode(scheme, cfg)
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      consts.KatibConfigMapName,
			Namespace: consts.DefaultKatibNamespace,
		},
		Data: map[string]string{consts.LabelKatibConfigTag: encoded},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()

	limit, err := LookupNamespaceBudget(client, "team-a")
	if err != nil || limit != 9 {
		t.Fatalf("team-a budget = %d, err=%v, want 9", limit, err)
	}
	limit, err = LookupNamespaceBudget(client, "unlisted-ns")
	if err != nil || limit != 5 {
		t.Fatalf("fallback budget = %d, err=%v, want 5", limit, err)
	}
}

func TestLookupNamespaceBudgetMissingConfigFailsClosed(t *testing.T) {
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	if _, err := LookupNamespaceBudget(client, "team-a"); err == nil {
		t.Fatal("missing config must return an error so the provider applies a conservative fallback")
	}

	provider := &ConfigBudgetProvider{Lookup: func(_ context.Context, ns string) (int32, error) {
		return LookupNamespaceBudget(client, ns)
	}}
	budget := provider.GetBudget(context.Background(), "team-a")
	if !budget.Fallback || budget.MaxRunningTrials != DefaultNamespaceRunningLimit {
		t.Fatalf("budget = %+v, want conservative fallback", budget)
	}
}
