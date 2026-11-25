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
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	commonv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/common/v1beta1"
	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
)

func int32Ptr(v int32) *int32 { return &v }

func newState(name string, priority int32, parallel, share, active, completed int32, created time.Time) *ExperimentState {
	exp := &experimentsv1beta1.Experiment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: experimentsv1beta1.ExperimentSpec{
			ParallelTrialCount: int32Ptr(parallel),
			MaxTrialCount:      int32Ptr(100),
		},
	}
	if priority != 0 {
		exp.Annotations = map[string]string{PriorityAnnotation: "10"}
	}
	if share != parallel {
		if exp.Annotations == nil {
			exp.Annotations = map[string]string{}
		}
		exp.Annotations[ShareAnnotation] = int32Str(share)
	}
	state := &ExperimentState{
		Experiment:         exp,
		Priority:           priority,
		Share:              share,
		ParallelTrialCount: parallel,
		Active:             active,
		Running:            active,
		Completed:          completed,
	}
	return state
}

func int32Str(v int32) string {
	return strconv.Itoa(int(v))
}

func TestResolvePriorityAndShare(t *testing.T) {
	exp := &experimentsv1beta1.Experiment{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				PriorityAnnotation: "5",
				ShareAnnotation:    "2",
			},
		},
		Spec: experimentsv1beta1.ExperimentSpec{
			ParallelTrialCount: int32Ptr(4),
		},
	}
	if got := ResolvePriority(exp); got != 5 {
		t.Fatalf("ResolvePriority = %d, want 5", got)
	}
	if got := ResolveShare(exp); got != 2 {
		t.Fatalf("ResolveShare = %d, want 2", got)
	}

	// Share above ParallelTrialCount is capped.
	exp.Annotations[ShareAnnotation] = "9"
	if got := ResolveShare(exp); got != 4 {
		t.Fatalf("ResolveShare capped = %d, want 4", got)
	}

	// Invalid annotations fall back to defaults without error.
	exp.Annotations[PriorityAnnotation] = "not-a-number"
	exp.Annotations[ShareAnnotation] = "bad"
	if got := ResolvePriority(exp); got != 0 {
		t.Fatalf("ResolvePriority fallback = %d, want 0", got)
	}
	if got := ResolveShare(exp); got != 4 {
		t.Fatalf("ResolveShare fallback = %d, want 4", got)
	}
}

func TestScheduleNamespaceBudgetCapsAdmission(t *testing.T) {
	base := time.Now()
	// Two experiments, each wants 3 active, namespace allows only 4 total.
	states := []*ExperimentState{
		newState("exp-a", 0, 3, 3, 0, 0, base),
		newState("exp-b", 0, 3, 3, 0, 0, base.Add(time.Second)),
	}
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 4}

	decision := Schedule(budget, states, base.Add(time.Minute))

	if decision.TotalAdmitted != 4 {
		t.Fatalf("TotalAdmitted = %d, want 4 (namespace hard limit)", decision.TotalAdmitted)
	}
	if decision.Allocations["exp-a"].Admit != 3 {
		t.Fatalf("exp-a Admit = %d, want 3", decision.Allocations["exp-a"].Admit)
	}
	if decision.Allocations["exp-b"].Admit != 1 {
		t.Fatalf("exp-b Admit = %d, want 1", decision.Allocations["exp-b"].Admit)
	}
	// Same inputs must produce the same order.
	again := Schedule(budget, states, base.Add(time.Minute))
	if !equalOrder(again.Order, decision.Order) {
		t.Fatalf("non deterministic order: %v vs %v", again.Order, decision.Order)
	}
}

func TestSchedulePriorityOrder(t *testing.T) {
	base := time.Now()
	// Lower priority experiment is submitted first, but the high priority
	// experiment must be admitted first when the budget cannot satisfy both.
	low := newState("low", 0, 3, 3, 0, 0, base)
	high := newState("high", 10, 3, 3, 0, 0, base.Add(time.Second))
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 3}

	decision := Schedule(budget, []*ExperimentState{low, high}, base.Add(time.Minute))

	if decision.Order[0] != "high" {
		t.Fatalf("first admission = %q, want high priority", decision.Order[0])
	}
	if decision.Allocations["high"].Admit != 3 {
		t.Fatalf("high Admit = %d, want 3", decision.Allocations["high"].Admit)
	}
	if decision.Allocations["low"].Admit != 0 {
		t.Fatalf("low Admit = %d, want 0", decision.Allocations["low"].Admit)
	}
}

func TestScheduleSamePriorityRoundRobinBySubmission(t *testing.T) {
	base := time.Now()
	first := newState("first", 0, 2, 2, 0, 0, base)
	second := newState("second", 0, 2, 2, 0, 0, base.Add(time.Second))
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 3}

	decision := Schedule(budget, []*ExperimentState{second, first}, base.Add(time.Minute))

	// Input order is deliberately reversed; admission must follow submission.
	if !equalOrder(decision.Order, []string{"first", "second"}) {
		t.Fatalf("order = %v, want [first second]", decision.Order)
	}
	if decision.Allocations["first"].Admit != 2 || decision.Allocations["second"].Admit != 1 {
		t.Fatalf("admission first=%d second=%d, want 2/1",
			decision.Allocations["first"].Admit, decision.Allocations["second"].Admit)
	}
}

func TestScheduleStarvationProtection(t *testing.T) {
	base := time.Now()
	now := base.Add(StarvationThreshold + time.Second)
	// Two shares of 2 fill the namespace budget of 4. The high priority
	// experiment saturates its own share; the starving low priority experiment
	// has demand but no free namespace capacity until a slot is released.
	high := newState("high", 10, 2, 2, 2, 0, base)
	other := newState("other", 5, 2, 2, 2, 0, base.Add(time.Second))
	low := newState("low", 0, 2, 2, 0, 0, base.Add(2*time.Second))
	low.EnqueuedAt = ptr.To(metav1.NewTime(base))
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 4}

	// Before the threshold: no admission for the starving experiment because
	// both the namespace budget and every share are fully used.
	early := Schedule(budget, []*ExperimentState{high, other, low}, base.Add(time.Second))
	if early.Allocations["low"].Admit != 0 {
		t.Fatalf("early low Admit = %d, want 0 before starvation threshold",
			early.Allocations["low"].Admit)
	}

	// After the threshold a slot frees: the starving experiment must win the
	// released slot even though other experiments have higher priority.
	otherFreed := newState("other", 5, 2, 2, 1, 0, base.Add(time.Second))
	decision := Schedule(budget, []*ExperimentState{high, otherFreed, low}, now)

	lowAllocation := decision.Allocations["low"]
	if lowAllocation.Admit != 1 || lowAllocation.Reason != ReasonStarvationGrant {
		t.Fatalf("low allocation = %+v, want 1 starvation grant", lowAllocation)
	}
	if decision.TotalAdmitted != 1 {
		t.Fatalf("TotalAdmitted = %d, want 1", decision.TotalAdmitted)
	}
}

func TestScheduleBorrowIdleShareAndYield(t *testing.T) {
	base := time.Now()
	// Lender share is 4 but only runs 1 trial; borrower can take idle slots.
	lender := newState("lender", 0, 4, 4, 1, 0, base)
	borrower := newState("borrower", 0, 2, 2, 0, 0, base.Add(time.Second))
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 6}

	decision := Schedule(budget, []*ExperimentState{lender, borrower}, base.Add(time.Minute))
	if decision.Allocations["borrower"].Admit != 2 {
		t.Fatalf("borrower Admit = %d, want 2", decision.Allocations["borrower"].Admit)
	}

	// When the lender needs its share back, new borrower admissions are blocked.
	// Rebuild the state as if the lender now wants to fill all 4 slots while the
	// borrower is still asking for more.
	lender2 := newState("lender", 0, 4, 4, 1, 0, base)
	borrower2 := newState("borrower", 0, 2, 2, 2, 0, base.Add(time.Second))
	decision2 := Schedule(budget, []*ExperimentState{lender2, borrower2}, base.Add(time.Minute))
	if decision2.Allocations["lender"].Admit != 3 {
		t.Fatalf("lender Admit = %d, want 3 own slots back", decision2.Allocations["lender"].Admit)
	}
}

func TestScheduleWaitingTimeoutReclaimsEntry(t *testing.T) {
	base := time.Now()
	now := base.Add(WaitingTimeout + time.Second)
	state := newState("exp", 0, 2, 2, 0, 0, base)
	state.EnqueuedAt = ptr.To(metav1.NewTime(base))
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 2}

	decision := Schedule(budget, []*ExperimentState{state}, now)
	allocation := decision.Allocations["exp"]
	if !allocation.TimedOut || allocation.Reason != ReasonWaitingTimeout {
		t.Fatalf("allocation = %+v, want waiting timeout", allocation)
	}
	if allocation.Admit != 0 {
		t.Fatalf("Admit = %d, want 0 after timeout", allocation.Admit)
	}
}

func TestScheduleCompletedAndGoneExperimentsDoNotAdmit(t *testing.T) {
	base := time.Now()
	state := newState("done", 0, 2, 2, 0, 5, base)
	state.Experiment.MarkExperimentStatusSucceeded("ExperimentMaxTrialsReached", "done")
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 2}

	decision := Schedule(budget, []*ExperimentState{state}, base.Add(time.Minute))
	if decision.Allocations["done"].Reason != ReasonExperimentCompleted {
		t.Fatalf("reason = %q, want ExperimentCompleted", decision.Allocations["done"].Reason)
	}
	if decision.TotalAdmitted != 0 {
		t.Fatalf("TotalAdmitted = %d, want 0", decision.TotalAdmitted)
	}
}

func TestBudgetProviderFallbackNeverUnlimited(t *testing.T) {
	// Lookup fails: conservative fallback must be used.
	provider := &ConfigBudgetProvider{Lookup: func(_ context.Context, _ string) (int32, error) {
		return 0, errBudgetUnreadable
	}}
	budget := provider.GetBudget(context.Background(), "default")
	if !budget.Fallback || budget.MaxRunningTrials != DefaultNamespaceRunningLimit {
		t.Fatalf("fallback budget = %+v, want conservative default", budget)
	}

	// Non-positive configured limit also falls back.
	provider.Lookup = func(_ context.Context, _ string) (int32, error) { return 0, nil }
	budget = provider.GetBudget(context.Background(), "default")
	if !budget.Fallback || budget.MaxRunningTrials <= 0 {
		t.Fatalf("zero limit must not be treated as unlimited: %+v", budget)
	}
}

func TestCountActiveUsesTrialConditionSemantics(t *testing.T) {
	trials := []trialsv1beta1.Trial{
		trialWithCondition("t1", trialsv1beta1.TrialRunning),
		trialWithCondition("t2", trialsv1beta1.TrialCreated),
		trialWithCondition("t3", trialsv1beta1.TrialSucceeded),
		trialWithCondition("t4", trialsv1beta1.TrialEarlyStopped),
		trialWithCondition("t5", trialsv1beta1.TrialFailed),
	}
	active, running, pending, completed := CountActive(trials)
	if active != 2 || running != 1 || pending != 1 || completed != 3 {
		t.Fatalf("active=%d running=%d pending=%d completed=%d, want 2/1/1/3",
			active, running, pending, completed)
	}
}

func TestBuildStatsExposesOnlyIdentifiersAndCounts(t *testing.T) {
	base := time.Now()
	states := []*ExperimentState{
		newState("exp-a", 0, 2, 2, 1, 0, base),
		newState("exp-b", 0, 2, 2, 0, 0, base.Add(time.Second)),
	}
	decision := Schedule(NamespaceBudget{Namespace: "default", MaxRunningTrials: 4}, states, base.Add(time.Minute))
	stats := BuildStats(decision, states)
	if stats.Running["exp-a"] != 1 {
		t.Fatalf("running stats = %d, want 1", stats.Running["exp-a"])
	}
	if stats.Waiting["exp-b"] != 2 {
		t.Fatalf("waiting stats = %d, want 2", stats.Waiting["exp-b"])
	}
}

func trialWithCondition(name string, conditionType trialsv1beta1.TrialConditionType) trialsv1beta1.Trial {
	trial := trialsv1beta1.Trial{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: trialsv1beta1.TrialSpec{
			Objective: &commonv1beta1.ObjectiveSpec{ObjectiveMetricName: "accuracy"},
		},
	}
	switch conditionType {
	case trialsv1beta1.TrialSucceeded:
		trial.MarkTrialStatusSucceeded("True", "TrialSucceeded", "ok")
	case trialsv1beta1.TrialFailed:
		trial.MarkTrialStatusFailed("TrialFailed", "ok")
	case trialsv1beta1.TrialRunning:
		trial.MarkTrialStatusRunning("TrialRunning", "ok")
	case trialsv1beta1.TrialCreated:
		trial.MarkTrialStatusCreated("TrialCreated", "ok")
	case trialsv1beta1.TrialEarlyStopped:
		trial.Status.Conditions = append(trial.Status.Conditions, trialsv1beta1.TrialCondition{
			Type:   trialsv1beta1.TrialEarlyStopped,
			Status: "True",
		})
	}
	return trial
}

func equalOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// errBudgetUnreadable is a sentinel error for the provider test.
var errBudgetUnreadable = budgetError("budget unreadable")

type budgetError string

func (e budgetError) Error() string { return string(e) }
