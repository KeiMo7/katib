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
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentSchedulesNeverExceedBudget hammers the pure scheduler from many
// goroutines with the same multi-experiment snapshot. Because Schedule is pure
// and deterministic, every concurrent pass must admit within the namespace
// hard limit and produce the same admission sequence.
func TestConcurrentSchedulesNeverExceedBudget(t *testing.T) {
	base := time.Now()
	states := []*ExperimentState{
		newState("exp-a", 0, 4, 4, 0, 0, base),
		newState("exp-b", 1, 4, 4, 0, 0, base.Add(time.Second)),
		newState("exp-c", 2, 4, 4, 0, 0, base.Add(2*time.Second)),
		newState("exp-d", 0, 4, 4, 0, 0, base.Add(3*time.Second)),
	}
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 6}

	var wg sync.WaitGroup
	results := make(chan *Decision, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- Schedule(budget, states, base.Add(time.Minute))
		}()
	}
	wg.Wait()
	close(results)

	var reference []string
	count := 0
	for decision := range results {
		// Hard invariant: admitted trials never exceed the namespace budget.
		if decision.TotalAdmitted > budget.MaxRunningTrials {
			t.Fatalf("TotalAdmitted = %d exceeds budget %d", decision.TotalAdmitted, budget.MaxRunningTrials)
		}
		// Per-experiment admissions never exceed their share.
		for _, state := range states {
			allocation := decision.Allocations[state.Experiment.Name]
			if allocation.Admit > state.Share {
				t.Fatalf("%s admitted %d beyond share %d",
					state.Experiment.Name, allocation.Admit, state.Share)
			}
		}
		if reference == nil {
			reference = decision.Order
		} else if !equalOrder(reference, decision.Order) {
			t.Fatalf("non deterministic concurrent order: %v vs %v", reference, decision.Order)
		}
		count++
	}
	if count != 100 {
		t.Fatalf("expected 100 decisions, got %d", count)
	}
}

// TestSlotReleaseReusedWithoutLeak verifies that as trials complete across
// repeated passes, freed capacity is admitted again while the running total
// never exceeds the budget and every slot is eventually reused (no leak).
func TestSlotReleaseReusedWithoutLeak(t *testing.T) {
	base := time.Now()
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 4}

	// Pass 1: two experiments each want 4, budget admits 4 total.
	states := []*ExperimentState{
		newState("exp-a", 0, 4, 4, 0, 0, base),
		newState("exp-b", 0, 4, 4, 0, 0, base.Add(time.Second)),
	}
	first := Schedule(budget, states, base.Add(time.Minute))
	if first.TotalAdmitted != 4 {
		t.Fatalf("first pass admitted %d, want 4", first.TotalAdmitted)
	}

	// Pass 2: two trials complete and release their namespace slots. The
	// surviving active count is 2, so the scheduler must readmit exactly 2 to
	// refill the budget without exceeding it.
	expA2 := newState("exp-a", 0, 4, 4, 1, 1, base)
	expB2 := newState("exp-b", 0, 4, 4, 1, 1, base.Add(time.Second))
	states2 := []*ExperimentState{expA2, expB2}
	second := Schedule(budget, states2, base.Add(2*time.Minute))
	if second.NamespaceRunning != 2 {
		t.Fatalf("namespace active baseline = %d, want 2 after releases", second.NamespaceRunning)
	}
	if second.TotalAdmitted != 2 {
		t.Fatalf("second pass admitted %d, want 2 to refill released slots", second.TotalAdmitted)
	}
	peak := second.NamespaceRunning + second.TotalAdmitted
	if peak > budget.MaxRunningTrials {
		t.Fatalf("peak running %d exceeds budget %d", peak, budget.MaxRunningTrials)
	}
}

// TestNoDoubleAdmissionAcrossRepeats verifies that once active trials reach an
// experiment's share, repeated scheduling passes admit zero more (no duplicate
// admission when nothing has changed).
func TestNoDoubleAdmissionAcrossRepeats(t *testing.T) {
	base := time.Now()
	budget := NamespaceBudget{Namespace: "default", MaxRunningTrials: 8}
	states := []*ExperimentState{
		newState("exp-a", 0, 3, 3, 3, 0, base),
		newState("exp-b", 0, 3, 3, 3, 0, base.Add(time.Second)),
	}
	for pass := 0; pass < 5; pass++ {
		decision := Schedule(budget, states, base.Add(time.Minute))
		if decision.TotalAdmitted != 0 {
			t.Fatalf("pass %d admitted %d, want 0 when shares are saturated",
				pass, decision.TotalAdmitted)
		}
	}
}

// TestBudgetFallbackCapsAdmission proves a missing budget never becomes
// unlimited concurrency: the conservative default is the effective cap.
func TestBudgetFallbackCapsAdmission(t *testing.T) {
	base := time.Now()
	fallback := NamespaceBudget{
		Namespace:        "default",
		MaxRunningTrials: DefaultNamespaceRunningLimit,
		Fallback:         true,
	}
	var states []*ExperimentState
	for i := 0; i < 5; i++ {
		states = append(states, newState(fmt.Sprintf("exp-%d", i), 0, 5, 5, 0, 0,
			base.Add(time.Duration(i)*time.Second)))
	}
	decision := Schedule(fallback, states, base.Add(time.Minute))
	if decision.TotalAdmitted != DefaultNamespaceRunningLimit {
		t.Fatalf("fallback admitted %d, want conservative cap %d",
			decision.TotalAdmitted, DefaultNamespaceRunningLimit)
	}
}
