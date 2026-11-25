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
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
)

// Default timing parameters for starvation protection, borrowing and waiting
// timeouts. They are defined as variables so tests can override them.
var (
	// StarvationThreshold is the maximum time an experiment with unsatisfied
	// demand may wait before it must be admitted, regardless of priority.
	StarvationThreshold = 2 * time.Minute
	// BorrowYieldTimeout bounds how long a borrowing experiment may keep a slot
	// that the share owner needs back.
	BorrowYieldTimeout = 30 * time.Second
	// WaitingTimeout bounds how long an experiment may stay queued before its
	// waiting entry is reclaimed without a trial ever being admitted.
	WaitingTimeout = 10 * time.Minute
)

// Borrow records one slot lent by an experiment whose share is not fully used.
type Borrow struct {
	// Lender is the experiment whose idle share is being used.
	Lender string
	// Borrower is the experiment using the lent slot.
	Borrower string
	// Since marks when the borrow started.
	Since metav1.Time
}

// Allocation is the per-experiment output of one scheduling pass.
type Allocation struct {
	// ExperimentName the allocation belongs to.
	ExperimentName string
	// Admit is the number of new trials the experiment may create in this pass.
	Admit int32
	// Reason explains the admission outcome.
	Reason Reason
	// Borrowed is the number of admitted slots that came from another
	// experiment's idle share.
	Borrowed int32
	// Yielded is the number of borrowed slots the experiment must stop using in
	// new admissions because the owner needs its share back.
	Yielded int32
	// TimedOut reports that the experiment's waiting entry exceeded the waiting
	// timeout and must be reclaimed.
	TimedOut bool
	// ShareInUse is the experiment's current consumption of its own share
	// (active trials counted against its share, excluding slots it lent out).
	ShareInUse int32
}

// Decision is the complete, deterministic result of one scheduling pass over a
// namespace. It is the single source of truth that the controller applies: the
// same input snapshots always produce the same decision.
type Decision struct {
	Namespace string
	Budget    NamespaceBudget
	// Allocations keyed by experiment name.
	Allocations map[string]*Allocation
	// Order is the experiment admission order actually used in this pass.
	Order []string
	// TotalAdmitted is the sum of admitted trials across experiments.
	TotalAdmitted int32
	// NamespaceRunning is the active trial count observed at the start of the
	// pass.
	NamespaceRunning int32
}

// Stats are the observable counters of one scheduling pass, suitable for
// Prometheus gauges and counters. Values carry identifiers only, never workload
// data.
type Stats struct {
	Waiting       map[string]int32
	Running       map[string]int32
	AdmittedTotal map[string]int32
	Borrows       int32
	Yields        int32
	ShareUsage    map[string]int32
}

// Schedule computes the admission decision for all experiments in one
// namespace from their reconstructed states. The function is pure: it mutates
// nothing and returns the same Decision for the same inputs, which is what
// guarantees deterministic admission sequences across requeues and restarts.
func Schedule(budget NamespaceBudget, states []*ExperimentState, now time.Time) *Decision {
	decision := &Decision{
		Namespace:   budget.Namespace,
		Budget:      budget,
		Allocations: map[string]*Allocation{},
	}

	eligible := make([]*ExperimentState, 0, len(states))
	for _, state := range states {
		allocation := &Allocation{
			ExperimentName: state.Experiment.Name,
			Reason:         ReasonNamespaceBudgetBlocked,
			ShareInUse:     state.Active,
		}
		decision.Allocations[state.Experiment.Name] = allocation

		// The namespace running count covers every active trial of every
		// non-completed experiment, including experiments that currently have no
		// additional demand. Otherwise the namespace budget would be
		// under-counted and idle shares could be lent against capacity that is
		// already consumed.
		if !state.Experiment.IsCompleted() {
			decision.NamespaceRunning += state.Active
		}

		// Completed experiments and experiments whose experiment object is gone
		// never admit and always get their entries reclaimed.
		if state.Experiment.IsCompleted() {
			allocation.Reason = ReasonExperimentCompleted
			continue
		}
		if state.Demand() <= 0 {
			allocation.Reason = ReasonAdmitted
			continue
		}
		eligible = append(eligible, state)
	}

	// Deterministic admission order: starvation deadline first, then priority,
	// then submission timestamp, then experiment name as the final tie breaker.
	sort.SliceStable(eligible, func(i, j int) bool {
		return lessState(eligible[i], eligible[j], now)
	})
	for _, state := range eligible {
		decision.Order = append(decision.Order, state.Experiment.Name)
	}

	// Reconstruct share usage and the pool of idle shares that can be lent.
	shareCaps := map[string]int32{}
	shareUsed := map[string]int32{}
	demand := map[string]int32{}
	// lendableByOwner records the share each experiment is allowed to lend in
	// this pass. A share is lendable only when its owner has no unsatisfied
	// demand after filling its own free capacity, so a saturated experiment can
	// never appear to lend slots it is already using.
	lendableByOwner := map[string]int32{}
	for _, state := range states {
		name := state.Experiment.Name
		shareCaps[name] = state.Share
		shareUsed[name] = state.Active
		if state.Experiment.IsCompleted() {
			continue
		}
		free := state.Share - state.Active
		if free <= 0 {
			continue
		}
		ownDemand := state.Demand()
		if ownDemand > 0 {
			// Reserve what the owner itself needs; the residual free share may
			// be lent until the owner asks for it back.
			if lend := free - ownDemand; lend > 0 {
				lendableByOwner[name] = lend
			}
			demand[name] = ownDemand
		} else {
			// The owner is at its own parallelism cap and asks for nothing more.
			// Its free share is genuinely idle and can be lent namespace-wide.
			lendableByOwner[name] = free
		}
	}

	idlePool := int32(0)
	for _, lendable := range lendableByOwner {
		idlePool += lendable
	}

	namespaceFree := budget.MaxRunningTrials - decision.NamespaceRunning
	if namespaceFree < 0 {
		namespaceFree = 0
	}

	for _, state := range eligible {
		name := state.Experiment.Name
		allocation := decision.Allocations[name]

		if timedOut(state, now) {
			allocation.TimedOut = true
			allocation.Reason = ReasonWaitingTimeout
			continue
		}

		// Starvation protection: an experiment that has waited past the
		// threshold gets admitted even if it has to consume idle borrowed
		// capacity. It still can never exceed the namespace hard limit.
		starved := isStarved(state, now)

		ownFree := state.Share - shareUsed[name]
		if ownFree < 0 {
			// Already running beyond its own share; it must rely on borrowing for
			// any new slot and must yield once the owner reclaims capacity.
			ownFree = 0
		}

		want := demand[name]
		admit := int32(0)
		borrowed := int32(0)

		// First consume the experiment's own share while namespace budget and
		// demand allow.
		fromOwn := min32(want, ownFree)
		fromOwn = min32(fromOwn, namespaceFree)
		admit += fromOwn
		shareUsed[name] += fromOwn
		namespaceFree -= fromOwn
		want -= fromOwn

		// Then consume idle lent share.
		if want > 0 && idlePool > 0 {
			fromBorrow := min32(want, idlePool)
			fromBorrow = min32(fromBorrow, namespaceFree)
			admit += fromBorrow
			borrowed += fromBorrow
			idlePool -= fromBorrow
			namespaceFree -= fromBorrow
			want -= fromBorrow
		}

		// Starvation guarantee: the starvation ordering already gives this
		// experiment first access to free capacity. If nothing could be admitted
		// while the namespace hard limit still has room, force one admission over
		// the share order.
		if admit == 0 && starved && namespaceFree > 0 {
			admit = 1
			borrowed++
			namespaceFree--
			want--
		}

		if admit > 0 {
			switch {
			case starved:
				allocation.Reason = ReasonStarvationGrant
			case borrowed > 0:
				allocation.Reason = ReasonBorrowedSlot
			default:
				allocation.Reason = ReasonAdmitted
			}
		}

		allocation.Admit = admit
		allocation.Borrowed = borrowed
		allocation.ShareInUse = shareUsed[name]
		decision.TotalAdmitted += admit
	}

	// Reclaim: a lender that now needs its share back forces borrowing
	// experiments to stop using lent capacity for new admissions. Reclaim only
	// blocks new admissions; it never kills running or finishing trials.
	applyYields(eligible, decision, shareCaps, shareUsed, now)

	return decision
}

// lessState defines the total order used for admission. The order is a total
// order thanks to the name tie breaker, so equal inputs always yield equal
// sequences.
func lessState(a, b *ExperimentState, now time.Time) bool {
	aStarved := isStarved(a, now)
	bStarved := isStarved(b, now)
	if aStarved != bStarved {
		return aStarved
	}
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	aTime := submissionTime(a)
	bTime := submissionTime(b)
	if !aTime.Equal(&bTime) {
		return aTime.Before(&bTime)
	}
	return a.Experiment.Name < b.Experiment.Name
}

// submissionTime returns the time used for same-priority round robin: the
// experiment's creation timestamp, which is assigned by the API server and is
// therefore stable across controller restarts and cache re-lists.
func submissionTime(state *ExperimentState) metav1.Time {
	return state.Experiment.CreationTimestamp
}

func isStarved(state *ExperimentState, now time.Time) bool {
	if state.EnqueuedAt == nil {
		return false
	}
	return now.Sub(state.EnqueuedAt.Time) >= StarvationThreshold
}

func timedOut(state *ExperimentState, now time.Time) bool {
	if state.EnqueuedAt == nil {
		return false
	}
	return now.Sub(state.EnqueuedAt.Time) >= WaitingTimeout
}

// applyYields marks allocations that must give back borrowed capacity. It only
// adjusts future admission: running trials are never terminated.
func applyYields(states []*ExperimentState, decision *Decision, shareCaps, shareUsed map[string]int32, now time.Time) {
	// An experiment needs its share back when its own demand cannot be satisfied
	// from its current free share. Borrowers are then asked to yield in the same
	// deterministic order used for admission.
	for _, owner := range states {
		ownerName := owner.Experiment.Name
		ownerFree := shareCaps[ownerName] - shareUsed[ownerName]
		if ownerFree < 0 {
			ownerFree = 0
		}
		if owner.Demand() <= ownerFree {
			continue
		}
		deficit := owner.Demand() - ownerFree
		for _, borrower := range states {
			if deficit <= 0 {
				break
			}
			borrowerName := borrower.Experiment.Name
			if borrowerName == ownerName {
				continue
			}
			allocation := decision.Allocations[borrowerName]
			if allocation == nil || allocation.Borrowed <= 0 {
				continue
			}
			yield := min32(deficit, allocation.Borrowed)
			allocation.Yielded += yield
			allocation.Borrowed -= yield
			allocation.Admit -= yield
			decision.TotalAdmitted -= yield
			if allocation.Reason != ReasonStarvationGrant {
				allocation.Reason = ReasonYieldedBorrowedSlot
			}
			deficit -= yield
		}
	}
}

// BuildStats derives observable counters from a decision and the input states.
func BuildStats(decision *Decision, states []*ExperimentState) Stats {
	stats := Stats{
		Waiting:       map[string]int32{},
		Running:       map[string]int32{},
		AdmittedTotal: map[string]int32{},
		ShareUsage:    map[string]int32{},
	}
	for _, state := range states {
		name := state.Experiment.Name
		stats.Running[name] = state.Active
		if state.Demand() > 0 && !state.Experiment.IsCompleted() {
			stats.Waiting[name] = state.Demand()
		}
		if allocation, ok := decision.Allocations[name]; ok {
			stats.AdmittedTotal[name] = allocation.Admit
			stats.ShareUsage[name] = allocation.ShareInUse
			stats.Borrows += allocation.Borrowed
			stats.Yields += allocation.Yielded
		}
	}
	return stats
}

// CountActive classifies trials into active (pending/running) and completed
// counts using the trial condition semantics. It is shared by state building so
// that scheduling, status summary and completion accounting use one definition.
func CountActive(trials []trialsv1beta1.Trial) (active, running, pending, completed int32) {
	for i := range trials {
		trial := &trials[i]
		switch {
		case trial.IsCompleted():
			completed++
		case trial.IsRunning():
			active++
			running++
		default:
			active++
			pending++
		}
	}
	return active, running, pending, completed
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
