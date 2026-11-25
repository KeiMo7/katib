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

package earlystop

import (
	"sort"
	"sync"

	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
)

// Record is the persisted-equivalent judgment entry for one stopped trial.
// It deliberately stores no raw metric values, credentials or tenant identity:
// only the trial name, the rule direction and the truncation step.
type Record struct {
	TrialName    string
	Comparison   string
	StartStep    int
	ObservedStep int
}

// RecordBook is the single in-memory registry of early stopping judgments,
// keyed by experiment. It makes repeated triggers idempotent within a
// controller process and is rebuilt from cluster Trial objects on startup, so
// verdicts cannot flip across a restart.
type RecordBook struct {
	mu sync.RWMutex
	// records maps experiment key (namespace/name) to the set of trial names
	// already judged in that experiment.
	records map[string]map[string]Record
}

// NewRecordBook returns an empty record book.
func NewRecordBook() *RecordBook {
	return &RecordBook{records: map[string]map[string]Record{}}
}

// Rebuild reconciles the judged-trial set for one experiment with the live
// Trial objects. A trial that already carries the EarlyStopped condition in the
// cluster is added to the set, which guarantees that after a restart the
// rebuilt book matches cluster state and no trial is judged a second time.
//
// Rebuild is additive: it never removes trials already judged in this process,
// because a verdict recorded moments ago may not yet be reflected in the trial
// condition. Entries are removed only through DropExperiment when the
// experiment itself terminates.
func (b *RecordBook) Rebuild(experimentKey string, trials []trialsv1beta1.Trial) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.records[experimentKey]; !ok {
		b.records[experimentKey] = map[string]Record{}
	}
	judged := b.records[experimentKey]
	for i := range trials {
		trial := &trials[i]
		if !trial.IsEarlyStopped() {
			continue
		}
		if _, exists := judged[trial.Name]; !exists {
			judged[trial.Name] = Record{TrialName: trial.Name}
		}
	}
}

// AlreadyJudged reports whether the trial has already received a stop verdict
// for the given experiment.
func (b *RecordBook) AlreadyJudged(experimentKey, trialName string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.records[experimentKey][trialName]
	return ok
}

// Mark records a stop verdict. Marking the same trial twice is a no-op, which
// is what makes the write path idempotent under duplicate reconcile triggers.
func (b *RecordBook) Mark(experimentKey string, record Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.records[experimentKey]; !ok {
		b.records[experimentKey] = map[string]Record{}
	}
	if _, exists := b.records[experimentKey][record.TrialName]; exists {
		return
	}
	b.records[experimentKey][record.TrialName] = record
}

// JudgedSet returns a snapshot set of judged trial names for an experiment.
// The returned map is safe for the caller to mutate.
func (b *RecordBook) JudgedSet(experimentKey string) map[string]bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	source := b.records[experimentKey]
	out := make(map[string]bool, len(source))
	for name := range source {
		out[name] = true
	}
	return out
}

// Records returns a deterministic, name-sorted snapshot of the records for one
// experiment. It is intended for status messages and metrics; it contains no
// workload data.
func (b *RecordBook) Records(experimentKey string) []Record {
	b.mu.RLock()
	defer b.mu.RUnlock()
	source := b.records[experimentKey]
	out := make([]Record, 0, len(source))
	for _, record := range source {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TrialName < out[j].TrialName })
	return out
}

// DropExperiment removes all records for an experiment that is deleted or
// completed, bounding memory growth and matching the quota reclamation path.
func (b *RecordBook) DropExperiment(experimentKey string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.records, experimentKey)
}
