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
	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
)

// Coordinator is the single entry point through which the experiment
// controller produces early stopping judgments. Controllers must not call the
// baseline or rule primitives directly: routing every round through Judge keeps
// the baseline, verdict records and later status summary derived from one
// consistent state.
type Coordinator struct {
	book *RecordBook
}

// NewCoordinator returns a Coordinator backed by an empty record book.
func NewCoordinator() *Coordinator {
	return &Coordinator{book: NewRecordBook()}
}

// RecordBook exposes the underlying book so the controller can rebuild it from
// cluster state during startup and drop entries when experiments terminate.
func (c *Coordinator) RecordBook() *RecordBook {
	return c.book
}

// JudgeResult is the outcome of one judging round.
type JudgeResult struct {
	// Baseline is the single shared baseline used for every trial this round.
	Baseline Baseline
	// Verdicts is the per-trial outcome, sorted by trial name.
	Verdicts []TrialVerdict
	// StopTrials is the subset of trials that must be stopped this round.
	StopTrials []TrialVerdict
	// Skipped is the number of trials that could not be judged (missing
	// metrics, before start step, unreadable baseline). Skipped trials keep
	// running and are retried next round.
	Skipped int
}

// Judge runs one complete, idempotent judging round for an experiment.
//
// It first rebuilds the idempotency set from the supplied live trials, builds
// one shared baseline, evaluates every non-terminal trial against that same
// baseline and records the resulting stop verdicts. Calling Judge repeatedly
// with unchanged state returns the same verdicts and never records a trial
// twice.
func (c *Coordinator) Judge(experimentKey string, experiment *experimentsv1beta1.Experiment,
	trials []trialsv1beta1.Trial) JudgeResult {

	// Re-sync the idempotency set with cluster state on every round. This is
	// what makes a restart safe: a trial already marked EarlyStopped in the
	// cluster is present in the book and therefore becomes an idempotent no-op.
	c.book.Rebuild(experimentKey, trials)

	baseline := BuildBaseline(experiment, trials)
	persisted := c.book.JudgedSet(experimentKey)
	verdicts := Evaluate(baseline, trials, persisted)

	result := JudgeResult{Baseline: baseline, Verdicts: verdicts}
	for _, verdict := range verdicts {
		switch verdict.Verdict {
		case VerdictStop:
			c.book.Mark(experimentKey, Record{
				TrialName:    verdict.TrialName,
				Comparison:   string(baseline.Comparison),
				StartStep:    baseline.StartStep,
				ObservedStep: verdict.ObservedStep,
			})
			result.StopTrials = append(result.StopTrials, verdict)
		case VerdictSkip, VerdictAlreadyJudged:
			result.Skipped++
		}
	}
	return result
}

// IsConfigured reports whether the experiment opts into early stopping. Trials
// of experiments without early stopping incur no baseline or history overhead.
func IsConfigured(experiment *experimentsv1beta1.Experiment) bool {
	return experiment != nil && experiment.Spec.EarlyStopping != nil
}
