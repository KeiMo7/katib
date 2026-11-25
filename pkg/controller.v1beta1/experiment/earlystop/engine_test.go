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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/common/v1beta1"
	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
	"github.com/kubeflow/katib/pkg/controller.v1beta1/consts"
)

const metricName = "loss"

func metaName(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: "default"}
}

func newExperiment() *experimentsv1beta1.Experiment {
	return &experimentsv1beta1.Experiment{
		Spec: experimentsv1beta1.ExperimentSpec{
			Objective: &commonv1beta1.ObjectiveSpec{
				Type:                commonv1beta1.ObjectiveTypeMinimize,
				ObjectiveMetricName: metricName,
			},
			EarlyStopping: &commonv1beta1.EarlyStoppingSpec{
				AlgorithmName: "medianstop",
				AlgorithmSettings: []commonv1beta1.EarlyStoppingSetting{
					{Name: "min_trials_required", Value: "1"},
					{Name: "start_step", Value: "1"},
				},
			},
		},
	}
}

func trialWithRule(name string, value string, startStep int) trialsv1beta1.Trial {
	return trialsv1beta1.Trial{
		ObjectMeta: metaName(name),
		Spec: trialsv1beta1.TrialSpec{
			Objective: &commonv1beta1.ObjectiveSpec{ObjectiveMetricName: metricName},
			EarlyStoppingRules: []commonv1beta1.EarlyStoppingRule{
				{
					Name:       metricName,
					Value:      value,
					Comparison: commonv1beta1.ComparisonTypeGreater,
					StartStep:  startStep,
				},
			},
		},
	}
}

func withObservation(trial *trialsv1beta1.Trial, latest string) *trialsv1beta1.Trial {
	trial.Status.Observation = &commonv1beta1.Observation{
		Metrics: []commonv1beta1.Metric{
			{Name: metricName, Min: latest, Max: latest, Latest: latest},
		},
	}
	return trial
}

func withUnavailableObservation(trial *trialsv1beta1.Trial) *trialsv1beta1.Trial {
	trial.Status.Observation = &commonv1beta1.Observation{
		Metrics: []commonv1beta1.Metric{
			{Name: metricName, Min: consts.UnavailableMetricValue, Max: consts.UnavailableMetricValue,
				Latest: consts.UnavailableMetricValue},
		},
	}
	return trial
}

func succeeded(trial *trialsv1beta1.Trial) *trialsv1beta1.Trial {
	trial.MarkTrialStatusSucceeded("True", "TrialSucceeded", "done")
	return trial
}

func running(trial *trialsv1beta1.Trial) *trialsv1beta1.Trial {
	trial.MarkTrialStatusRunning("TrialRunning", "running")
	return trial
}

func earlyStopped(trial *trialsv1beta1.Trial) *trialsv1beta1.Trial {
	trial.Status.Conditions = append(trial.Status.Conditions, trialsv1beta1.TrialCondition{
		Type:   trialsv1beta1.TrialEarlyStopped,
		Status: "True",
	})
	return trial
}

func TestBuildBaselineUnavailableWithoutHistory(t *testing.T) {
	experiment := newExperiment()
	// No completed reference trial: baseline must be unavailable.
	trials := []trialsv1beta1.Trial{
		*running(withObservation(ptrTrial(trialWithRule("t1", "0.5", 1)), "0.9")),
	}
	baseline := BuildBaseline(experiment, trials)
	if !baseline.Unavailable || baseline.Reason != reasonInsufficientHistory {
		t.Fatalf("baseline = %+v, want unavailable due to insufficient history", baseline)
	}
}

func TestBuildBaselineUnavailableWhenRulesMissing(t *testing.T) {
	experiment := newExperiment()
	completed := succeeded(withObservation(ptrTrial(trialsv1beta1.Trial{
		ObjectMeta: metaName("ref"),
		Spec:       trialsv1beta1.TrialSpec{Objective: &commonv1beta1.ObjectiveSpec{ObjectiveMetricName: metricName}},
	}), "0.4"))
	// Completed reference exists but carries no early stopping rules.
	baseline := BuildBaseline(experiment, []trialsv1beta1.Trial{*completed})
	if !baseline.Unavailable || baseline.Reason != reasonNoRules {
		t.Fatalf("baseline = %+v, want unavailable due to missing rules", baseline)
	}
}

func TestJudgeSharedBaselineAndMixedHistory(t *testing.T) {
	experiment := newExperiment()
	// One succeeded reference establishes the baseline threshold of 0.5.
	reference := succeeded(withObservation(ptrTrial(trialWithRule("ref", "0.5", 1)), "0.5"))
	// Running trial above the threshold must stop.
	stopCandidate := running(withObservation(ptrTrial(trialWithRule("stop-me", "0.5", 1)), "0.9"))
	// Running trial below the threshold must continue.
	continueCandidate := running(withObservation(ptrTrial(trialWithRule("keep-me", "0.5", 1)), "0.2"))
	// Running trial with unavailable metrics must be skipped, never stopped.
	missing := running(withUnavailableObservation(ptrTrial(trialWithRule("missing", "0.5", 1))))
	// Failed trial must be skipped, never judged.
	failed := ptrTrial(trialWithRule("failed", "0.5", 1))
	failed.MarkTrialStatusFailed("TrialFailed", "boom")
	// Already early stopped trial must be idempotent.
	stopped := earlyStopped(withObservation(ptrTrial(trialWithRule("already", "0.5", 1)), "0.9"))

	trials := []trialsv1beta1.Trial{
		*reference, *stopCandidate, *continueCandidate, *missing, *failed, *stopped,
	}

	coordinator := NewCoordinator()
	result := coordinator.Judge("default/exp", experiment, trials)

	if result.Baseline.Unavailable {
		t.Fatalf("baseline should be available: %+v", result.Baseline)
	}
	if len(result.StopTrials) != 1 || result.StopTrials[0].TrialName != "stop-me" {
		t.Fatalf("stop trials = %+v, want only stop-me", result.StopTrials)
	}
	verdictByName := map[string]Verdict{}
	for _, verdict := range result.Verdicts {
		verdictByName[verdict.TrialName] = verdict.Verdict
	}
	if verdictByName["keep-me"] != VerdictContinue {
		t.Fatalf("keep-me verdict = %q, want Continue", verdictByName["keep-me"])
	}
	if verdictByName["missing"] != VerdictSkip {
		t.Fatalf("missing verdict = %q, want Skip", verdictByName["missing"])
	}
	if verdictByName["failed"] != VerdictSkip {
		t.Fatalf("failed verdict = %q, want Skip", verdictByName["failed"])
	}
	if verdictByName["already"] != VerdictAlreadyJudged {
		t.Fatalf("already verdict = %q, want AlreadyJudged", verdictByName["already"])
	}
}

func TestJudgeIdempotentRepeatedTrigger(t *testing.T) {
	experiment := newExperiment()
	reference := succeeded(withObservation(ptrTrial(trialWithRule("ref", "0.5", 1)), "0.5"))
	candidate := running(withObservation(ptrTrial(trialWithRule("candidate", "0.5", 1)), "0.9"))
	trials := []trialsv1beta1.Trial{*reference, *candidate}

	coordinator := NewCoordinator()
	first := coordinator.Judge("default/exp", experiment, trials)
	if len(first.StopTrials) != 1 {
		t.Fatalf("first round stop count = %d, want 1", len(first.StopTrials))
	}

	// Second trigger with unchanged state must not double count.
	second := coordinator.Judge("default/exp", experiment, trials)
	if len(second.StopTrials) != 0 {
		t.Fatalf("second round stop count = %d, want 0 (idempotent)", len(second.StopTrials))
	}
	if !coordinator.RecordBook().AlreadyJudged("default/exp", "candidate") {
		t.Fatal("candidate must be recorded as judged")
	}
}

func TestJudgeRestartDoesNotFlip(t *testing.T) {
	experiment := newExperiment()
	// Simulate a restart: the candidate now carries the EarlyStopped condition
	// in cluster state. A fresh coordinator rebuilds its book and must not
	// judge it again.
	reference := succeeded(withObservation(ptrTrial(trialWithRule("ref", "0.5", 1)), "0.5"))
	candidate := earlyStopped(running(withObservation(ptrTrial(trialWithRule("candidate", "0.5", 1)), "0.9")))
	trials := []trialsv1beta1.Trial{*reference, *candidate}

	freshCoordinator := NewCoordinator()
	result := freshCoordinator.Judge("default/exp", experiment, trials)
	if len(result.StopTrials) != 0 {
		t.Fatalf("after restart stop count = %d, want 0", len(result.StopTrials))
	}
	if !freshCoordinator.RecordBook().AlreadyJudged("default/exp", "candidate") {
		t.Fatal("rebuilt book must contain the early stopped trial")
	}
}

func TestJudgeDeterministicAcrossRuns(t *testing.T) {
	experiment := newExperiment()
	buildTrials := func() []trialsv1beta1.Trial {
		reference := succeeded(withObservation(ptrTrial(trialWithRule("ref", "0.5", 1)), "0.5"))
		a := running(withObservation(ptrTrial(trialWithRule("a", "0.5", 1)), "0.8"))
		b := running(withObservation(ptrTrial(trialWithRule("b", "0.5", 1)), "0.7"))
		// Deliberately shuffled input order.
		return []trialsv1beta1.Trial{*b, *reference, *a}
	}

	first := NewCoordinator().Judge("ns/e", experiment, buildTrials())
	second := NewCoordinator().Judge("ns/e", experiment, buildTrials())
	if len(first.StopTrials) != len(second.StopTrials) {
		t.Fatal("verdict counts differ across identical runs")
	}
	for i := range first.Verdicts {
		if first.Verdicts[i].TrialName != second.Verdicts[i].TrialName ||
			first.Verdicts[i].Verdict != second.Verdicts[i].Verdict {
			t.Fatalf("non deterministic verdict at %d: %+v vs %+v",
				i, first.Verdicts[i], second.Verdicts[i])
		}
	}
}

func TestJudgeBeforeStartStepSkips(t *testing.T) {
	experiment := newExperiment()
	reference := succeeded(withObservation(ptrTrial(trialWithRule("ref", "0.5", 4)), "0.5"))
	candidate := running(withObservation(ptrTrial(trialWithRule("candidate", "0.5", 4)), "0.9"))
	result := NewCoordinator().Judge("ns/e", experiment,
		[]trialsv1beta1.Trial{*reference, *candidate})
	// Observed step is 1, start step is 4: candidate must keep running.
	if result.Baseline.StartStep != 4 {
		t.Fatalf("start step = %d, want 4", result.Baseline.StartStep)
	}
	for _, verdict := range result.Verdicts {
		if verdict.TrialName == "candidate" && verdict.Verdict != VerdictSkip {
			t.Fatalf("candidate verdict = %q, want Skip before start step", verdict.Verdict)
		}
	}
}

func TestJudgeNoOverheadWhenEarlyStoppingAbsent(t *testing.T) {
	experiment := newExperiment()
	experiment.Spec.EarlyStopping = nil
	if IsConfigured(experiment) {
		t.Fatal("experiment without early stopping must report not configured")
	}
	baseline := BuildBaseline(experiment, nil)
	if !baseline.Unavailable || baseline.Reason != reasonNoRules {
		t.Fatalf("baseline = %+v, want no-rules unavailable", baseline)
	}
}

func TestRecordBookMarkTwiceIsNoop(t *testing.T) {
	book := NewRecordBook()
	book.Mark("ns/e", Record{TrialName: "t1", StartStep: 1})
	book.Mark("ns/e", Record{TrialName: "t1", StartStep: 1})
	records := book.Records("ns/e")
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1 after duplicate mark", len(records))
	}
	book.DropExperiment("ns/e")
	if book.AlreadyJudged("ns/e", "t1") {
		t.Fatal("records must be dropped with the experiment")
	}
}

func ptrTrial(t trialsv1beta1.Trial) *trialsv1beta1.Trial { return &t }
