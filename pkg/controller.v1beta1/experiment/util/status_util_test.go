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

package util

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/common/v1beta1"
	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
	"github.com/kubeflow/katib/pkg/controller.v1beta1/consts"
)

const objectiveMetric = "loss"

func summaryExperiment(objType commonv1beta1.ObjectiveType, goal *float64) *experimentsv1beta1.Experiment {
	return &experimentsv1beta1.Experiment{
		ObjectMeta: metav1.ObjectMeta{Name: "exp", Namespace: "default"},
		Spec: experimentsv1beta1.ExperimentSpec{
			Objective: &commonv1beta1.ObjectiveSpec{
				Type:                objType,
				ObjectiveMetricName: objectiveMetric,
				Goal:                goal,
				MetricStrategies: []commonv1beta1.MetricStrategy{
					{Name: objectiveMetric, Value: commonv1beta1.ExtractByLatest},
				},
			},
		},
	}
}

func trialSummary(name, latest string) trialsv1beta1.Trial {
	return trialsv1beta1.Trial{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: trialsv1beta1.TrialSpec{
			Objective: &commonv1beta1.ObjectiveSpec{
				ObjectiveMetricName: objectiveMetric,
				MetricStrategies: []commonv1beta1.MetricStrategy{
					{Name: objectiveMetric, Value: commonv1beta1.ExtractByLatest},
				},
			},
		},
		Status: trialsv1beta1.TrialStatus{
			Observation: &commonv1beta1.Observation{
				Metrics: []commonv1beta1.Metric{
					{Name: objectiveMetric, Min: latest, Max: latest, Latest: latest},
				},
			},
		},
	}
}

func TestTruncatedEarlyStoppedTrialNeverBeatsFullTrial(t *testing.T) {
	// Minimize: an early stopped trial reports a superficially excellent loss,
	// but it is truncated and must not win while a succeeded trial exists.
	instance := summaryExperiment(commonv1beta1.ObjectiveTypeMinimize, nil)

	earlyStopped := trialSummary("truncated", "0.01")
	earlyStopped.Status.Conditions = append(earlyStopped.Status.Conditions,
		trialsv1beta1.TrialCondition{Type: trialsv1beta1.TrialEarlyStopped, Status: "True"})

	full := trialSummary("complete", "0.50")
	full.MarkTrialStatusSucceeded("True", "TrialSucceeded", "done")

	trials := &trialsv1beta1.TrialList{Items: []trialsv1beta1.Trial{earlyStopped, full}}
	updateTrialsSummary(instance, trials)

	if instance.Status.CurrentOptimalTrial.BestTrialName != "complete" {
		t.Fatalf("best trial = %q, want complete (truncated trial must not win)",
			instance.Status.CurrentOptimalTrial.BestTrialName)
	}
	if instance.Status.TrialsEarlyStopped != 1 || instance.Status.TrialsSucceeded != 1 {
		t.Fatalf("status counts earlyStopped=%d succeeded=%d, want 1/1",
			instance.Status.TrialsEarlyStopped, instance.Status.TrialsSucceeded)
	}
}

func TestEarlyStoppedTrialCanWinWhenNoFullTrialExists(t *testing.T) {
	// With no succeeded trial carrying metrics, the truncated observation is
	// still recorded so the experiment has a best-effort optimum.
	instance := summaryExperiment(commonv1beta1.ObjectiveTypeMinimize, nil)

	earlyStopped := trialSummary("truncated", "0.01")
	earlyStopped.Status.Conditions = append(earlyStopped.Status.Conditions,
		trialsv1beta1.TrialCondition{Type: trialsv1beta1.TrialEarlyStopped, Status: "True"})

	trials := &trialsv1beta1.TrialList{Items: []trialsv1beta1.Trial{earlyStopped}}
	updateTrialsSummary(instance, trials)

	if instance.Status.CurrentOptimalTrial.BestTrialName != "truncated" {
		t.Fatalf("best trial = %q, want truncated as best-effort optimum",
			instance.Status.CurrentOptimalTrial.BestTrialName)
	}
}

func TestTruncatedTrialCannotTriggerGoal(t *testing.T) {
	goal := 0.1
	instance := summaryExperiment(commonv1beta1.ObjectiveTypeMinimize, &goal)

	earlyStopped := trialSummary("truncated", "0.01")
	earlyStopped.Status.Conditions = append(earlyStopped.Status.Conditions,
		trialsv1beta1.TrialCondition{Type: trialsv1beta1.TrialEarlyStopped, Status: "True"})

	trials := &trialsv1beta1.TrialList{Items: []trialsv1beta1.Trial{earlyStopped}}
	reached := updateTrialsSummary(instance, trials)
	if reached {
		t.Fatal("truncated early-stopped metric must not trigger goal reachment")
	}
}

func TestFullTrialGoalReachmentUnchanged(t *testing.T) {
	goal := 0.1
	instance := summaryExperiment(commonv1beta1.ObjectiveTypeMinimize, &goal)

	full := trialSummary("complete", "0.05")
	full.MarkTrialStatusSucceeded("True", "TrialSucceeded", "done")
	trials := &trialsv1beta1.TrialList{Items: []trialsv1beta1.Trial{full}}

	reached := updateTrialsSummary(instance, trials)
	if !reached {
		t.Fatal("succeeded trial meeting the goal must trigger goal reachment")
	}
	if instance.Status.CurrentOptimalTrial.BestTrialName != "complete" {
		t.Fatalf("best trial = %q, want complete", instance.Status.CurrentOptimalTrial.BestTrialName)
	}
}

func TestUnavailableMetricExcludedFromBest(t *testing.T) {
	instance := summaryExperiment(commonv1beta1.ObjectiveTypeMaximize, nil)

	unavailable := trialSummary("no-metric", consts.UnavailableMetricValue)
	unavailable.MarkTrialStatusMetricsUnavailable("MetricsUnavailable", "none")
	good := trialSummary("good", "0.9")
	good.MarkTrialStatusSucceeded("True", "TrialSucceeded", "done")

	trials := &trialsv1beta1.TrialList{Items: []trialsv1beta1.Trial{unavailable, good}}
	updateTrialsSummary(instance, trials)

	if instance.Status.CurrentOptimalTrial.BestTrialName != "good" {
		t.Fatalf("best trial = %q, want good", instance.Status.CurrentOptimalTrial.BestTrialName)
	}
	if instance.Status.TrialMetricsUnavailable != 1 {
		t.Fatalf("metrics unavailable count = %d, want 1", instance.Status.TrialMetricsUnavailable)
	}
}
