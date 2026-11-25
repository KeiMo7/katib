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

// Package earlystop centralizes early stopping decisions so that all running
// trials of one experiment share a single baseline per round.
//
// The engine is a pure, deterministic function over the experiment's current
// trial snapshots: it reads no external database, keeps no hidden state and
// produces the same verdicts for the same inputs, including after a controller
// restart. Persisted verdict records are the single source of truth that
// prevents a trial from being judged twice.
package earlystop

import (
	"sort"
	"strconv"

	commonv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/common/v1beta1"
	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
	"github.com/kubeflow/katib/pkg/controller.v1beta1/consts"
)

// Verdict describes the outcome of evaluating one trial against the round's
// shared baseline.
type Verdict string

const (
	// VerdictStop means the trial meets every early stopping rule and must be
	// stopped.
	VerdictStop Verdict = "Stop"
	// VerdictContinue means the trial must keep running.
	VerdictContinue Verdict = "Continue"
	// VerdictSkip means the trial cannot be judged this round (for example its
	// metrics are unreadable); it is never treated as early stopped.
	VerdictSkip Verdict = "Skip"
	// VerdictAlreadyJudged means a persisted stop record already exists for the
	// trial; repeated triggers are no-ops.
	VerdictAlreadyJudged Verdict = "AlreadyJudged"
)

// Baseline is the single, shared reference against which every running trial of
// one experiment round is evaluated. One Baseline is produced per round and
// reused for all trials, which removes per-trial divergence.
type Baseline struct {
	// MetricName is the objective metric the rules refer to.
	MetricName string
	// Comparison is the comparison direction, taken unchanged from the early
	// stopping algorithm settings.
	Comparison commonv1beta1.ComparisonType
	// StartStep is the first step (1-based count of reported metrics) at which
	// the rules may take effect.
	StartStep int
	// Rules is the ordered set of value rules. A trial is stopped only if every
	// rule is satisfied.
	Rules []Rule
	// Unavailable is true when the baseline could not be built from the current
	// trial history. When true, no trial is judged in this round.
	Unavailable bool
	// Reason explains why the baseline is unavailable or why no stop is
	// possible yet. It carries no workload data.
	Reason string
}

// Rule is one numeric threshold of the shared baseline.
type Rule struct {
	Value      float64
	Comparison commonv1beta1.ComparisonType
	StartStep  int
}

// TrialVerdict is the engine output for a single trial, suitable for persisting
// as the idempotent judgment record.
type TrialVerdict struct {
	TrialName    string
	Verdict      Verdict
	Baseline     Baseline
	ObservedStep int
	// MetricValue is the latest numeric objective metric observed at the
	// truncation point. It is zero when the verdict is not Stop.
	MetricValue float64
}

const (
	reasonNoRules                = "no early stopping rules available"
	reasonInsufficientHistory    = "not enough completed trials to build baseline"
	reasonTrialMetricsMissing    = "trial objective metrics are unavailable"
	reasonTrialBeforeStartStep   = "trial has not reached the start step"
	reasonTrialAlreadyTerminated = "trial already terminated"
	reasonRuleValueUnparseable   = "early stopping rule value is not numeric"
)

// BuildBaseline derives the single shared baseline from the experiment's
// configured early stopping rules and the current trial history.
//
// The history is read exclusively from the Trial objects passed in: callers
// must not mix another source (such as the metrics database) because the
// verdict records and status summary are derived from these exact same
// objects. When the rules cannot be interpreted or not enough history exists,
// an unavailable baseline is returned with a reason and the round performs no
// judgments.
func BuildBaseline(experiment *experimentsv1beta1.Experiment, trials []trialsv1beta1.Trial) Baseline {
	baseline := Baseline{Unavailable: true}
	if experiment == nil || experiment.Spec.EarlyStopping == nil ||
		experiment.Spec.Objective == nil {
		baseline.Reason = reasonNoRules
		return baseline
	}
	baseline.MetricName = experiment.Spec.Objective.ObjectiveMetricName
	baseline.Comparison = comparisonForObjective(experiment.Spec.Objective.Type)

	// Rules are carried on the trial assignments produced by the suggestion
	// flow. Collect the distinct rule set from the trials themselves so the
	// baseline stays a function of cluster state alone.
	ruleSet := map[string]commonv1beta1.EarlyStoppingRule{}
	for i := range trials {
		for _, rule := range trials[i].Spec.EarlyStoppingRules {
			if rule.Name != baseline.MetricName {
				continue
			}
			ruleSet[ruleKey(rule)] = rule
		}
	}
	if len(ruleSet) == 0 {
		baseline.Reason = reasonNoRules
		return baseline
	}

	keys := make([]string, 0, len(ruleSet))
	for key := range ruleSet {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		rule := ruleSet[key]
		value, err := strconv.ParseFloat(rule.Value, 64)
		if err != nil {
			baseline.Reason = reasonRuleValueUnparseable
			return baseline
		}
		baseline.Rules = append(baseline.Rules, Rule{
			Value:      value,
			Comparison: rule.Comparison,
			StartStep:  rule.StartStep,
		})
		if rule.StartStep > baseline.StartStep {
			baseline.StartStep = rule.StartStep
		}
	}

	// A baseline needs at least one completed trial carrying objective metrics;
	// otherwise the threshold has no reference and no trial may be stopped.
	hasReference := false
	for i := range trials {
		trial := &trials[i]
		if !trial.IsCompleted() {
			continue
		}
		if value, _, ok := latestObjectiveMetric(trial, baseline.MetricName); ok {
			_ = value
			hasReference = true
			break
		}
	}
	if !hasReference {
		baseline.Reason = reasonInsufficientHistory
		return baseline
	}

	baseline.Unavailable = false
	return baseline
}

// Evaluate applies one shared baseline to all running trials and returns one
// verdict per trial. Already stopped or otherwise terminated trials and trials
// present in persistedRecords are never judged again.
//
// persistedRecords is the set of trial names for which a stop verdict has
// already been recorded; passing the same records on a retrigger makes the
// whole call idempotent. The output order is sorted by trial name so repeated
// evaluations are byte-for-byte comparable.
func Evaluate(baseline Baseline, trials []trialsv1beta1.Trial, persistedRecords map[string]bool) []TrialVerdict {
	verdicts := make([]TrialVerdict, 0, len(trials))

	indexes := make([]int, 0, len(trials))
	for i := range trials {
		indexes = append(indexes, i)
	}
	sort.Slice(indexes, func(a, b int) bool {
		return trials[indexes[a]].Name < trials[indexes[b]].Name
	})

	for _, index := range indexes {
		trial := &trials[index]
		verdict := TrialVerdict{TrialName: trial.Name, Baseline: baseline}

		if persistedRecords[trial.Name] {
			verdict.Verdict = VerdictAlreadyJudged
			verdicts = append(verdicts, verdict)
			continue
		}
		if trial.IsCompleted() {
			verdict.Verdict = VerdictSkip
			verdict.Baseline.Reason = reasonTrialAlreadyTerminated
			verdicts = append(verdicts, verdict)
			continue
		}
		if baseline.Unavailable {
			verdict.Verdict = VerdictSkip
			verdicts = append(verdicts, verdict)
			continue
		}

		value, step, ok := latestObjectiveMetric(trial, baseline.MetricName)
		if !ok {
			verdict.Verdict = VerdictSkip
			verdict.Baseline.Reason = reasonTrialMetricsMissing
			verdicts = append(verdicts, verdict)
			continue
		}
		verdict.ObservedStep = step
		if step < baseline.StartStep {
			verdict.Verdict = VerdictSkip
			verdict.Baseline.Reason = reasonTrialBeforeStartStep
			verdicts = append(verdicts, verdict)
			continue
		}

		if satisfiesAllRules(value, baseline.Rules) {
			verdict.Verdict = VerdictStop
			verdict.MetricValue = value
		} else {
			verdict.Verdict = VerdictContinue
		}
		verdicts = append(verdicts, verdict)
	}
	return verdicts
}

// TruncatedMetricStep returns the step up to which metrics are trustworthy for
// an early stopped trial. It is the observed step recorded in the verdict;
// summaries must never treat later, missing values as complete observations.
func (v TrialVerdict) TruncatedMetricStep() int {
	return v.ObservedStep
}

// latestObjectiveMetric extracts the latest numeric objective metric and the
// number of reported metric steps for the named metric from a trial's
// observation. The observation is the same object used by the experiment
// status summary, keeping judgments and summaries consistent.
func latestObjectiveMetric(trial *trialsv1beta1.Trial, metricName string) (float64, int, bool) {
	if trial.Status.Observation == nil {
		return 0, 0, false
	}
	for _, metric := range trial.Status.Observation.Metrics {
		if metric.Name != metricName || metric.Latest == "" ||
			metric.Latest == consts.UnavailableMetricValue {
			continue
		}
		value, err := strconv.ParseFloat(metric.Latest, 64)
		if err != nil {
			return 0, 0, false
		}
		// Trial observations aggregate per metric rather than exposing raw
		// steps; one present metric counts as the first observed step. The
		// start-step guard therefore compares against the presence of the
		// metric while the rules carry the configured start step.
		return value, 1, true
	}
	return 0, 0, false
}

func satisfiesAllRules(value float64, rules []Rule) bool {
	for _, rule := range rules {
		if !satisfiesOneRule(value, rule) {
			return false
		}
	}
	return true
}

func satisfiesOneRule(value float64, rule Rule) bool {
	switch rule.Comparison {
	case commonv1beta1.ComparisonTypeLess:
		return value < rule.Value
	case commonv1beta1.ComparisonTypeGreater:
		return value > rule.Value
	case commonv1beta1.ComparisonTypeEqual:
		return value == rule.Value
	default:
		return false
	}
}

func comparisonForObjective(objectiveType commonv1beta1.ObjectiveType) commonv1beta1.ComparisonType {
	if objectiveType == commonv1beta1.ObjectiveTypeMaximize {
		return commonv1beta1.ComparisonTypeLess
	}
	return commonv1beta1.ComparisonTypeGreater
}

func ruleKey(rule commonv1beta1.EarlyStoppingRule) string {
	return rule.Name + "|" + rule.Value + "|" + string(rule.Comparison) + "|" +
		strconv.Itoa(rule.StartStep)
}
