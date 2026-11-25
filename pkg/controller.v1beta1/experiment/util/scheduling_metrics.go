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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/kubeflow/katib/pkg/controller.v1beta1/experiment/scheduler"
)

// SchedulingCollector exposes the scheduler and early stopping observability
// signals required for operations: waiting and running counts, judged trials,
// borrow/yield counters and per-experiment share usage. Labels carry only
// namespace and experiment identifiers, never workload parameter values.
type SchedulingCollector struct {
	waitingTrials   *prometheus.GaugeVec
	runningTrials   *prometheus.GaugeVec
	shareUsage      *prometheus.GaugeVec
	borrowedSlots   *prometheus.CounterVec
	yieldedSlots    *prometheus.CounterVec
	judgedTrials    *prometheus.CounterVec
	namespaceBudget *prometheus.GaugeVec
}

// NewSchedulingCollector registers and returns the scheduling metrics.
func NewSchedulingCollector(registerer prometheus.Registerer) *SchedulingCollector {
	c := &SchedulingCollector{
		waitingTrials: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "katib_scheduler_waiting_trials",
			Help: "Number of trials an experiment wants to start but has not yet been admitted",
		}, []string{"namespace", "experiment"}),
		runningTrials: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "katib_scheduler_running_trials",
			Help: "Number of active (pending plus running) trials per experiment",
		}, []string{"namespace", "experiment"}),
		shareUsage: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "katib_scheduler_share_in_use",
			Help: "Share of the namespace concurrency budget currently consumed by an experiment",
		}, []string{"namespace", "experiment"}),
		borrowedSlots: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "katib_scheduler_borrowed_slots_total",
			Help: "Total number of slots borrowed from another experiment's idle share",
		}, []string{"namespace"}),
		yieldedSlots: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "katib_scheduler_yielded_slots_total",
			Help: "Total number of borrowed slots yielded back to the share owner",
		}, []string{"namespace"}),
		judgedTrials: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "katib_earlystop_judged_trials_total",
			Help: "Total number of trials judged by the shared early stopping baseline",
		}, []string{"namespace", "experiment", "verdict"}),
		namespaceBudget: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "katib_scheduler_namespace_budget",
			Help: "Configured namespace-wide running trial budget; fallback label indicates conservative default",
		}, []string{"namespace", "fallback"}),
	}
	registerer.MustRegister(c)
	return c
}

// Describe implements prometheus.Collector.
func (c *SchedulingCollector) Describe(ch chan<- *prometheus.Desc) {
	c.waitingTrials.Describe(ch)
	c.runningTrials.Describe(ch)
	c.shareUsage.Describe(ch)
	c.borrowedSlots.Describe(ch)
	c.yieldedSlots.Describe(ch)
	c.judgedTrials.Describe(ch)
	c.namespaceBudget.Describe(ch)
}

// Collect implements prometheus.Collector.
func (c *SchedulingCollector) Collect(ch chan<- prometheus.Metric) {
	c.waitingTrials.Collect(ch)
	c.runningTrials.Collect(ch)
	c.shareUsage.Collect(ch)
	c.borrowedSlots.Collect(ch)
	c.yieldedSlots.Collect(ch)
	c.judgedTrials.Collect(ch)
	c.namespaceBudget.Collect(ch)
}

// RecordDecision publishes the gauges and counters derived from one scheduling
// pass. Gauges are reset per scrape path through the label set; the counters
// accumulate borrow and yield events.
func (c *SchedulingCollector) RecordDecision(namespace string, states []*scheduler.ExperimentState,
	decision *scheduler.Decision) {
	c.waitingTrials.Reset()
	c.runningTrials.Reset()
	c.shareUsage.Reset()

	stats := scheduler.BuildStats(decision, states)
	for _, state := range states {
		name := state.Experiment.Name
		c.runningTrials.WithLabelValues(namespace, name).Set(float64(stats.Running[name]))
		c.shareUsage.WithLabelValues(namespace, name).Set(float64(stats.ShareUsage[name]))
		if waiting, ok := stats.Waiting[name]; ok {
			c.waitingTrials.WithLabelValues(namespace, name).Set(float64(waiting))
		} else {
			c.waitingTrials.WithLabelValues(namespace, name).Set(0)
		}
	}
	if stats.Borrows > 0 {
		c.borrowedSlots.WithLabelValues(namespace).Add(float64(stats.Borrows))
	}
	if stats.Yields > 0 {
		c.yieldedSlots.WithLabelValues(namespace).Add(float64(stats.Yields))
	}

	c.namespaceBudget.Reset()
	fallback := "false"
	if decision.Budget.Fallback {
		fallback = "true"
	}
	c.namespaceBudget.WithLabelValues(namespace, fallback).Set(float64(decision.Budget.MaxRunningTrials))
}

// RecordEarlyStopVerdict increments the judged-trial counter for one verdict.
func (c *SchedulingCollector) RecordEarlyStopVerdict(namespace, experiment, verdict string) {
	c.judgedTrials.WithLabelValues(namespace, experiment, verdict).Inc()
}
