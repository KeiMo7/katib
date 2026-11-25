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

package experiment

import (
	commonv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/common/v1beta1"
	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	suggestionsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/suggestions/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
	"github.com/kubeflow/katib/pkg/controller.v1beta1/experiment/dedup"
	"github.com/kubeflow/katib/pkg/controller.v1beta1/experiment/scheduler"
)

// dedupResult reports how a candidate assignment batch was merged against the
// trials that already exist in the same experiment.
type dedupResult struct {
	// assignments are the candidates that correspond to genuinely new parameter
	// combinations and may consume a scheduling slot.
	assignments []suggestionsv1beta1.TrialAssignment
	// duplicateNames lists candidate trial names whose parameter combination was
	// already represented by another trial in this experiment. They must not
	// consume a slot or be counted again.
	duplicateNames []string
}

// filterDuplicateAssignments drops candidates whose normalized parameter
// fingerprint matches an existing trial or an earlier candidate in the same
// batch. Fingerprints are independent of parameter key order, so assignments
// differing only in map/slice order are treated as one effective trial.
//
// Merging is opt-in via the experiment's dedup annotation. When disabled every
// candidate is returned unchanged so legacy experiments preserve their exact
// admission and completion-count behavior.
func filterDuplicateAssignments(instance *experimentsv1beta1.Experiment,
	existingTrials []trialsv1beta1.Trial,
	candidates []suggestionsv1beta1.TrialAssignment) dedupResult {

	result := dedupResult{assignments: candidates}
	if !scheduler.ResolveDedup(instance) {
		return result
	}

	known := map[string]bool{}
	for i := range existingTrials {
		fingerprint := dedup.AssignmentFingerprint(existingTrials[i].Spec.ParameterAssignments)
		if fingerprint != "" {
			known[fingerprint] = true
		}
	}

	result.assignments = nil
	for _, candidate := range candidates {
		fingerprint := assignmentFingerprint(candidate.ParameterAssignments)
		if fingerprint != "" && dedup.IsDuplicate(known, fingerprint) {
			result.duplicateNames = append(result.duplicateNames, candidate.Name)
			continue
		}
		if fingerprint != "" {
			known[fingerprint] = true
		}
		result.assignments = append(result.assignments, candidate)
	}
	return result
}

// assignmentFingerprint normalizes suggestion assignments into the same
// order-independent fingerprint used for live trials.
func assignmentFingerprint(assignments []commonv1beta1.ParameterAssignment) string {
	return dedup.AssignmentFingerprint(assignments)
}
