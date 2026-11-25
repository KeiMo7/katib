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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/common/v1beta1"
	experimentsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/experiments/v1beta1"
	suggestionsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/suggestions/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
	"github.com/kubeflow/katib/pkg/controller.v1beta1/experiment/scheduler"
)

func assignments(a, b string) []commonv1beta1.ParameterAssignment {
	return []commonv1beta1.ParameterAssignment{
		{Name: "max_depth", Value: a},
		{Name: "n_estimators", Value: b},
	}
}

func TestFilterDuplicatesDisabledByDefault(t *testing.T) {
	instance := &experimentsv1beta1.Experiment{ObjectMeta: metav1.ObjectMeta{Name: "exp"}}
	candidates := []suggestionsv1beta1.TrialAssignment{
		{Name: "t1", ParameterAssignments: assignments("6", "100")},
		{Name: "t2", ParameterAssignments: assignments("6", "100")},
	}
	result := filterDuplicateAssignments(instance, nil, candidates)
	if len(result.assignments) != 2 || len(result.duplicateNames) != 0 {
		t.Fatalf("default path must keep all candidates, got %+v", result)
	}
}

func TestFilterDuplicatesMergesSameCombinations(t *testing.T) {
	instance := &experimentsv1beta1.Experiment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "exp",
			Annotations: map[string]string{scheduler.DedupAnnotation: "true"},
		},
	}
	// Existing trial already represents depth=6, n_estimators=100.
	existing := []trialsv1beta1.Trial{
		{Spec: trialsv1beta1.TrialSpec{ParameterAssignments: assignments("6", "100")}},
	}
	candidates := []suggestionsv1beta1.TrialAssignment{
		// Same values as the existing trial, different key order -> duplicate.
		{Name: "dup-of-existing", ParameterAssignments: []commonv1beta1.ParameterAssignment{
			{Name: "n_estimators", Value: "100"},
			{Name: "max_depth", Value: "6"},
		}},
		// New combination.
		{Name: "fresh", ParameterAssignments: assignments("8", "200")},
		// Duplicate of an earlier candidate in the same batch.
		{Name: "dup-in-batch", ParameterAssignments: assignments("8", "200")},
	}
	result := filterDuplicateAssignments(instance, existing, candidates)
	if len(result.assignments) != 1 || result.assignments[0].Name != "fresh" {
		t.Fatalf("only fresh combination should survive, got %+v", result.assignments)
	}
	if len(result.duplicateNames) != 2 {
		t.Fatalf("duplicate names = %v, want 2", result.duplicateNames)
	}
}

func TestFilterDuplicatesInvalidAnnotationFallsBackToLegacy(t *testing.T) {
	instance := &experimentsv1beta1.Experiment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "exp",
			Annotations: map[string]string{scheduler.DedupAnnotation: "not-a-bool"},
		},
	}
	candidates := []suggestionsv1beta1.TrialAssignment{
		{Name: "t1", ParameterAssignments: assignments("6", "100")},
		{Name: "t2", ParameterAssignments: assignments("6", "100")},
	}
	result := filterDuplicateAssignments(instance, nil, candidates)
	if len(result.assignments) != 2 {
		t.Fatalf("invalid annotation must fall back to legacy behavior, got %d", len(result.assignments))
	}
}
