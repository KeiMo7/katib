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

package dedup

import (
	"testing"

	commonv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/common/v1beta1"
)

func TestAssignmentFingerprintOrderIndependent(t *testing.T) {
	a := []commonv1beta1.ParameterAssignment{
		{Name: "max_depth", Value: "6"},
		{Name: "n_estimators", Value: "100"},
		{Name: "n_clusters", Value: "8"},
	}
	b := []commonv1beta1.ParameterAssignment{
		{Name: "n_clusters", Value: "8"},
		{Name: "max_depth", Value: "6"},
		{Name: "n_estimators", Value: "100"},
	}
	if AssignmentFingerprint(a) != AssignmentFingerprint(b) {
		t.Fatal("fingerprints must be independent of assignment order")
	}
}

func TestAssignmentFingerprintDistinguishesValues(t *testing.T) {
	base := []commonv1beta1.ParameterAssignment{
		{Name: "max_depth", Value: "6"},
		{Name: "n_estimators", Value: "100"},
	}
	otherValue := []commonv1beta1.ParameterAssignment{
		{Name: "max_depth", Value: "7"},
		{Name: "n_estimators", Value: "100"},
	}
	otherName := []commonv1beta1.ParameterAssignment{
		{Name: "max_depth", Value: "6"},
		{Name: "n_estimators", Value: "200"},
	}
	fp := AssignmentFingerprint(base)
	if fp == AssignmentFingerprint(otherValue) {
		t.Fatal("different values must produce different fingerprints")
	}
	if fp == AssignmentFingerprint(otherName) {
		t.Fatal("different parameter names must produce different fingerprints")
	}
}

func TestAssignmentFingerprintBoundaryUnambiguous(t *testing.T) {
	// Adjacent name/value pairs must not collapse into the same fingerprint.
	first := []commonv1beta1.ParameterAssignment{
		{Name: "ab", Value: "c"},
	}
	second := []commonv1beta1.ParameterAssignment{
		{Name: "a", Value: "bc"},
	}
	if AssignmentFingerprint(first) == AssignmentFingerprint(second) {
		t.Fatal("name/value boundary must be unambiguous")
	}
}

func TestAssignmentFingerprintEmpty(t *testing.T) {
	if AssignmentFingerprint(nil) != "" {
		t.Fatal("empty assignments must produce an empty fingerprint")
	}
	if IsDuplicate(map[string]bool{}, "") {
		t.Fatal("empty fingerprint must never be treated as duplicate")
	}
}

func TestIsDuplicate(t *testing.T) {
	fp := AssignmentFingerprint([]commonv1beta1.ParameterAssignment{
		{Name: "max_depth", Value: "6"},
	})
	known := map[string]bool{fp: true}
	if !IsDuplicate(known, fp) {
		t.Fatal("existing fingerprint must be detected as duplicate")
	}
	if IsDuplicate(known, AssignmentFingerprint([]commonv1beta1.ParameterAssignment{
		{Name: "max_depth", Value: "9"},
	})) {
		t.Fatal("unknown fingerprint must not be detected as duplicate")
	}
}

func TestFingerprintContainsNoRawValues(t *testing.T) {
	secretValue := "super-secret-parameter-value"
	fp := AssignmentFingerprint([]commonv1beta1.ParameterAssignment{
		{Name: "token", Value: secretValue},
	})
	if contains(fp, secretValue) {
		t.Fatal("fingerprint must not contain raw parameter values")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
