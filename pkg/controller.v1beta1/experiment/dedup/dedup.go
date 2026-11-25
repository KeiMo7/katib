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

// Package dedup provides deterministic identity keys for trial parameter
// assignments so that two assignments carrying the same values in a different
// order are treated as the same effective trial.
package dedup

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	commonv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/common/v1beta1"
)

// AssignmentFingerprint returns a stable, order-independent fingerprint of the
// given parameter assignments. Assignments differing only in slice order or in
// nil-vs-empty produce the same fingerprint. The fingerprint never contains the
// raw parameter values, so it is safe to expose in metrics and events.
func AssignmentFingerprint(assignments []commonv1beta1.ParameterAssignment) string {
	if len(assignments) == 0 {
		return ""
	}
	normalized := make([]commonv1beta1.ParameterAssignment, len(assignments))
	copy(normalized, assignments)
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].Name < normalized[j].Name
	})

	var builder strings.Builder
	for _, assignment := range normalized {
		// Name and value are separated with a byte that cannot appear in the
		// separator position of a subsequent entry; '\x1f' (unit separator) and
		// '\x1e' (record separator) keep name/value boundaries unambiguous.
		builder.WriteString(assignment.Name)
		builder.WriteByte(0x1f)
		builder.WriteString(assignment.Value)
		builder.WriteByte(0x1e)
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(sum[:])
}

// IsDuplicate reports whether the fingerprint is already present in the set of
// known fingerprints. An empty fingerprint is never considered a duplicate so
// that trials without parameter assignments keep the legacy behavior.
func IsDuplicate(known map[string]bool, fingerprint string) bool {
	if fingerprint == "" {
		return false
	}
	return known[fingerprint]
}
