// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pkdbspfreshbulk

import "fmt"

// RefinementPlan freezes geometry before the coordinator assigns final task IDs.
// A single child reuses the parent fragments and has no routing centroids.
// The coordinator checks refs and centroids against the authenticated BuildPlan
// and parent; those contextual checks are separate from canonical decoding.
type RefinementPlan struct {
	Plan                 ManifestRef `json:"plan"`
	ParentInput          ManifestRef `json:"parent_input"`
	TargetRows           uint64      `json:"target_rows"`
	MemoryBudgetBytes    uint64      `json:"memory_budget_bytes"`
	ChildCount           uint32      `json:"child_count"`
	CentroidsF32LEBase64 []string    `json:"centroids_f32_le_base64"`
}

func (value RefinementPlan) validate() error {
	if value.TargetRows == 0 || value.MemoryBudgetBytes == 0 || value.ChildCount == 0 || value.CentroidsF32LEBase64 == nil ||
		(value.ChildCount == 1 && len(value.CentroidsF32LEBase64) != 0) ||
		(value.ChildCount > 1 && uint64(len(value.CentroidsF32LEBase64)) != uint64(value.ChildCount)) {
		return fmt.Errorf("invalid frozen refinement layout")
	}
	return nil
}

// EncodeRefinementPlanJSON encodes the current frozen refinement contract.
func EncodeRefinementPlanJSON(value RefinementPlan) ([]byte, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}
	return marshalCanonicalJSON(value)
}

// DecodeRefinementPlanJSON rejects missing, obsolete and noncanonical fields.
func DecodeRefinementPlanJSON(data []byte) (RefinementPlan, error) {
	var value RefinementPlan
	err := decodeCanonicalJSON(data, &value, func() error { return value.validate() })
	return value, err
}
