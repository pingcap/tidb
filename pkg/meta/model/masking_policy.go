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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// MaskingPolicyStatus is the status of a masking policy.
type MaskingPolicyStatus string

// MaskingPolicyStatus values.
const (
	MaskingPolicyStatusEnabled  MaskingPolicyStatus = "ENABLED"
	MaskingPolicyStatusDisabled MaskingPolicyStatus = "DISABLED"
)

// String implements fmt.Stringer interface.
func (s MaskingPolicyStatus) String() string {
	return string(s)
}

// MaskingPolicyType describes the masking type.
type MaskingPolicyType string

// MaskingPolicyType values.
const (
	MaskingPolicyTypeMaskFull    MaskingPolicyType = "MASK_FULL"
	MaskingPolicyTypeMaskPartial MaskingPolicyType = "MASK_PARTIAL"
	MaskingPolicyTypeMaskNull    MaskingPolicyType = "MASK_NULL"
	MaskingPolicyTypeMaskDate    MaskingPolicyType = "MASK_DATE"
	MaskingPolicyTypeCustom      MaskingPolicyType = "CUSTOM"
)

// MaskingPolicyInfo is the struct to store the masking policy.
type MaskingPolicyInfo struct {
	ID   int64     `json:"id"`
	Name ast.CIStr `json:"name"`
	// SchemaName/TableName/ColumnName are denormalized snapshots used by metadata
	// lookup and observation surfaces.
	SchemaName ast.CIStr `json:"schema_name"`
	TableName  ast.CIStr `json:"table_name"`
	// TableID/ColumnID are the stable bindings and should be treated as source of truth.
	TableID     int64                        `json:"table_id"`
	ColumnID    int64                        `json:"column_id"`
	ColumnName  ast.CIStr                    `json:"column_name"`
	MaskingType MaskingPolicyType            `json:"masking_type,omitempty"`
	Expression  string                       `json:"expression"`
	RestrictOps ast.MaskingPolicyRestrictOps `json:"restrict_ops,omitempty"`
	Status      MaskingPolicyStatus          `json:"status"`
	CreatedAt   time.Time                    `json:"created_at,omitempty"`
	CreatedBy   string                       `json:"created_by,omitempty"`
	UpdatedBy   string                       `json:"updated_by,omitempty"`
	UpdatedAt   time.Time                    `json:"updated_at,omitempty"`
	State       SchemaState                  `json:"state"`
}

// Clone clones MaskingPolicyInfo.
func (p *MaskingPolicyInfo) Clone() *MaskingPolicyInfo {
	if p == nil {
		return nil
	}
	cloned := *p
	return &cloned
}
