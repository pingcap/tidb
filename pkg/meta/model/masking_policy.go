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
type MaskingPolicyStatus byte

// MaskingPolicyStatus values.
const (
	MaskingPolicyStatusDisable MaskingPolicyStatus = iota
	MaskingPolicyStatusEnable

	// Keep the old names as aliases for compatibility in incremental phase PRs.
	MaskingPolicyStatusDisabled = MaskingPolicyStatusDisable
	MaskingPolicyStatusEnabled  = MaskingPolicyStatusEnable
)

// String implements fmt.Stringer interface.
func (s MaskingPolicyStatus) String() string {
	switch s {
	case MaskingPolicyStatusDisable:
		return "DISABLED"
	case MaskingPolicyStatusEnable:
		return "ENABLED"
	default:
		return ""
	}
}

// MaskingPolicyType describes the masking policy type.
type MaskingPolicyType string

// MaskingPolicyType values.
const (
	MaskingPolicyTypeFull    MaskingPolicyType = "MASK_FULL"
	MaskingPolicyTypePartial MaskingPolicyType = "MASK_PARTIAL"
	MaskingPolicyTypeNull    MaskingPolicyType = "MASK_NULL"
	MaskingPolicyTypeDate    MaskingPolicyType = "MASK_DATE"
	MaskingPolicyTypeCustom  MaskingPolicyType = "CUSTOM"

	// Keep the old names as aliases for compatibility in incremental phase PRs.
	MaskingPolicyTypeMaskFull    = MaskingPolicyTypeFull
	MaskingPolicyTypeMaskPartial = MaskingPolicyTypePartial
	MaskingPolicyTypeMaskNull    = MaskingPolicyTypeNull
	MaskingPolicyTypeMaskDate    = MaskingPolicyTypeDate
)

// MaskingPolicyInfo is the struct to store the masking policy.
type MaskingPolicyInfo struct {
	ID          int64                        `json:"id"`
	Name        ast.CIStr                    `json:"name"`
	DBName      ast.CIStr                    `json:"db_name"`
	TableName   ast.CIStr                    `json:"table_name"`
	TableID     int64                        `json:"table_id"`
	ColumnName  ast.CIStr                    `json:"column_name"`
	ColumnID    int64                        `json:"column_id"`
	Expression  string                       `json:"expression"`
	Status      MaskingPolicyStatus          `json:"status"`
	MaskingType MaskingPolicyType            `json:"masking_type,omitempty"`
	RestrictOps ast.MaskingPolicyRestrictOps `json:"restrict_ops,omitempty"`
	CreatedAt   time.Time                    `json:"created_at,omitempty"`
	UpdatedAt   time.Time                    `json:"updated_at,omitempty"`
	CreatedBy   string                       `json:"created_by,omitempty"`
	UpdatedBy   string                       `json:"updated_by,omitempty"`
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
