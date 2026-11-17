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

package masking

import (
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const policyTableColumns = "policy_id, policy_name, db_name, table_name, table_id, column_name, column_id, expression, status, masking_type, restrict_on, created_at, updated_at, created_by"

// SelectSQL returns the canonical policy read query in DecodeMaskingPolicyRow order.
func SelectSQL() string {
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(policyTableColumns)
	query.WriteString(" FROM mysql.tidb_masking_policy")
	return query.String()
}

// DecodeMaskingPolicyRow decodes the canonical mysql.tidb_masking_policy column order.
func DecodeMaskingPolicyRow(row chunk.Row) (*model.MaskingPolicyInfo, error) {
	status, err := parseMaskingPolicyStatus(row.GetString(8))
	if err != nil {
		return nil, err
	}
	restrictOn := ""
	if !row.IsNull(10) {
		restrictOn = row.GetString(10)
	}
	restrictOps, err := parseMaskingPolicyRestrictOps(restrictOn)
	if err != nil {
		return nil, err
	}

	createdAt := time.Time{}
	if !row.IsNull(11) {
		createdAt, err = row.GetTime(11).GoTime(time.Local)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	updatedAt := time.Time{}
	if !row.IsNull(12) {
		updatedAt, err = row.GetTime(12).GoTime(time.Local)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	createdBy := ""
	if !row.IsNull(13) {
		createdBy = row.GetString(13)
	}

	return &model.MaskingPolicyInfo{
		ID:          row.GetInt64(0),
		Name:        ast.NewCIStr(row.GetString(1)),
		DBName:      ast.NewCIStr(row.GetString(2)),
		TableName:   ast.NewCIStr(row.GetString(3)),
		TableID:     row.GetInt64(4),
		ColumnName:  ast.NewCIStr(row.GetString(5)),
		ColumnID:    row.GetInt64(6),
		Expression:  row.GetString(7),
		Status:      status,
		MaskingType: parseMaskingPolicyType(row.GetString(9)),
		RestrictOps: restrictOps,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		CreatedBy:   createdBy,
		State:       model.StatePublic,
	}, nil
}

// parseMaskingPolicyStatus decodes enabled and disabled policy states.
func parseMaskingPolicyStatus(status string) (model.MaskingPolicyStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ENABLE", "ENABLED":
		return model.MaskingPolicyStatusEnable, nil
	case "DISABLE", "DISABLED":
		return model.MaskingPolicyStatusDisable, nil
	default:
		return model.MaskingPolicyStatusDisable, errors.Errorf("unknown masking policy status: %s", status)
	}
}

// parseMaskingPolicyType decodes a policy type, preserving compatibility with custom types.
func parseMaskingPolicyType(tp string) model.MaskingPolicyType {
	switch model.MaskingPolicyType(strings.ToUpper(strings.TrimSpace(tp))) {
	case model.MaskingPolicyTypeFull,
		model.MaskingPolicyTypePartial,
		model.MaskingPolicyTypeNull,
		model.MaskingPolicyTypeDate,
		model.MaskingPolicyTypeCustom:
		return model.MaskingPolicyType(strings.ToUpper(strings.TrimSpace(tp)))
	default:
		return model.MaskingPolicyTypeCustom
	}
}

// parseMaskingPolicyRestrictOps decodes the persisted restricted-operation list.
func parseMaskingPolicyRestrictOps(restrictOn string) (ast.MaskingPolicyRestrictOps, error) {
	restrictOn = strings.TrimSpace(strings.ToUpper(restrictOn))
	if restrictOn == "" || restrictOn == "NONE" {
		return ast.MaskingPolicyRestrictOpNone, nil
	}
	ops := ast.MaskingPolicyRestrictOpNone
	for _, token := range strings.Split(restrictOn, ",") {
		switch strings.TrimSpace(token) {
		case ast.MaskingPolicyRestrictNameInsertIntoSelect:
			ops |= ast.MaskingPolicyRestrictOpInsertIntoSelect
		case ast.MaskingPolicyRestrictNameUpdateSelect:
			ops |= ast.MaskingPolicyRestrictOpUpdateSelect
		case ast.MaskingPolicyRestrictNameDeleteSelect:
			ops |= ast.MaskingPolicyRestrictOpDeleteSelect
		case ast.MaskingPolicyRestrictNameCTAS:
			ops |= ast.MaskingPolicyRestrictOpCTAS
		case "NONE", "":
			// No-op.
		default:
			return ast.MaskingPolicyRestrictOpNone, errors.Errorf("unknown masking policy restrict option: %s", token)
		}
	}
	return ops, nil
}

// RestrictOpsToString encodes restricted operations in their canonical order.
func RestrictOpsToString(ops ast.MaskingPolicyRestrictOps) string {
	if ops == ast.MaskingPolicyRestrictOpNone {
		return "NONE"
	}

	vals := make([]string, 0, 4)
	if ops&ast.MaskingPolicyRestrictOpInsertIntoSelect != 0 {
		vals = append(vals, ast.MaskingPolicyRestrictNameInsertIntoSelect)
	}
	if ops&ast.MaskingPolicyRestrictOpUpdateSelect != 0 {
		vals = append(vals, ast.MaskingPolicyRestrictNameUpdateSelect)
	}
	if ops&ast.MaskingPolicyRestrictOpDeleteSelect != 0 {
		vals = append(vals, ast.MaskingPolicyRestrictNameDeleteSelect)
	}
	if ops&ast.MaskingPolicyRestrictOpCTAS != 0 {
		vals = append(vals, ast.MaskingPolicyRestrictNameCTAS)
	}
	return strings.Join(vals, ",")
}
