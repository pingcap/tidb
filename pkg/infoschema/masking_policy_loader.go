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

package infoschema

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// LoadMaskingPolicies loads masking policy metadata from mysql.tidb_masking_policy.
// If tableIDs is empty, all policies are loaded.
func LoadMaskingPolicies(
	factory func() (pools.Resource, error),
	policyTblInfo *model.TableInfo,
	tableIDs ...int64,
) ([]*model.MaskingPolicyInfo, error) {
	if policyTblInfo == nil {
		return nil, nil
	}
	if factory == nil {
		return nil, nil
	}

	resource, err := factory()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if closer, ok := resource.(interface{ Close() }); ok {
		defer closer.Close()
	}

	sctx, ok := resource.(sessionctx.Context)
	if !ok {
		return nil, errors.New("failed to cast resource to sessionctx.Context")
	}
	// During bootstrap/reload, the internal session may temporarily carry a
	// SessionExtendedInfoSchema wrapper without a concrete base infoschema.
	// Skip loading in this transient state and let a later reload pick policies up.
	if is := sctx.GetInfoSchema(); is == nil {
		return nil, nil
	} else if ext, ok := is.(*SessionExtendedInfoSchema); ok && ext.InfoSchema == nil {
		return nil, nil
	}

	query, args := buildLoadMaskingPoliciesQuery(tableIDs)
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	// Drop any pending txn state on this pooled internal session before querying.
	sctx.RollbackTxn(internalCtx)
	// Use current internal session directly to avoid re-entering session pool creation path.
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
		internalCtx,
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		query,
		args...,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	policies := make([]*model.MaskingPolicyInfo, 0, len(rows))
	for _, row := range rows {
		policy, err := maskingPolicyInfoFromChunkRow(row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		policies = append(policies, policy)
	}

	slices.SortFunc(policies, func(a, b *model.MaskingPolicyInfo) int {
		if x := cmp.Compare(a.TableID, b.TableID); x != 0 {
			return x
		}
		if x := cmp.Compare(a.ColumnID, b.ColumnID); x != 0 {
			return x
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return policies, nil
}

func buildLoadMaskingPoliciesQuery(tableIDs []int64) (string, []any) {
	const baseQuery = `SELECT policy_id, policy_name, db_name, table_name, table_id, column_name, column_id, expression, CAST(status AS CHAR), masking_type, restrict_on, created_at, updated_at, created_by
FROM mysql.tidb_masking_policy`

	idSet := make(map[int64]struct{}, len(tableIDs))
	ids := make([]int64, 0, len(tableIDs))
	for _, id := range tableIDs {
		if id <= 0 {
			continue
		}
		if _, ok := idSet[id]; ok {
			continue
		}
		idSet[id] = struct{}{}
		ids = append(ids, id)
	}
	slices.Sort(ids)

	var sb strings.Builder
	sb.WriteString(baseQuery)
	args := make([]any, 0, len(ids))
	if len(ids) > 0 {
		sb.WriteString(" WHERE ")
		for i, id := range ids {
			if i > 0 {
				sb.WriteString(" OR ")
			}
			sb.WriteString("table_id = %?")
			args = append(args, id)
		}
	}
	sb.WriteString(" ORDER BY table_id, column_id, policy_id")
	return sb.String(), args
}

func maskingPolicyInfoFromChunkRow(row chunk.Row) (*model.MaskingPolicyInfo, error) {
	status, err := maskingPolicyStatusFromString(row.GetString(8))
	if err != nil {
		return nil, err
	}
	restrictOn := ""
	if !row.IsNull(10) {
		restrictOn = row.GetString(10)
	}
	restrictOps, err := maskingPolicyRestrictOpsFromString(restrictOn)
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
		MaskingType: maskingPolicyTypeFromString(row.GetString(9)),
		RestrictOps: restrictOps,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		CreatedBy:   createdBy,
		State:       model.StatePublic,
	}, nil
}

func maskingPolicyStatusFromString(status string) (model.MaskingPolicyStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ENABLE", "ENABLED":
		return model.MaskingPolicyStatusEnable, nil
	case "DISABLE", "DISABLED":
		return model.MaskingPolicyStatusDisable, nil
	default:
		return model.MaskingPolicyStatusDisable, errors.Errorf("unknown masking policy status: %s", status)
	}
}

func maskingPolicyTypeFromString(tp string) model.MaskingPolicyType {
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

func maskingPolicyRestrictOpsFromString(restrictOn string) (ast.MaskingPolicyRestrictOps, error) {
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
