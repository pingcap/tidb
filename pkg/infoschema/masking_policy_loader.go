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

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	maskingmeta "github.com/pingcap/tidb/pkg/meta/masking"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// LoadMaskingPolicies loads all masking policy metadata through mysql.tidb_masking_policy.
// This is the simplified version for delayed loading - loads ALL policies
func LoadMaskingPolicies(
	factory func() (pools.Resource, error),
) ([]*model.MaskingPolicyInfo, error) {
	return loadMaskingPoliciesWithTableIDs(context.Background(), factory, nil, 0, nil)
}

// loadMaskingPoliciesWithTableIDs loads masking policy metadata through mysql.tidb_masking_policy.
// If tableIDs is empty, all policies are loaded.
func loadMaskingPoliciesWithTableIDs(ctx context.Context, factory func() (pools.Resource, error), tableIDs []int64, snapshotTS uint64, snapshotSchema InfoSchema) ([]*model.MaskingPolicyInfo, error) {
	const maxBatchSize = 1024
	failpoint.InjectCall("beforeLoadMaskingPolicies", ctx)
	failpoint.Inject("mockLoadMaskingPoliciesError", func() {
		failpoint.Return(nil, errors.New("mock masking policy load error"))
	})

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
	opts := []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}
	if snapshotSchema != nil {
		// This schema may not be in Domain's cache yet. Bind it directly to the
		// dedicated internal session instead of recursively resolving its timestamp.
		vars := sctx.GetSessionVars()
		vars.SnapshotTS = snapshotTS
		vars.SnapshotInfoschema = snapshotSchema
	} else {
		opts = append(opts, sqlexec.ExecOptionWithSnapshot(snapshotTS))
	}

	ids, hasFilter := normalizeMaskingPolicyTableIDs(tableIDs)
	if hasFilter && len(ids) == 0 {
		return nil, nil
	}

	loadBatch := func(batchIDs []int64, policies []*model.MaskingPolicyInfo) ([]*model.MaskingPolicyInfo, error) {
		query, args := buildLoadMaskingPoliciesQuery(batchIDs)
		internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
		rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
			internalCtx,
			opts,
			query,
			args...,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}

		for _, row := range rows {
			policy, err := maskingPolicyInfoFromChunkRow(row)
			if err != nil {
				return nil, errors.Trace(err)
			}
			policies = append(policies, policy)
		}
		return policies, nil
	}

	policies := make([]*model.MaskingPolicyInfo, 0)
	if !hasFilter {
		policies, err = loadBatch(nil, policies)
		if err != nil {
			return nil, err
		}
	} else {
		for start := 0; start < len(ids); start += maxBatchSize {
			end := min(start+maxBatchSize, len(ids))
			policies, err = loadBatch(ids[start:end], policies)
			if err != nil {
				return nil, err
			}
		}
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
	var sb strings.Builder
	sb.WriteString(maskingmeta.SelectSQL())
	args := make([]any, 0, len(tableIDs))
	if len(tableIDs) > 0 {
		sb.WriteString(" WHERE table_id IN (")
		for i, id := range tableIDs {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("%?")
			args = append(args, id)
		}
		sb.WriteString(")")
	}
	sb.WriteString(" ORDER BY table_id, column_id, policy_id")
	return sb.String(), args
}

func maskingPolicyInfoFromChunkRow(row chunk.Row) (*model.MaskingPolicyInfo, error) {
	return maskingmeta.DecodeMaskingPolicyRow(row)
}

func normalizeMaskingPolicyTableIDs(tableIDs []int64) ([]int64, bool) {
	hasFilter := len(tableIDs) > 0
	if !hasFilter {
		return nil, false
	}

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
	return ids, true
}
