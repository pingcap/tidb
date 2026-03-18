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

package core

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func (b *PlanBuilder) checkMaskingPolicyRestrictOnSelectPlan(ctx context.Context, p base.LogicalPlan, op ast.MaskingPolicyRestrictOps) error {
	if b.is == nil || p == nil || op == ast.MaskingPolicyRestrictOpNone {
		return nil
	}
	if len(b.is.AllMaskingPolicies()) == 0 {
		return nil
	}
	cols := p.Schema().Columns
	if len(cols) == 0 {
		return nil
	}

	sv := b.ctx.GetSessionVars()
	schemaVersion := b.is.SchemaMetaVersion()
	checkedPolicyIDs := make(map[int64]struct{})
	for i := range cols {
		for _, candidate := range b.extractMaskingPolicyCandidateNamesFromOutputColumn(p, i) {
			policy, tblInfo, colInfo := b.findMaskingPolicyByFieldName(ctx, candidate)
			if policy == nil || tblInfo == nil || colInfo == nil || policy.Status != model.MaskingPolicyStatusEnable {
				continue
			}
			if policy.RestrictOps&op == 0 {
				continue
			}
			if _, ok := checkedPolicyIDs[policy.ID]; ok {
				continue
			}
			allowed, err := b.canCurrentSessionReadUnmaskedColumn(sv, schemaVersion, policy, tblInfo, colInfo)
			if err != nil {
				return err
			}
			if !allowed {
				return plannererrors.ErrAccessDeniedToMaskedColumn.GenWithStackByArgs(colInfo.Name.O)
			}
			checkedPolicyIDs[policy.ID] = struct{}{}
		}
	}
	return nil
}

func (*PlanBuilder) extractMaskingPolicyCandidateNamesFromOutputColumn(
	p base.LogicalPlan,
	outputColIdx int,
) []*types.FieldName {
	candidates := make([]*types.FieldName, 0, 2)
	seen := make(map[string]struct{}, 2)
	appendCandidate := func(name *types.FieldName) {
		if name == nil {
			return
		}
		key := name.DBName.L + "/" + name.TblName.L + "/" + name.OrigTblName.L + "/" + name.ColName.L + "/" + name.OrigColName.L
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		candidates = append(candidates, name)
	}

	visited := make(map[[2]int]struct{}, 4)
	var walkByIndex func(plan base.LogicalPlan, colIdx int)
	var walkByColumn func(plan base.LogicalPlan, col *expression.Column)

	walkByIndex = func(plan base.LogicalPlan, colIdx int) {
		if plan == nil || colIdx < 0 || colIdx >= plan.Schema().Len() {
			return
		}
		visitKey := [2]int{plan.ID(), colIdx}
		if _, ok := visited[visitKey]; ok {
			return
		}
		visited[visitKey] = struct{}{}

		names := plan.OutputNames()
		if colIdx < len(names) {
			appendCandidate(names[colIdx])
		}

		if proj, ok := plan.(*logicalop.LogicalProjection); ok && colIdx < len(proj.Exprs) && len(proj.Children()) > 0 {
			for _, sourceCol := range expression.ExtractColumns(proj.Exprs[colIdx]) {
				walkByColumn(proj.Children()[0], sourceCol)
			}
			return
		}

		outputCol := plan.Schema().Columns[colIdx]
		for _, child := range plan.Children() {
			if child == nil {
				continue
			}
			childColIdx := child.Schema().ColumnIndex(outputCol)
			if childColIdx >= 0 {
				walkByIndex(child, childColIdx)
			}
		}
	}

	walkByColumn = func(plan base.LogicalPlan, col *expression.Column) {
		if plan == nil || col == nil {
			return
		}
		colIdx := plan.Schema().ColumnIndex(col)
		if colIdx >= 0 {
			walkByIndex(plan, colIdx)
			return
		}
		for _, child := range plan.Children() {
			if child != nil {
				walkByColumn(child, col)
			}
		}
	}

	walkByIndex(p, outputColIdx)
	return candidates
}

func (b *PlanBuilder) findMaskingPolicyByFieldName(ctx context.Context, name *types.FieldName) (*model.MaskingPolicyInfo, *model.TableInfo, *model.ColumnInfo) {
	if name == nil || name.Hidden {
		return nil, nil, nil
	}
	tblName := name.OrigTblName
	if tblName.L == "" {
		tblName = name.TblName
	}
	if tblName.L == "" {
		return nil, nil, nil
	}
	colName := name.OrigColName
	if colName.L == "" {
		colName = name.ColName
	}
	if colName.L == "" {
		return nil, nil, nil
	}
	dbName := name.DBName
	if dbName.L == "" {
		dbName = pmodel.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if dbName.L == "" {
		return nil, nil, nil
	}
	tbl, err := b.is.TableByName(ctx, dbName, tblName)
	if err != nil {
		return nil, nil, nil
	}
	tblInfo := tbl.Meta()
	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil {
		return nil, nil, nil
	}
	policy, ok := b.is.MaskingPolicyByTableColumn(tblInfo.ID, colInfo.ID)
	if !ok {
		return nil, nil, nil
	}
	return policy, tblInfo, colInfo
}

func (b *PlanBuilder) canCurrentSessionReadUnmaskedColumn(
	sv *variable.SessionVars,
	schemaVersion int64,
	policy *model.MaskingPolicyInfo,
	tblInfo *model.TableInfo,
	colInfo *model.ColumnInfo,
) (bool, error) {
	expr, placeholder, err := getMaskingPolicyExpr(b.ctx.GetExprCtx(), sv, schemaVersion, policy, tblInfo, colInfo)
	if err != nil {
		return false, err
	}
	if placeholder == nil {
		return false, nil
	}

	// Use multiple test values to reduce false positive risk
	// If the expression returns different values for different inputs,
	// it's actually doing transformation (not just returning the input)
	// Only return true (can read unmasked) when EVERY sampled input
	// round-trips unchanged to avoid bypass on policies that preserve
	// one probe value or emit a fixed constant.
	sentinels := maskingPolicySentinelDatums(colInfo)
	evalCtx := b.ctx.GetExprCtx().GetEvalCtx()
	allRoundTrip := len(sentinels) > 0

	for _, sentinel := range sentinels {
		// The masking expression placeholder keeps the original column offset.
		// Build a row with enough columns and place the sentinel at that offset.
		colIdx := placeholder.Index
		if colIdx < 0 {
			colIdx = 0
		}
		rowDatums := make([]types.Datum, colIdx+1)
		rowDatums[colIdx] = sentinel
		row := chunk.MutRowFromDatums(rowDatums).ToRow()
		val, err := expr.Eval(evalCtx, row)
		if err != nil {
			return false, err
		}
		if val.IsNull() {
			return false, nil
		}
		// Compare with sentinel to check if expression returned input unchanged
		cmp, err := val.Compare(evalCtx.TypeCtx(), &sentinel, collate.GetBinaryCollator())
		if err != nil {
			return false, err
		}
		// If any sentinel does NOT round-trip unchanged, the expression
		// is actually masking (not just preserving specific values)
		if cmp != 0 {
			allRoundTrip = false
		}
	}

	return allRoundTrip, nil
}

// maskingPolicySentinelDatums returns multiple test values for checking if a masking
// expression properly transforms values. Using multiple values reduces the risk of
// false positives where an expression happens to return one of the test values.
func maskingPolicySentinelDatums(colInfo *model.ColumnInfo) []types.Datum {
	switch colInfo.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return []types.Datum{
			types.NewIntDatum(2099),
			types.NewIntDatum(1999),
			types.NewIntDatum(2000),
		}
	case mysql.TypeYear:
		return []types.Datum{
			types.NewIntDatum(2099),
			types.NewIntDatum(2000),
			types.NewIntDatum(2024),
		}
	case mysql.TypeDuration:
		fsp := colInfo.GetDecimal()
		if fsp == types.UnspecifiedFsp {
			fsp = types.DefaultFsp
		}
		return []types.Datum{
			types.NewDatum(types.Duration{Duration: 12*time.Hour + 34*time.Minute + 56*time.Second, Fsp: fsp}),
			types.NewDatum(types.Duration{Duration: 10*time.Hour + 20*time.Minute + 30*time.Second, Fsp: fsp}),
			types.NewDatum(types.Duration{Duration: 5*time.Hour + 15*time.Minute + 45*time.Second, Fsp: fsp}),
		}
	case mysql.TypeDate:
		return []types.Datum{
			types.NewDatum(types.NewTime(types.FromDate(2099, 12, 31, 0, 0, 0, 0), mysql.TypeDate, 0)),
			types.NewDatum(types.NewTime(types.FromDate(2000, 1, 1, 0, 0, 0, 0), mysql.TypeDate, 0)),
			types.NewDatum(types.NewTime(types.FromDate(2024, 6, 15, 0, 0, 0, 0), mysql.TypeDate, 0)),
		}
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		fsp := colInfo.GetDecimal()
		if fsp == types.UnspecifiedFsp {
			fsp = 0
		}
		return []types.Datum{
			types.NewDatum(types.NewTime(types.FromDate(2099, 12, 31, 23, 59, 58, 0), colInfo.GetType(), fsp)),
			types.NewDatum(types.NewTime(types.FromDate(2000, 1, 1, 0, 0, 0, 0), colInfo.GetType(), fsp)),
			types.NewDatum(types.NewTime(types.FromDate(2024, 6, 15, 12, 30, 45, 0), colInfo.GetType(), fsp)),
		}
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		return []types.Datum{
			types.NewStringDatum("TIDB_MASKING_SENTINEL_1"),
			types.NewStringDatum("TIDB_MASKING_SENTINEL_2"),
			types.NewStringDatum("TIDB_MASKING_SENTINEL_3"),
		}
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return []types.Datum{
			types.NewBytesDatum([]byte("TIDB_MASKING_SENTINEL_1")),
			types.NewBytesDatum([]byte("TIDB_MASKING_SENTINEL_2")),
			types.NewBytesDatum([]byte("TIDB_MASKING_SENTINEL_3")),
		}
	default:
		zero := table.GetZeroValue(colInfo)
		return []types.Datum{zero}
	}
}
