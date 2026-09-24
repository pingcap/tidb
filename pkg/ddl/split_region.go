// Copyright 2019 PingCAP, Inc.
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

package ddl

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/regionsplit"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
)

// GlobalScatterGroupID is used to indicate the global scatter group ID.
const GlobalScatterGroupID int64 = -1

func splitPartitionTableRegion(ctx sessionctx.Context, store kv.SplittableStore, tbInfo *model.TableInfo, parts []model.PartitionDefinition, scatterScope string) {
	// Max partition count is 8192, should we sample and just choose some partitions to split?
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	ctxWithTimeout = kv.WithInternalSourceType(ctxWithTimeout, kv.InternalTxnDDL)

	var regionIDs []uint64
	if hasSplitPolicies(tbInfo) {
		regionIDs = append(regionIDs,
			applySplitPoliciesForTable(ctxWithTimeout, ctx, store, tbInfo, tbInfo.ID, scatterScope)...)
		for _, def := range parts {
			regionIDs = append(regionIDs,
				applySplitPoliciesForTable(ctxWithTimeout, ctx, store, tbInfo, def.ID, scatterScope)...)
		}
	} else if hasExplicitRegionSplitConfig(tbInfo) {
		regionIDs = make([]uint64, 0, len(parts)*(len(tbInfo.Indices)+1))
		scatter, scatterGroupID := getScatterConfig(scatterScope, tbInfo.ID)
		// Try to split global index region here.
		regionIDs = append(regionIDs, splitIndexRegion(store, tbInfo, scatter, tbInfo.ID, scatterGroupID)...)
		for _, def := range parts {
			regionIDs = append(regionIDs, preSplitPhysicalTableByShardRowID(ctxWithTimeout, store, tbInfo, def.ID, scatterScope)...)
		}
	} else {
		regionIDs = make([]uint64, 0, len(parts))
		for _, def := range parts {
			regionIDs = append(regionIDs, SplitRecordRegion(ctxWithTimeout, store, def.ID, tbInfo.ID, scatterScope))
		}
	}
	if scatterScope != vardef.ScatterOff {
		WaitScatterRegionFinish(ctxWithTimeout, store, regionIDs...)
	}
}

func splitTableRegion(ctx sessionctx.Context, store kv.SplittableStore, tbInfo *model.TableInfo, scatterScope string) {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	ctxWithTimeout = kv.WithInternalSourceType(ctxWithTimeout, kv.InternalTxnDDL)

	var regionIDs []uint64
	if hasSplitPolicies(tbInfo) {
		regionIDs = applySplitPoliciesForTable(ctxWithTimeout, ctx, store, tbInfo, tbInfo.ID, scatterScope)
	} else if hasExplicitRegionSplitConfig(tbInfo) {
		regionIDs = preSplitPhysicalTableByShardRowID(ctxWithTimeout, store, tbInfo, tbInfo.ID, scatterScope)
	} else {
		regionIDs = append(regionIDs, SplitRecordRegion(ctxWithTimeout, store, tbInfo.ID, tbInfo.ID, scatterScope))
	}
	if scatterScope != vardef.ScatterOff {
		WaitScatterRegionFinish(ctxWithTimeout, store, regionIDs...)
	}
}

// `tID` is used to control the scope of scatter. If it is `ScatterTable`, the corresponding tableID is used.
// If it is `ScatterGlobal`, the scatter configured at global level uniformly use -1 as `tID`.
func getScatterConfig(scope string, tableID int64) (scatter bool, tID int64) {
	switch scope {
	case vardef.ScatterTable:
		return true, tableID
	case vardef.ScatterGlobal:
		return true, GlobalScatterGroupID
	default:
		return false, tableID
	}
}

func preSplitPhysicalTableByShardRowID(ctx context.Context, store kv.SplittableStore, tbInfo *model.TableInfo, physicalID int64, scatterScope string) []uint64 {
	// Example:
	// sharding_bits = 4
	// PreSplitRegions = 2
	//
	// then will pre-split 2^2 = 4 regions.
	//
	// in this code:
	// max   = 1 << sharding_bits = 16
	// step := int64(1 << (sharding_bits - tblInfo.PreSplitRegions)) = 1 << (4-2) = 4;
	//
	// then split regionID is below:
	// 4  << 59 = 2305843009213693952
	// 8  << 59 = 4611686018427387904
	// 12 << 59 = 6917529027641081856
	//
	// The 4 pre-split regions range is below:
	// 0                   ~ 2305843009213693952
	// 2305843009213693952 ~ 4611686018427387904
	// 4611686018427387904 ~ 6917529027641081856
	// 6917529027641081856 ~ 9223372036854775807 ( (1 << 63) - 1 )
	//
	// And the max _tidb_rowid is 9223372036854775807, it won't be negative number.

	// Split table region.
	var ft *types.FieldType
	if pkCol := tbInfo.GetPkColInfo(); pkCol != nil {
		ft = &pkCol.FieldType
	} else {
		ft = types.NewFieldType(mysql.TypeLonglong)
	}
	shardFmt := autoid.NewShardIDFormat(ft, shardingBits(tbInfo), tbInfo.AutoRandomRangeBits)
	step := int64(1 << (shardFmt.ShardBits - tbInfo.PreSplitRegions))
	maxv := int64(1 << shardFmt.ShardBits)
	splitTableKeys := make([][]byte, 0, 1<<(tbInfo.PreSplitRegions))
	splitTableKeys = append(splitTableKeys, tablecodec.GenTablePrefix(physicalID))
	for p := step; p < maxv; p += step {
		recordID := p << shardFmt.IncrementalBits
		recordPrefix := tablecodec.GenTableRecordPrefix(physicalID)
		key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(recordID))
		splitTableKeys = append(splitTableKeys, key)
	}
	scatter, scatterGroupID := getScatterConfig(scatterScope, tbInfo.ID)
	regionIDs, err := store.SplitRegions(ctx, splitTableKeys, scatter, &scatterGroupID)
	if err != nil {
		logutil.DDLLogger().Warn("pre split some table regions failed",
			zap.Stringer("table", tbInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	regionIDs = append(regionIDs, splitIndexRegion(store, tbInfo, scatter, physicalID, scatterGroupID)...)
	return regionIDs
}

// SplitRecordRegion is to split region in store by table prefix.
func SplitRecordRegion(ctx context.Context, store kv.SplittableStore, physicalTableID, tableID int64, scatterScope string) uint64 {
	tableStartKey := tablecodec.GenTablePrefix(physicalTableID)
	scatter, tID := getScatterConfig(scatterScope, tableID)
	regionIDs, err := store.SplitRegions(ctx, [][]byte{tableStartKey}, scatter, &tID)
	if err != nil {
		// It will be automatically split by TiKV later.
		logutil.DDLLogger().Warn("split table region failed", zap.Error(err))
	}
	if len(regionIDs) == 1 {
		return regionIDs[0]
	}
	return 0
}

func splitIndexRegion(store kv.SplittableStore, tblInfo *model.TableInfo, scatter bool, physicalTableID, scatterGroupID int64) []uint64 {
	splitKeys := make([][]byte, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if tblInfo.GetPartitionInfo() != nil &&
			((idx.Global && tblInfo.ID != physicalTableID) || (!idx.Global && tblInfo.ID == physicalTableID)) {
			continue
		}
		id := idx.ID
		// For normal index, split regions like
		// [t_tid_, 			t_tid_i_idx1ID+1),
		// [t_tid_i_idx1ID+1,	t_tid_i_idx2ID+1),
		// ...
		// [t_tid_i_idxMaxID+1, t_tid_r_xxxx)
		//
		// For global index, split regions like
		// [t_tid_i_idx1ID, t_tid_i_idx2ID),
		// [t_tid_i_idx2ID, t_tid_i_idx3ID),
		// ...
		// [t_tid_i_idxMaxID, t_pid1_)
		if !idx.Global {
			id = id + 1
		}
		indexPrefix := tablecodec.EncodeTableIndexPrefix(physicalTableID, id)
		splitKeys = append(splitKeys, indexPrefix)
	}
	regionIDs, err := store.SplitRegions(context.Background(), splitKeys, scatter, &scatterGroupID)
	if err != nil {
		logutil.DDLLogger().Warn("pre split some table index regions failed",
			zap.Stringer("table", tblInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	return regionIDs
}

// WaitScatterRegionFinish will block until all regions are scattered.
func WaitScatterRegionFinish(ctx context.Context, store kv.SplittableStore, regionIDs ...uint64) {
	for _, regionID := range regionIDs {
		err := store.WaitScatterRegionFinish(ctx, regionID, 0)
		if err != nil {
			logutil.DDLLogger().Warn("wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
			// We don't break for PDError because it may caused by ScatterRegion request failed.
			if _, ok := errors.Cause(err).(*tikverr.PDError); !ok {
				break
			}
		}
	}
}

func hasSplitPolicies(tbInfo *model.TableInfo) bool {
	if tbInfo.TableSplitPolicy != nil {
		return true
	}
	for _, idx := range tbInfo.Indices {
		if idx.RegionSplitPolicy != nil {
			return true
		}
	}
	return false
}

func hasExplicitRegionSplitConfig(tbInfo *model.TableInfo) bool {
	return hasSplitPolicies(tbInfo) || (shardingBits(tbInfo) > 0 && tbInfo.PreSplitRegions > 0)
}

func applySplitPoliciesForTable(ctx context.Context, sctx sessionctx.Context, store kv.SplittableStore, tbInfo *model.TableInfo, physicalTableID int64, scatterScope string) []uint64 {
	var regionIDs []uint64

	scatter, scatterGroupID := getScatterConfig(scatterScope, tbInfo.ID)

	// apply table policy
	// Partitioned-table records use physical partition IDs. The logical table ID
	// remains available for global index policies below.
	if policy := tbInfo.TableSplitPolicy; policy != nil &&
		(tbInfo.GetPartitionInfo() == nil || physicalTableID != tbInfo.ID) {
		boundCols := splitPolicyHandleColumns(tbInfo)
		policyCtx, err := splitPolicyApplyCtx(sctx, policy.TimeZone)
		if err != nil {
			// A zone name that cannot be parsed is treated like any other
			// malformed persisted policy: skip it instead of failing the DDL
			// that reapplied the policy.
			logutil.DDLLogger().Warn("failed to resolve time zone for table policy",
				zap.String("table", tbInfo.Name.O),
				zap.String("timeZone", policy.TimeZone),
				zap.Error(err))
			goto index
		}
		lower, err := parseValuesToDatums(sctx.GetExprCtx(), policy.Lower, boundCols, policyCtx.TypeCtx())
		if err != nil {
			logutil.DDLLogger().Warn("failed to parse lower bound for table policy",
				zap.String("table", tbInfo.Name.O), zap.Error(err))
			goto index
		}
		upper, err := parseValuesToDatums(sctx.GetExprCtx(), policy.Upper, boundCols, policyCtx.TypeCtx())
		if err != nil {
			logutil.DDLLogger().Warn("failed to parse upper bound for table policy",
				zap.String("table", tbInfo.Name.O), zap.Error(err))
			goto index
		}

		handleCols := regionsplit.BuildHandleColsForSplit(tbInfo)
		keys, err := regionsplit.GetSplitTableKeys(policyCtx, tbInfo, handleCols, physicalTableID, lower, upper, int(policy.Regions), nil, dbterror.ErrInvalidSplitRegionRanges)
		if err != nil {
			logutil.DDLLogger().Warn("failed to generate split keys for table policy",
				zap.String("table", tbInfo.Name.O), zap.Error(err))
			goto index
		}

		ids, err := store.SplitRegions(ctx, keys, scatter, &scatterGroupID)
		if err != nil {
			logutil.DDLLogger().Warn("split regions failed", zap.Error(err))
			goto index
		}
		regionIDs = ids
	}

index:
	// 2. Apply index policies (including PRIMARY)
	for _, idx := range tbInfo.Indices {
		if tbInfo.GetPartitionInfo() != nil &&
			((idx.Global && tbInfo.ID != physicalTableID) || (!idx.Global && tbInfo.ID == physicalTableID)) {
			continue
		}

		if idx.RegionSplitPolicy == nil {
			continue
		}

		// skip clustered primary
		if tbInfo.HasClusteredIndex() && idx.Primary {
			continue
		}

		policy := idx.RegionSplitPolicy
		boundCols := splitPolicyIndexColumns(tbInfo, idx)
		policyCtx, err := splitPolicyApplyCtx(sctx, policy.TimeZone)
		if err != nil {
			logutil.DDLLogger().Warn("failed to resolve time zone for index policy",
				zap.String("table", tbInfo.Name.O),
				zap.String("index", idx.Name.O),
				zap.String("timeZone", policy.TimeZone),
				zap.Error(err))
			continue
		}
		lower, err := parseValuesToDatums(sctx.GetExprCtx(), policy.Lower, boundCols, policyCtx.TypeCtx())
		if err != nil {
			logutil.DDLLogger().Warn("failed to parse lower bound for index policy",
				zap.String("table", tbInfo.Name.O),
				zap.String("index", idx.Name.O),
				zap.Error(err))
			continue
		}
		upper, err := parseValuesToDatums(sctx.GetExprCtx(), policy.Upper, boundCols, policyCtx.TypeCtx())
		if err != nil {
			logutil.DDLLogger().Warn("failed to parse upper bound for index policy",
				zap.String("table", tbInfo.Name.O),
				zap.String("index", idx.Name.O),
				zap.Error(err))
			continue
		}

		keys, err := regionsplit.GetSplitIndexKeys(policyCtx, tbInfo, idx, physicalTableID, lower, upper, int(policy.Regions), nil, dbterror.ErrInvalidSplitRegionRanges)
		if err != nil {
			logutil.DDLLogger().Warn("failed to generate split keys for index policy",
				zap.String("table", tbInfo.Name.O),
				zap.String("index", idx.Name.O),
				zap.Error(err))
			continue
		}

		ids, err := store.SplitRegions(ctx, keys, scatter, &scatterGroupID)
		if err != nil {
			logutil.DDLLogger().Warn("split regions failed", zap.Error(err))
			continue
		}
		regionIDs = append(regionIDs, ids...)
	}

	return regionIDs
}

// parseValuesToDatums evaluates the persisted split-policy bound expressions and
// converts the results to the target column types. The conversion mirrors the
// one-shot `SPLIT TABLE`/`SPLIT INDEX` statements so that a persisted policy
// produces the same split keys, e.g. a string literal bound for an integer
// handle column is converted to an integer value instead of retaining its
// untyped string form. See https://github.com/pingcap/tidb/issues/71395.
//
// typeCtx is a detached context. It does not inherit the applying session's
// SQL mode, so a non-strict worker cannot turn an invalid bound into a warning
// and a zero value. Policies stored before this validation existed can still
// fail conversion; callers log that error and skip the policy.
func parseValuesToDatums(exprCtx exprctx.ExprContext, values []string, cols []*model.ColumnInfo, typeCtx types.Context) ([]types.Datum, error) {
	datums := make([]types.Datum, len(values))
	// Only convert when the bound count matches the target columns. A mismatch
	// means the persisted policy is malformed, keep the previous best-effort
	// behavior instead of failing the whole split.
	convert := len(cols) == len(values)
	for i, val := range values {
		d, err := expression.ParseSimpleExpr(exprCtx, val)
		if err != nil {
			return nil, err
		}
		datum, err := d.Eval(exprCtx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil, err
		}
		if convert {
			datum, err = convertSplitPolicyValue(datum, cols[i], typeCtx)
			if err != nil {
				return nil, err
			}
		}
		datums[i] = datum
	}
	return datums, nil
}

// splitPolicyApplyCtx returns a detached statement context for converting and
// encoding one persisted policy. The context uses default statement flags, so
// conversion errors stay errors regardless of the worker SQL mode, and its
// warnings stay off the live session.
//
// An empty timeZone is the compatibility fallback for policies written before
// the defining zone was stored: those bounds are interpreted in the applying
// session's zone.
func splitPolicyApplyCtx(sctx sessionctx.Context, timeZone string) (*stmtctx.StatementContext, error) {
	loc := sctx.GetSessionVars().Location()
	if timeZone != "" {
		parsed, err := timeutil.ParseTimeZone(timeZone)
		if err != nil {
			return nil, err
		}
		loc = parsed
	}
	if loc == nil {
		loc = time.UTC
	}
	return stmtctx.NewStmtCtxWithTimeZone(loc), nil
}

// splitPolicyTypeCtx returns the detached conversion context used when a policy
// is defined. It uses the defining session's time zone and default statement
// flags, so non-strict sql_mode cannot persist a bound that later fails to convert.
func splitPolicyTypeCtx(loc *time.Location) types.Context {
	if loc == nil {
		loc = time.UTC
	}
	return stmtctx.NewStmtCtxWithTimeZone(loc).TypeCtx()
}

// splitPolicyHandleColumns returns the columns a table-level split policy bound
// is compared against, following the same rules as the handle columns used by
// the one-shot `SPLIT TABLE ... BETWEEN` statement.
func splitPolicyHandleColumns(tbInfo *model.TableInfo) []*model.ColumnInfo {
	switch {
	case tbInfo.PKIsHandle:
		if col := tbInfo.GetPkColInfo(); col != nil {
			return []*model.ColumnInfo{col}
		}
	case tbInfo.IsCommonHandle:
		if pkIdx := tables.FindPrimaryIndex(tbInfo); pkIdx != nil {
			cols := make([]*model.ColumnInfo, 0, len(pkIdx.Columns))
			for _, idxCol := range pkIdx.Columns {
				cols = append(cols, tbInfo.Columns[idxCol.Offset])
			}
			return cols
		}
	default:
		return []*model.ColumnInfo{model.NewExtraHandleColInfo()}
	}
	return nil
}

// splitPolicyIndexColumns returns the columns an index split policy bound is
// compared against.
func splitPolicyIndexColumns(tbInfo *model.TableInfo, indexInfo *model.IndexInfo) []*model.ColumnInfo {
	cols := make([]*model.ColumnInfo, 0, len(indexInfo.Columns))
	for _, idxCol := range indexInfo.Columns {
		cols = append(cols, tbInfo.Columns[idxCol.Offset])
	}
	return cols
}

// convertSplitPolicyValue converts a split-policy bound value to the target
// column type, mirroring the conversion performed for the one-shot
// `SPLIT TABLE`/`SPLIT INDEX` statements in PlanBuilder.convertValue.
func convertSplitPolicyValue(value types.Datum, col *model.ColumnInfo, typeCtx types.Context) (types.Datum, error) {
	d, err := value.ConvertTo(typeCtx, &col.FieldType)
	if err != nil {
		if !types.ErrTruncated.Equal(err) && !types.ErrTruncatedWrongVal.Equal(err) && !types.ErrBadNumber.Equal(err) {
			return d, err
		}
		valStr, err1 := value.ToString()
		if err1 != nil {
			return d, err1
		}
		return d, types.ErrTruncated.GenWithStack("Incorrect value: '%-.128s' for column '%.192s'", valStr, col.Name.O)
	}
	return d, nil
}

func normalizeSplitPolicy(ctx expression.BuildContext, splitOpt *ast.SplitIndexOption, tbInfo *model.TableInfo) (*model.RegionSplitPolicy, string, error) {
	indexName := ""
	if !splitOpt.TableLevel {
		pkName := strings.ToLower(mysql.PrimaryKeyName)
		indexName = splitOpt.IndexName.L
		// fill primary key name if empty
		if splitOpt.PrimaryKey && indexName == "" {
			indexName = pkName
		}
		// set PrimaryKey if SPLIT INDEX `PRIMARY`
		if indexName == pkName {
			splitOpt.PrimaryKey = true
		}
	}

	if tbInfo.HasClusteredIndex() && splitOpt.PrimaryKey {
		// cannot specify both SPLIT PRIMARY for CLUSTERED table
		// it is for unclustered primary
		return nil, "", dbterror.ErrForbiddenDDL.FastGenByArgs("SPLIT PRIMARY is only for non-clustered table")
	}

	if splitOpt.SplitOpt.Num < 1 {
		// must larger than 1
		return nil, "", dbterror.ErrForbiddenDDL.FastGenByArgs("SPLIT REGION number must not be zero or negative")
	}

	// Resolve the columns the bound values are compared against so they can be
	// converted to the same types as the one-shot `SPLIT TABLE`/`SPLIT INDEX`
	// statements. See https://github.com/pingcap/tidb/issues/71395.
	var boundCols []*model.ColumnInfo
	if splitOpt.TableLevel {
		boundCols = splitPolicyHandleColumns(tbInfo)
	} else {
		idx := tbInfo.FindIndexByName(indexName)
		if idx == nil {
			return nil, "", dbterror.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
		}
		boundCols = splitPolicyIndexColumns(tbInfo, idx)
	}
	if len(boundCols) != len(splitOpt.SplitOpt.Upper) || len(boundCols) != len(splitOpt.SplitOpt.Lower) {
		return nil, "", dbterror.ErrInvalidSplitRegionRanges.GenWithStackByArgs("length of index columns and split values differ")
	}

	loc := ctx.GetEvalCtx().Location()
	if loc == nil {
		loc = time.UTC
	}
	typeCtx := splitPolicyTypeCtx(loc)
	policy := &model.RegionSplitPolicy{
		Regions:  splitOpt.SplitOpt.Num,
		TimeZone: timeutil.ZoneName(loc),
	}
	var err error
	policy.Lower, err = normalizeSplitPolicyBounds(ctx, splitOpt.SplitOpt.Lower, boundCols, typeCtx)
	if err != nil {
		return nil, "", err
	}
	policy.Upper, err = normalizeSplitPolicyBounds(ctx, splitOpt.SplitOpt.Upper, boundCols, typeCtx)
	if err != nil {
		return nil, "", err
	}

	return policy, indexName, nil
}

// normalizeSplitPolicyBounds validates each bound expression, converts it to the
// matching column type, and returns the restored SQL text.
func normalizeSplitPolicyBounds(ctx expression.BuildContext, exprs []ast.ExprNode, boundCols []*model.ColumnInfo, typeCtx types.Context) ([]string, error) {
	var buf strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
	bounds := make([]string, len(exprs))
	for i, expr := range exprs {
		buf.Reset()
		d, err := expression.BuildSimpleExpr(ctx, expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value, err := d.Eval(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Convert with a detached strict context so an unconvertible bound is
		// rejected here instead of being stored and skipped when the policy is
		// applied later.
		if _, err := convertSplitPolicyValue(value, boundCols[i], typeCtx); err != nil {
			return nil, errors.Trace(err)
		}
		if err := expr.Restore(restoreCtx); err != nil {
			return nil, errors.Trace(err)
		}
		bounds[i] = buf.String()
	}
	return bounds, nil
}
