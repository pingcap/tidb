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
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/regionsplit"
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
			applySplitPoliciesForTable(ctxWithTimeout, ctx, store, tbInfo, tbInfo.ID)...)
		for _, def := range parts {
			regionIDs = append(regionIDs,
				applySplitPoliciesForTable(ctxWithTimeout, ctx, store, tbInfo, def.ID)...)
		}
	} else if shardingBits(tbInfo) > 0 && tbInfo.PreSplitRegions > 0 {
		regionIDs = make([]uint64, 0, len(parts)*(len(tbInfo.Indices)+1))
		scatter, tableID := getScatterConfig(scatterScope, tbInfo.ID)
		// Try to split global index region here.
		regionIDs = append(regionIDs, splitIndexRegion(store, tbInfo, scatter, tableID)...)
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
		regionIDs = applySplitPoliciesForTable(ctxWithTimeout, ctx, store, tbInfo, tbInfo.ID)
	} else if shardingBits(tbInfo) > 0 && tbInfo.PreSplitRegions > 0 {
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
	scatter, tableID := getScatterConfig(scatterScope, tbInfo.ID)
	regionIDs, err := store.SplitRegions(ctx, splitTableKeys, scatter, &tableID)
	if err != nil {
		logutil.DDLLogger().Warn("pre split some table regions failed",
			zap.Stringer("table", tbInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	regionIDs = append(regionIDs, splitIndexRegion(store, tbInfo, scatter, physicalID)...)
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

func splitIndexRegion(store kv.SplittableStore, tblInfo *model.TableInfo, scatter bool, physicalTableID int64) []uint64 {
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
	regionIDs, err := store.SplitRegions(context.Background(), splitKeys, scatter, &physicalTableID)
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

func applySplitPoliciesForTable(ctx context.Context, sctx sessionctx.Context, store kv.SplittableStore, tbInfo *model.TableInfo, physicalTableID int64) []uint64 {
	var regionIDs []uint64

	sc := sctx.GetSessionVars().StmtCtx
	svars := sctx.GetSessionVars() //nolint:forbidigo
	scatter, tableID := getScatterConfig(svars.ScatterRegion, tbInfo.ID)

	// apply table policy
	if policy := tbInfo.TableSplitPolicy; policy != nil {
		lower, err := parseValuesToDatums(sctx.GetExprCtx(), policy.Lower)
		if err != nil {
			logutil.DDLLogger().Warn("failed to parse lower bound for table policy",
				zap.String("table", tbInfo.Name.O), zap.Error(err))
			goto index
		}
		upper, err := parseValuesToDatums(sctx.GetExprCtx(), policy.Upper)
		if err != nil {
			logutil.DDLLogger().Warn("failed to parse upper bound for table policy",
				zap.String("table", tbInfo.Name.O), zap.Error(err))
			goto index
		}

		handleCols := regionsplit.BuildHandleColsForSplit(tbInfo)
		keys, err := regionsplit.GetSplitTableKeys(sc, tbInfo, handleCols, physicalTableID, lower, upper, int(policy.Regions), nil, dbterror.ErrInvalidSplitRegionRanges)
		if err != nil {
			logutil.DDLLogger().Warn("failed to generate split keys for table policy",
				zap.String("table", tbInfo.Name.O), zap.Error(err))
			goto index
		}

		ids, err := store.SplitRegions(ctx, keys, scatter, &tableID)
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
		lower, err := parseValuesToDatums(sctx.GetExprCtx(), policy.Lower)
		if err != nil {
			logutil.DDLLogger().Warn("failed to parse lower bound for index policy",
				zap.String("table", tbInfo.Name.O),
				zap.String("index", idx.Name.O),
				zap.Error(err))
			continue
		}
		upper, err := parseValuesToDatums(sctx.GetExprCtx(), policy.Upper)
		if err != nil {
			logutil.DDLLogger().Warn("failed to parse upper bound for index policy",
				zap.String("table", tbInfo.Name.O),
				zap.String("index", idx.Name.O),
				zap.Error(err))
			continue
		}

		keys, err := regionsplit.GetSplitIndexKeys(sc, tbInfo, idx, physicalTableID, lower, upper, int(policy.Regions), nil, dbterror.ErrInvalidSplitRegionRanges)
		if err != nil {
			logutil.DDLLogger().Warn("failed to generate split keys for index policy",
				zap.String("table", tbInfo.Name.O),
				zap.String("index", idx.Name.O),
				zap.Error(err))
		}

		ids, err := store.SplitRegions(ctx, keys, scatter, &tableID)
		if err != nil {
			logutil.DDLLogger().Warn("split regions failed", zap.Error(err))
			goto index
		}
		regionIDs = append(regionIDs, ids...)
	}

	return regionIDs
}

func parseValuesToDatums(exprCtx exprctx.ExprContext, values []string) ([]types.Datum, error) {
	datums := make([]types.Datum, len(values))
	for i, val := range values {
		d, err := expression.ParseSimpleExpr(exprCtx, val)
		if err != nil {
			return nil, err
		}
		datums[i], err = d.Eval(exprCtx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil, err
		}
	}
	return datums, nil
}

func normalizeSplitPolicy(ctx expression.BuildContext, splitOpt *ast.SplitIndexOption, tbInfo *model.TableInfo) (*model.RegionSplitPolicy, string, error) {
	if tbInfo.HasClusteredIndex() && splitOpt.PrimaryKey {
		// cannot specify both SPLIT PRIMARY for CLUSTERED table
		// it is for unclustered primary
		return nil, "", dbterror.ErrForbiddenDDL.FastGenByArgs("SPLIT PRIMARY is only for non-clustered table")
	}

	if splitOpt.SplitOpt.Num < 1 {
		// must larger than 1
		return nil, "", dbterror.ErrForbiddenDDL.FastGenByArgs("SPLIT REGION number must not be zero or negative")
	}

	indexName := ""
	if !splitOpt.TableLevel {
		pkName := strings.ToLower(mysql.PrimaryKeyName)
		indexName = splitOpt.IndexName.L

		// fill primary key name
		isPK := splitOpt.PrimaryKey
		if isPK && indexName == "" {
			indexName = pkName
		}
		if indexName == pkName {
			isPK = true
		}

		if isPK && (pkName != indexName) {
			// specified pk, but incorrect name, or reverse
			return nil, "", dbterror.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
		}
	}

	// default int, it is 1
	colen := 1
	if tbInfo.IsCommonHandle && splitOpt.PrimaryKey {
		pk := tables.FindPrimaryIndex(tbInfo)
		colen = len(pk.Columns)
	} else if indexName != "" {
		idx := tbInfo.FindIndexByName(indexName)
		if idx == nil {
			return nil, "", dbterror.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
		}
		colen = len(idx.Columns)
	}
	if colen != len(splitOpt.SplitOpt.Upper) || colen != len(splitOpt.SplitOpt.Lower) {
		return nil, "", dbterror.ErrInvalidSplitRegionRanges.GenWithStackByArgs("length of index columns and split values differ")
	}

	var buf strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)

	policy := &model.RegionSplitPolicy{
		Regions: splitOpt.SplitOpt.Num,
	}

	policy.Lower = make([]string, len(splitOpt.SplitOpt.Lower))
	for i, expr := range splitOpt.SplitOpt.Lower {
		buf.Reset()
		// validate expr
		d, err := expression.BuildSimpleExpr(ctx, expr)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		if _, err := d.Eval(ctx.GetEvalCtx(), chunk.Row{}); err != nil {
			return nil, "", errors.Trace(err)
		}
		if err := expr.Restore(restoreCtx); err != nil {
			return nil, "", errors.Trace(err)
		}
		policy.Lower[i] = buf.String()
	}

	policy.Upper = make([]string, len(splitOpt.SplitOpt.Upper))
	for i, expr := range splitOpt.SplitOpt.Upper {
		buf.Reset()
		// validate expr
		d, err := expression.BuildSimpleExpr(ctx, expr)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		if _, err := d.Eval(ctx.GetEvalCtx(), chunk.Row{}); err != nil {
			return nil, "", errors.Trace(err)
		}
		if err := expr.Restore(restoreCtx); err != nil {
			return nil, "", errors.Trace(err)
		}
		policy.Upper[i] = buf.String()
	}

	return policy, indexName, nil
}
