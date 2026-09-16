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

package executor

import (
	"bytes"
	"context"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/tikv/client-go/v2/tikv"
)

// PrepareImportQueryRanges selects the range path only when independent executions
// can concatenate their results. Unsupported queries retain whole-query execution.
func PrepareImportQueryRanges(
	ctx context.Context, sctx sessionctx.Context, node ast.StmtNode,
	p base.PhysicalPlan, plan *importer.Plan,
) error {
	plan.MaxNodeCnt = 1
	q := plan.Query
	validator := &importRangeQueryValidator{}
	ast.Walk(node, validator)
	scan := importRangeTableScan(p)
	if !validator.valid() || scan == nil {
		return nil
	}
	store, ok := sctx.GetStore().(tikv.Storage)
	if !ok {
		return nil
	}
	nodeLimit := scheduler.CalcMaxNodeCountByStoresNum(ctx, sctx.GetStore())
	if nodeLimit <= 1 {
		return nil
	}
	start := tablecodec.GenTableRecordPrefix(scan.Table.ID)
	end := start.PrefixNext()
	regions, err := store.GetRegionCache().LoadRegionsInKeyRange(tikv.NewBackofferWithVars(ctx, 20000, nil), start, end)
	if err != nil {
		return err
	}
	// Region boundaries are only split hints. Sorting and deduplicating the keys
	// keeps coverage exact even if Region splits/merges overlap PD scan pages.
	boundaries := []kv.Key{start}
	for _, region := range regions {
		key := kv.Key(region.StartKey())
		if key.Cmp(start) > 0 && key.Cmp(end) < 0 {
			boundaries = append(boundaries, key)
		}
	}
	slices.SortFunc(boundaries, func(a, b kv.Key) int { return a.Cmp(b) })
	boundaries = slices.CompactFunc(boundaries, func(a, b kv.Key) bool { return bytes.Equal(a, b) })
	boundaries = append(boundaries, end)
	// Use Region count as the first size proxy, with up to four tasks per worker
	// for load balancing. Byte-based sizing can replace this without changing execution.
	batch := max(1, (len(boundaries)-2)/(nodeLimit*4)+1)
	var ranges []kv.KeyRange
	for i := 0; i < len(boundaries)-1; i += batch {
		ranges = append(ranges, kv.KeyRange{StartKey: boundaries[i], EndKey: boundaries[min(i+batch, len(boundaries)-1)]})
	}
	if len(ranges) <= 1 {
		return nil
	}
	ver, err := sctx.GetStore().CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return err
	}
	q.Scan = &importer.QueryScan{TableID: scan.Table.ID, ReadTS: ver.Ver, Ranges: ranges}
	plan.MaxNodeCnt = min(nodeLimit, len(ranges))
	return nil
}

// Check the AST as well as the physical plan: constant folding can hide NOW(),
// session-dependent functions or an already evaluated scalar subquery in a plan.
type importRangeQueryValidator struct {
	selects, tables int
	rejected        bool
}

func (v *importRangeQueryValidator) valid() bool {
	return !v.rejected && v.selects == 1 && v.tables == 1
}
func (v *importRangeQueryValidator) Enter(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.SelectStmt:
		v.selects++
		// The submitted physical plan may depend on hints that workers discard.
		// Keep hinted submissions whole instead of splitting a different access path.
		v.rejected = v.rejected || n.Distinct || n.GroupBy != nil || n.Having != nil || n.OrderBy != nil ||
			n.Limit != nil || n.LockInfo != nil || n.With != nil || len(n.WindowSpecs) != 0 || n.SelectIntoOpt != nil || len(n.TableHints) != 0
	case *ast.TableName:
		v.tables++
		v.rejected = n.TableSample != nil || n.AsOf != nil || len(n.IndexHints) != 0
	case *ast.FuncCallExpr, *ast.AggregateFuncExpr, *ast.WindowFuncExpr, *ast.SubqueryExpr,
		*ast.VariableExpr, ast.ParamMarkerExpr, *ast.SetOprStmt, *ast.DefaultExpr:
		// Start with column/literal expressions, arithmetic, comparisons and casts.
		// Function calls can be added with an explicit determinism contract later.
		v.rejected = true
	}
	return v.rejected
}
func (v *importRangeQueryValidator) Leave(ast.Node) bool { return !v.rejected }

func importRangeTableScan(p base.PhysicalPlan) *physicalop.PhysicalTableScan {
	switch x := p.(type) {
	case *physicalop.PhysicalProjection:
		if slices.ContainsFunc(x.Exprs, expression.IsMutableEffectsExpr) {
			return nil
		}
	case *physicalop.PhysicalSelection:
		if slices.ContainsFunc(x.Conditions, expression.IsMutableEffectsExpr) {
			return nil
		}
	case *physicalop.PhysicalTableReader:
		if x.StoreType != kv.TiKV {
			return nil
		}
		return importRangeTableScan(x.GetTablePlan())
	case *physicalop.PhysicalTableScan:
		if x.Table.GetPartitionInfo() != nil || x.StoreType != kv.TiKV || len(x.ByItems) != 0 {
			return nil
		}
		for _, col := range x.Columns {
			if col.IsGenerated() {
				return nil
			}
		}
		return x
	default:
		return nil
	}
	if len(p.Children()) != 1 {
		return nil
	}
	return importRangeTableScan(p.Children()[0])
}

func setImportQueryRange(e exec.Executor, r *kv.KeyRange) error {
	if reader, ok := e.(*TableReaderExecutor); ok {
		reader.kvRangeBuilder = importQueryRangeBuilder{
			tableID: reader.table.Meta().ID, commonHandle: reader.table.Meta().IsCommonHandle, bound: *r,
		}
		return nil
	}
	if len(e.AllChildren()) != 1 {
		return errors.New("import query range requires a single table reader")
	}
	return setImportQueryRange(e.AllChildren()[0], r)
}

// Use the reader's existing range-builder interface so every generated request,
// including unsigned-handle range groups, intersects with the assigned subtask.
type importQueryRangeBuilder struct {
	tableID      int64
	commonHandle bool
	bound        kv.KeyRange
}

func (b importQueryRangeBuilder) buildKeyRange(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([][]kv.KeyRange, error) {
	keys, err := distsql.TableHandleRangesToKVRanges(dctx, []int64{b.tableID}, b.commonHandle, ranges)
	if err != nil {
		return nil, err
	}
	bounded := make([]kv.KeyRange, 0)
	for _, r := range keys.FirstPartitionRange() {
		if r.StartKey.Cmp(b.bound.StartKey) < 0 {
			r.StartKey = b.bound.StartKey
		}
		if r.EndKey.Cmp(b.bound.EndKey) > 0 {
			r.EndKey = b.bound.EndKey
		}
		if r.StartKey.Cmp(r.EndKey) < 0 {
			bounded = append(bounded, r)
		}
	}
	return [][]kv.KeyRange{bounded}, nil
}

func (b importQueryRangeBuilder) buildKeyRangeSeparately(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([]int64, [][]kv.KeyRange, error) {
	keys, err := b.buildKeyRange(dctx, ranges)
	return []int64{b.tableID}, keys, err
}
