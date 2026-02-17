// Copyright 2015 PingCAP, Inc.
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
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/unionexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)
func (b *executorBuilder) buildUnionScanExec(v *physicalop.PhysicalUnionScan) exec.Executor {
	oriEncounterUnionScan := b.encounterUnionScan
	b.encounterUnionScan = true
	defer func() {
		b.encounterUnionScan = oriEncounterUnionScan
	}()
	reader := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	return b.buildUnionScanFromReader(reader, v)
}

func collectColIdxFromByItems(byItems []*plannerutil.ByItems, cols []*model.ColumnInfo) ([]int, error) {
	var colIdxs []int
	for _, item := range byItems {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, errors.Errorf("Not support non-column in orderBy pushed down")
		}
		for i, c := range cols {
			if c.ID == col.ID {
				colIdxs = append(colIdxs, i)
				break
			}
		}
	}
	return colIdxs, nil
}

// buildUnionScanFromReader builds union scan executor from child executor.
// Note that this function may be called by inner workers of index lookup join concurrently.
// Be careful to avoid data race.
func (b *executorBuilder) buildUnionScanFromReader(reader exec.Executor, v *physicalop.PhysicalUnionScan) exec.Executor {
	// If reader is union, it means a partition table and we should transfer as above.
	if x, ok := reader.(*unionexec.UnionExec); ok {
		for i, child := range x.AllChildren() {
			x.SetChildren(i, b.buildUnionScanFromReader(child, v))
			if b.err != nil {
				return nil
			}
		}
		return x
	}
	us := &UnionScanExec{BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), reader)}
	// Get the handle column index of the below Plan.
	us.handleCols = v.HandleCols
	us.mutableRow = chunk.MutRowFromTypes(exec.RetTypes(us))

	// If the push-downed condition contains virtual column, we may build a selection upon reader
	originReader := reader
	if sel, ok := reader.(*SelectionExec); ok {
		reader = sel.Children(0)
	}

	us.collators = make([]collate.Collator, 0, len(us.columns))
	for _, tp := range exec.RetTypes(us) {
		us.collators = append(us.collators, collate.GetCollator(tp.GetCollate()))
	}

	startTS, err := b.getSnapshotTS()
	sessionVars := b.ctx.GetSessionVars()
	if err != nil {
		b.err = err
		return nil
	}

	switch x := reader.(type) {
	case *MPPGather:
		us.desc = false
		us.keepOrder = false
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = x.virtualColumnIndex
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *TableReaderExecutor:
		us.desc = x.desc
		us.keepOrder = x.keepOrder
		colIdxes, err := collectColIdxFromByItems(x.byItems, x.columns)
		if err != nil {
			b.err = err
			return nil
		}
		us.usedIndex = colIdxes
		if len(us.usedIndex) > 0 {
			us.needExtraSorting = true
		}
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = x.virtualColumnIndex
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexReaderExecutor:
		us.desc = x.desc
		us.keepOrder = x.keepOrder
		colIdxes, err := collectColIdxFromByItems(x.byItems, x.columns)
		if err != nil {
			b.err = err
			return nil
		}
		us.usedIndex = colIdxes
		if len(us.usedIndex) > 0 {
			us.needExtraSorting = true
		} else {
			for _, ic := range x.index.Columns {
				for i, col := range x.columns {
					if col.Name.L == ic.Name.L {
						us.usedIndex = append(us.usedIndex, i)
						break
					}
				}
			}
		}
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.partitionIDMap = x.partitionIDMap
		us.table = x.table
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexLookUpExecutor:
		us.desc = x.desc
		us.keepOrder = x.keepOrder
		colIdxes, err := collectColIdxFromByItems(x.byItems, x.columns)
		if err != nil {
			b.err = err
			return nil
		}
		us.usedIndex = colIdxes
		if len(us.usedIndex) > 0 {
			us.needExtraSorting = true
		} else {
			for _, ic := range x.index.Columns {
				for i, col := range x.columns {
					if col.Name.L == ic.Name.L {
						us.usedIndex = append(us.usedIndex, i)
						break
					}
				}
			}
		}
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.partitionIDMap = x.partitionIDMap
		us.virtualColumnIndex = buildVirtualColumnIndex(us.Schema(), us.columns)
		us.handleCachedTable(b, x, sessionVars, startTS)
	case *IndexMergeReaderExecutor:
		if len(x.byItems) != 0 {
			us.keepOrder = x.keepOrder
			us.desc = x.byItems[0].Desc
			colIdxes, err := collectColIdxFromByItems(x.byItems, x.columns)
			if err != nil {
				b.err = err
				return nil
			}
			us.usedIndex = colIdxes
			us.needExtraSorting = true
		}
		us.partitionIDMap = x.partitionIDMap
		us.conditions, us.conditionsWithVirCol = physicalop.SplitSelCondsWithVirtualColumn(v.Conditions)
		us.columns = x.columns
		us.table = x.table
		us.virtualColumnIndex = buildVirtualColumnIndex(us.Schema(), us.columns)
	case *PointGetExecutor, *BatchPointGetExec,
		// PointGet and BatchPoint can handle virtual columns and dirty txn data themselves.
		// If TableDual, the result must be empty, so we can skip UnionScan and use TableDual directly here.
		// TableSample only supports sampling from disk, don't need to consider in-memory txn data for simplicity.
		*TableDualExec,
		*TableSampleExecutor:
		return originReader
	default:
		// TODO: consider more operators like Projection.
		b.err = errors.NewNoStackErrorf("unexpected operator %T under UnionScan", reader)
		return nil
	}
	return us
}

type bypassDataSourceExecutor interface {
	dataSourceExecutor
	setDummy()
}

func (us *UnionScanExec) handleCachedTable(b *executorBuilder, x bypassDataSourceExecutor, vars *variable.SessionVars, startTS uint64) {
	tbl := x.Table()
	if tbl.Meta().TableCacheStatusType == model.TableCacheStatusEnable {
		cachedTable := tbl.(table.CachedTable)
		// Determine whether the cache can be used.
		leaseDuration := time.Duration(vardef.TableCacheLease.Load()) * time.Second
		cacheData, loading := cachedTable.TryReadFromCache(startTS, leaseDuration)
		if cacheData != nil {
			vars.StmtCtx.ReadFromTableCache = true
			x.setDummy()
			us.cacheTable = cacheData
		} else if loading {
			return
		} else if !b.inUpdateStmt && !b.inDeleteStmt && !b.inInsertStmt && !vars.StmtCtx.InExplainStmt {
			store := b.ctx.GetStore()
			cachedTable.UpdateLockForRead(context.Background(), store, startTS, leaseDuration)
		}
	}
}
