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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/cteutil"
	"github.com/tikv/client-go/v2/tikv"
)
type emptySampler struct{}

func (*emptySampler) writeChunk(_ *chunk.Chunk) error {
	return nil
}

func (*emptySampler) finished() bool {
	return true
}

func (b *executorBuilder) buildTableSample(v *physicalop.PhysicalTableSample) *TableSampleExecutor {
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	b.ctx.GetSessionVars().StmtCtx.IsTiKV.Store(true)
	e := &TableSampleExecutor{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		table:        v.TableInfo,
		startTS:      startTS,
	}

	tblInfo := v.TableInfo.Meta()
	if tblInfo.TempTableType != model.TempTableNone {
		if tblInfo.TempTableType != model.TempTableGlobal {
			b.err = errors.New("TABLESAMPLE clause can not be applied to local temporary tables")
			return nil
		}
		e.sampler = &emptySampler{}
	} else if v.TableSampleInfo.AstNode.SampleMethod == ast.SampleMethodTypeTiDBRegion {
		e.sampler = newTableRegionSampler(
			b.ctx, v.TableInfo, startTS, v.PhysicalTableID, v.TableSampleInfo.Partitions, v.Schema(),
			v.TableSampleInfo.FullSchema, e.RetFieldTypes(), v.Desc)
	}

	return e
}

func (b *executorBuilder) buildCTE(v *physicalop.PhysicalCTE) exec.Executor {
	if b.Ti != nil {
		b.Ti.UseNonRecursive = true
	}
	if v.RecurPlan != nil && b.Ti != nil {
		b.Ti.UseRecursive = true
	}

	storageMap, ok := b.ctx.GetSessionVars().StmtCtx.CTEStorageMap.(map[int]*CTEStorages)
	if !ok {
		b.err = errors.New("type assertion for CTEStorageMap failed")
		return nil
	}

	chkSize := b.ctx.GetSessionVars().MaxChunkSize
	// iterOutTbl will be constructed in CTEExec.Open().
	var producer *cteProducer
	storages, ok := storageMap[v.CTE.IDForStorage]
	if ok {
		// Storage already setup.
		producer = storages.Producer
	} else {
		if v.SeedPlan == nil {
			b.err = errors.New("cte.seedPlan cannot be nil")
			return nil
		}
		// Build seed part.
		corCols := plannercore.ExtractOuterApplyCorrelatedCols(v.SeedPlan)
		seedExec := b.build(v.SeedPlan)
		if b.err != nil {
			return nil
		}

		// Setup storages.
		tps := seedExec.RetFieldTypes()
		resTbl := cteutil.NewStorageRowContainer(tps, chkSize)
		if err := resTbl.OpenAndRef(); err != nil {
			b.err = err
			return nil
		}
		iterInTbl := cteutil.NewStorageRowContainer(tps, chkSize)
		if err := iterInTbl.OpenAndRef(); err != nil {
			b.err = err
			return nil
		}
		storageMap[v.CTE.IDForStorage] = &CTEStorages{ResTbl: resTbl, IterInTbl: iterInTbl}

		// Build recursive part.
		var recursiveExec exec.Executor
		if v.RecurPlan != nil {
			recursiveExec = b.build(v.RecurPlan)
			if b.err != nil {
				return nil
			}
			corCols = append(corCols, plannercore.ExtractOuterApplyCorrelatedCols(v.RecurPlan)...)
		}

		var sel []int
		if v.CTE.IsDistinct {
			sel = make([]int, chkSize)
			for i := range chkSize {
				sel[i] = i
			}
		}

		var corColHashCodes [][]byte
		for _, corCol := range corCols {
			corColHashCodes = append(corColHashCodes, getCorColHashCode(corCol))
		}

		producer = &cteProducer{
			ctx:             b.ctx,
			seedExec:        seedExec,
			recursiveExec:   recursiveExec,
			resTbl:          resTbl,
			iterInTbl:       iterInTbl,
			isDistinct:      v.CTE.IsDistinct,
			sel:             sel,
			hasLimit:        v.CTE.HasLimit,
			limitBeg:        v.CTE.LimitBeg,
			limitEnd:        v.CTE.LimitEnd,
			corCols:         corCols,
			corColHashCodes: corColHashCodes,
		}
		storageMap[v.CTE.IDForStorage].Producer = producer
	}

	return &CTEExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		producer:     producer,
	}
}

func (b *executorBuilder) buildCTETableReader(v *physicalop.PhysicalCTETable) exec.Executor {
	storageMap, ok := b.ctx.GetSessionVars().StmtCtx.CTEStorageMap.(map[int]*CTEStorages)
	if !ok {
		b.err = errors.New("type assertion for CTEStorageMap failed")
		return nil
	}
	storages, ok := storageMap[v.IDForStorage]
	if !ok {
		b.err = errors.Errorf("iterInTbl should already be set up by CTEExec(id: %d)", v.IDForStorage)
		return nil
	}
	return &CTETableReaderExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		iterInTbl:    storages.IterInTbl,
		chkIdx:       0,
	}
}

func (b *executorBuilder) validCanReadTemporaryOrCacheTable(tbl *model.TableInfo) error {
	err := b.validCanReadTemporaryTable(tbl)
	if err != nil {
		return err
	}
	return b.validCanReadCacheTable(tbl)
}

func (b *executorBuilder) validCanReadCacheTable(tbl *model.TableInfo) error {
	if tbl.TableCacheStatusType == model.TableCacheStatusDisable {
		return nil
	}

	sessionVars := b.ctx.GetSessionVars()

	// Temporary table can't switch into cache table. so the following code will not cause confusion
	if sessionVars.TxnCtx.IsStaleness || b.isStaleness {
		return errors.Trace(errors.New("can not stale read cache table"))
	}

	return nil
}

func (b *executorBuilder) validCanReadTemporaryTable(tbl *model.TableInfo) error {
	if tbl.TempTableType == model.TempTableNone {
		return nil
	}

	// Some tools like dumpling use history read to dump all table's records and will be fail if we return an error.
	// So we do not check SnapshotTS here

	sessionVars := b.ctx.GetSessionVars()

	if tbl.TempTableType == model.TempTableLocal && sessionVars.SnapshotTS != 0 {
		return errors.New("can not read local temporary table when 'tidb_snapshot' is set")
	}

	if sessionVars.TxnCtx.IsStaleness || b.isStaleness {
		return errors.New("can not stale read temporary table")
	}

	return nil
}

func (b *executorBuilder) getCacheTable(tblInfo *model.TableInfo, startTS uint64) kv.MemBuffer {
	tbl, ok := b.is.TableByID(context.Background(), tblInfo.ID)
	if !ok {
		b.err = errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(b.ctx.GetSessionVars().CurrentDB, tblInfo.Name))
		return nil
	}
	sessVars := b.ctx.GetSessionVars()
	leaseDuration := time.Duration(vardef.TableCacheLease.Load()) * time.Second
	cacheData, loading := tbl.(table.CachedTable).TryReadFromCache(startTS, leaseDuration)
	if cacheData != nil {
		sessVars.StmtCtx.ReadFromTableCache = true
		return cacheData
	} else if loading {
		return nil
	}
	if !b.ctx.GetSessionVars().StmtCtx.InExplainStmt && !b.inDeleteStmt && !b.inUpdateStmt {
		tbl.(table.CachedTable).UpdateLockForRead(context.Background(), b.ctx.GetStore(), startTS, leaseDuration)
	}
	return nil
}

func (b *executorBuilder) buildCompactTable(v *plannercore.CompactTable) exec.Executor {
	if v.ReplicaKind != ast.CompactReplicaKindTiFlash && v.ReplicaKind != ast.CompactReplicaKindAll {
		b.err = errors.Errorf("compact %v replica is not supported", strings.ToLower(string(v.ReplicaKind)))
		return nil
	}

	store := b.ctx.GetStore()
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		b.err = errors.New("compact tiflash replica can only run with tikv compatible storage")
		return nil
	}

	var partitionIDs []int64
	if v.PartitionNames != nil {
		if v.TableInfo.Partition == nil {
			b.err = errors.Errorf("table:%s is not a partition table, but user specify partition name list:%+v", v.TableInfo.Name.O, v.PartitionNames)
			return nil
		}
		// use map to avoid FindPartitionDefinitionByName
		partitionMap := map[string]int64{}
		for _, partition := range v.TableInfo.Partition.Definitions {
			partitionMap[partition.Name.L] = partition.ID
		}

		for _, partitionName := range v.PartitionNames {
			partitionID, ok := partitionMap[partitionName.L]
			if !ok {
				b.err = table.ErrUnknownPartition.GenWithStackByArgs(partitionName.O, v.TableInfo.Name.O)
				return nil
			}
			partitionIDs = append(partitionIDs, partitionID)
		}
		if b.Ti.PartitionTelemetry == nil {
			b.Ti.PartitionTelemetry = &PartitionTelemetryInfo{}
		}
		b.Ti.PartitionTelemetry.UseCompactTablePartition = true
	}

	return &CompactTableTiFlashExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tableInfo:    v.TableInfo,
		partitionIDs: partitionIDs,
		tikvStore:    tikvStore,
	}
}

func (b *executorBuilder) buildAdminShowBDRRole(v *plannercore.AdminShowBDRRole) exec.Executor {
	return &AdminShowBDRRoleExec{BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())}
}

func (b *executorBuilder) buildRecommendIndex(v *plannercore.RecommendIndexPlan) exec.Executor {
	return &RecommendIndexExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		Action:       v.Action,
		SQL:          v.SQL,
		AdviseID:     v.AdviseID,
		Options:      v.Options,
	}
}

func (b *executorBuilder) buildWorkloadRepoCreate(_ *plannercore.WorkloadRepoCreate) exec.Executor {
	base := exec.NewBaseExecutor(b.ctx, nil, 0)
	return &WorkloadRepoCreateExec{base}
}
