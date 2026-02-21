// Copyright 2018 PingCAP, Inc.
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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// CleanupIndexExec represents a cleanup index executor.
// It is built from "admin cleanup index" statement, is used to delete
// dangling index data.
type CleanupIndexExec struct {
	exec.BaseExecutor

	done      bool
	removeCnt uint64

	index      table.Index
	table      table.Table
	physicalID int64

	columns          []*model.ColumnInfo
	idxColFieldTypes []*types.FieldType
	idxChunk         *chunk.Chunk
	handleCols       plannercore.HandleCols

	idxValues   *kv.HandleMap // kv.Handle -> [][]types.Datum
	batchSize   uint64
	batchKeys   []kv.Key
	idxValsBufs [][]types.Datum
	lastIdxKey  []byte
	scanRowCnt  uint64
}

func (e *CleanupIndexExec) getIdxColTypes() []*types.FieldType {
	if e.idxColFieldTypes != nil {
		return e.idxColFieldTypes
	}
	e.idxColFieldTypes = make([]*types.FieldType, 0, len(e.columns))
	for _, col := range e.columns {
		e.idxColFieldTypes = append(e.idxColFieldTypes, col.FieldType.ArrayType())
	}
	return e.idxColFieldTypes
}

func (e *CleanupIndexExec) batchGetRecord(txn kv.Transaction) (map[string][]byte, error) {
	e.idxValues.Range(func(h kv.Handle, _ any) bool {
		e.batchKeys = append(e.batchKeys, tablecodec.EncodeRecordKey(e.table.RecordPrefix(), h))
		return true
	})
	values, err := kv.BatchGetValue(context.Background(), txn, e.batchKeys)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (e *CleanupIndexExec) deleteDanglingIdx(txn kv.Transaction, values map[string][]byte) error {
	for _, k := range e.batchKeys {
		if _, found := values[string(k)]; !found {
			pid, handle, err := tablecodec.DecodeRecordKey(k)
			if err != nil {
				return err
			}
			if e.index.Meta().Global {
				handle = kv.NewPartitionHandle(pid, handle)
			}
			handleIdxValsGroup, ok := e.idxValues.Get(handle)
			if !ok {
				return errors.Trace(errors.Errorf("batch keys are inconsistent with handles"))
			}
			for _, handleIdxVals := range handleIdxValsGroup.([][]types.Datum) {
				if err := e.index.Delete(e.Ctx().GetTableCtx(), txn, handleIdxVals, handle); err != nil {
					return err
				}
				e.removeCnt++
				if e.removeCnt%e.batchSize == 0 {
					logutil.BgLogger().Info("clean up dangling index", zap.String("table", e.table.Meta().Name.String()),
						zap.String("index", e.index.Meta().Name.String()), zap.Uint64("count", e.removeCnt))
				}
			}
		}
	}
	return nil
}

func extractIdxVals(row chunk.Row, idxVals []types.Datum,
	fieldTypes []*types.FieldType, idxValLen int) []types.Datum {
	if cap(idxVals) < idxValLen {
		idxVals = make([]types.Datum, idxValLen)
	} else {
		idxVals = idxVals[:idxValLen]
	}

	for i := range idxValLen {
		colVal := row.GetDatum(i, fieldTypes[i])
		colVal.Copy(&idxVals[i])
	}
	return idxVals
}

func (e *CleanupIndexExec) fetchIndex(ctx context.Context, txn kv.Transaction) error {
	result, err := e.buildIndexScan(ctx, txn)
	if err != nil {
		return err
	}
	defer terror.Call(result.Close)

	sc := e.Ctx().GetSessionVars().StmtCtx
	idxColLen := len(e.index.Meta().Columns)
	for {
		err := result.Next(ctx, e.idxChunk)
		if err != nil {
			return err
		}
		if e.idxChunk.NumRows() == 0 {
			return nil
		}
		iter := chunk.NewIterator4Chunk(e.idxChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle, err := e.handleCols.BuildHandle(sc, row)
			if err != nil {
				return err
			}
			if e.index.Meta().Global {
				handle = kv.NewPartitionHandle(row.GetInt64(row.Len()-1), handle)
			}
			idxVals := extractIdxVals(row, e.idxValsBufs[e.scanRowCnt], e.idxColFieldTypes, idxColLen)
			e.idxValsBufs[e.scanRowCnt] = idxVals
			existingIdxVals, ok := e.idxValues.Get(handle)
			if ok {
				updatedIdxVals := append(existingIdxVals.([][]types.Datum), idxVals)
				e.idxValues.Set(handle, updatedIdxVals)
			} else {
				e.idxValues.Set(handle, [][]types.Datum{idxVals})
			}
			idxKey, _, err := e.index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), idxVals, handle, nil)
			if err != nil {
				return err
			}
			e.scanRowCnt++
			e.lastIdxKey = idxKey
			if e.scanRowCnt >= e.batchSize {
				return nil
			}
		}
	}
}

// Next implements the Executor Next interface.
func (e *CleanupIndexExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	cleaningClusteredPrimaryKey := e.table.Meta().IsCommonHandle && e.index.Meta().Primary
	if cleaningClusteredPrimaryKey {
		e.done = true
		req.AppendUint64(0, 0)
		return nil
	}

	var err error
	if tbl, ok := e.table.(table.PartitionedTable); ok && !e.index.Meta().Global {
		pi := e.table.Meta().GetPartitionInfo()
		for _, p := range pi.Definitions {
			e.table = tbl.GetPartition(p.ID)
			e.index = tables.GetWritableIndexByName(e.index.Meta().Name.L, e.table)
			e.physicalID = p.ID
			err = e.init()
			if err != nil {
				return err
			}
			err = e.cleanTableIndex(ctx)
			if err != nil {
				return err
			}
		}
	} else {
		err = e.cleanTableIndex(ctx)
		if err != nil {
			return err
		}
	}
	e.done = true
	req.AppendUint64(0, e.removeCnt)
	return nil
}

func (e *CleanupIndexExec) cleanTableIndex(ctx context.Context) error {
	for {
		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)
		errInTxn := kv.RunInNewTxn(ctx, e.Ctx().GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
			txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
			setOptionForTopSQL(e.Ctx().GetSessionVars().StmtCtx, txn)
			err := e.fetchIndex(ctx, txn)
			if err != nil {
				return err
			}
			values, err := e.batchGetRecord(txn)
			if err != nil {
				return err
			}
			err = e.deleteDanglingIdx(txn, values)
			if err != nil {
				return err
			}
			return nil
		})
		if errInTxn != nil {
			return errInTxn
		}
		if e.scanRowCnt == 0 {
			break
		}
		e.scanRowCnt = 0
		e.batchKeys = e.batchKeys[:0]
		e.idxValues.Range(func(h kv.Handle, _ any) bool {
			e.idxValues.Delete(h)
			return true
		})
	}
	return nil
}

func (e *CleanupIndexExec) buildIndexScan(ctx context.Context, txn kv.Transaction) (distsql.SelectResult, error) {
	dagPB, err := e.buildIdxDAGPB()
	if err != nil {
		return nil, err
	}
	var builder distsql.RequestBuilder
	ranges := ranger.FullRange()
	keyRanges, err := distsql.IndexRangesToKVRanges(e.Ctx().GetDistSQLCtx(), e.physicalID, e.index.Meta().ID, ranges)
	if err != nil {
		return nil, err
	}
	err = keyRanges.SetToNonPartitioned()
	if err != nil {
		return nil, err
	}
	keyRanges.FirstPartitionRange()[0].StartKey = kv.Key(e.lastIdxKey).PrefixNext()
	kvReq, err := builder.SetWrappedKeyRanges(keyRanges).
		SetDAGRequest(dagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromSessionVars(e.Ctx().GetDistSQLCtx()).
		SetFromInfoSchema(e.Ctx().GetInfoSchema()).
		SetConnIDAndConnAlias(e.Ctx().GetSessionVars().ConnectionID, e.Ctx().GetSessionVars().SessionAlias).
		Build()
	if err != nil {
		return nil, err
	}

	kvReq.Concurrency = 1
	result, err := distsql.Select(ctx, e.Ctx().GetDistSQLCtx(), kvReq, e.getIdxColTypes())
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Open implements the Executor Open interface.
func (e *CleanupIndexExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	return e.init()
}

func (e *CleanupIndexExec) init() error {
	e.idxChunk = chunk.New(e.getIdxColTypes(), e.InitCap(), e.MaxChunkSize())
	e.idxValues = kv.NewHandleMap()
	e.batchKeys = make([]kv.Key, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Datum, e.batchSize)
	sc := e.Ctx().GetSessionVars().StmtCtx
	idxKey, _, err := e.index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), []types.Datum{{}}, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return err
	}
	e.lastIdxKey = idxKey
	return nil
}

func (e *CleanupIndexExec) buildIdxDAGPB() (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.Ctx().GetSessionVars().Location())
	sc := e.Ctx().GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	for i := range e.columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	execPB := e.constructIndexScanPB()
	dagReq.Executors = append(dagReq.Executors, execPB)
	err := tables.SetPBColumnsDefaultValue(e.Ctx().GetExprCtx(), dagReq.Executors[0].IdxScan.Columns, e.columns)
	if err != nil {
		return nil, err
	}

	limitExec := e.constructLimitPB()
	dagReq.Executors = append(dagReq.Executors, limitExec)
	distsql.SetEncodeType(e.Ctx().GetDistSQLCtx(), dagReq)
	return dagReq, nil
}

func (e *CleanupIndexExec) constructIndexScanPB() *tipb.Executor {
	idxExec := &tipb.IndexScan{
		TableId:          e.physicalID,
		IndexId:          e.index.Meta().ID,
		Columns:          util.ColumnsToProto(e.columns, e.table.Meta().PKIsHandle, true, false),
		PrimaryColumnIds: tables.TryGetCommonPkColumnIds(e.table.Meta()),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}
}

func (e *CleanupIndexExec) constructLimitPB() *tipb.Executor {
	limitExec := &tipb.Limit{
		Limit: e.batchSize,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

// Close implements the Executor Close interface.
func (*CleanupIndexExec) Close() error {
	return nil
}
