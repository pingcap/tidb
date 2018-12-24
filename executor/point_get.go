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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

func (b *executorBuilder) buildPointGet(p *plannercore.PointGetPlan) Executor {
	startTS, err := b.getStartTS()
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	return &PointGetExecutor{
		ctx:     b.ctx,
		schema:  p.Schema(),
		tblInfo: p.TblInfo,
		idxInfo: p.IndexInfo,
		idxVals: p.IndexValues,
		handle:  p.Handle,
		startTS: startTS,
	}
}

// PointGetExecutor executes point select query.
type PointGetExecutor struct {
	ctx      sessionctx.Context
	schema   *expression.Schema
	tps      []*types.FieldType
	tblInfo  *model.TableInfo
	handle   int64
	idxInfo  *model.IndexInfo
	idxVals  []types.Datum
	startTS  uint64
	snapshot kv.Snapshot
	done     bool
}

// Open implements the Executor interface.
func (e *PointGetExecutor) Open(context.Context) error {
	return nil
}

// Close implements the Executor interface.
func (e *PointGetExecutor) Close() error {
	return nil
}

// Next implements the Executor interface.
func (e *PointGetExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true
	var err error
	e.snapshot, err = e.ctx.GetStore().GetSnapshot(kv.Version{Ver: e.startTS})
	if err != nil {
		return errors.Trace(err)
	}
	if e.idxInfo != nil {
		idxKey, err1 := e.encodeIndexKey()
		if err1 != nil {
			return errors.Trace(err1)
		}
		handleVal, err1 := e.get(idxKey)
		if err1 != nil && !kv.ErrNotExist.Equal(err1) {
			return errors.Trace(err1)
		}
		if len(handleVal) == 0 {
			return nil
		}
		e.handle, err1 = tables.DecodeHandle(handleVal)
		if err1 != nil {
			return errors.Trace(err1)
		}
	}
	key := tablecodec.EncodeRowKeyWithHandle(e.tblInfo.ID, e.handle)
	val, err := e.get(key)
	if err != nil && !kv.ErrNotExist.Equal(err) {
		return errors.Trace(err)
	}
	if len(val) == 0 {
		if e.idxInfo != nil {
			return kv.ErrNotExist.GenWithStack("inconsistent extra index %s, handle %d not found in table",
				e.idxInfo.Name.O, e.handle)
		}
		return nil
	}
	return e.decodeRowValToChunk(val, chk)
}

func (e *PointGetExecutor) encodeIndexKey() ([]byte, error) {
	for i := range e.idxVals {
		colInfo := e.tblInfo.Columns[e.idxInfo.Columns[i].Offset]
		casted, err := table.CastValue(e.ctx, e.idxVals[i], colInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.idxVals[i] = casted
	}
	encodedIdxVals, err := codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, nil, e.idxVals...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tablecodec.EncodeIndexSeekKey(e.tblInfo.ID, e.idxInfo.ID, encodedIdxVals), nil
}

func (e *PointGetExecutor) get(key kv.Key) (val []byte, err error) {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if txn != nil && txn.Valid() && !txn.IsReadOnly() {
		return txn.Get(key)
	}
	return e.snapshot.Get(key)
}

func (e *PointGetExecutor) decodeRowValToChunk(rowVal []byte, chk *chunk.Chunk) error {
	//  One column could be filled for multi-times in the schema. e.g. select b, b, c, c from t where a = 1.
	// We need to set the positions in the schema for the same column.
	colID2DecodedPos := make(map[int64]int, e.schema.Len())
	decodedPos2SchemaPos := make([][]int, 0, e.schema.Len())
	for schemaPos, col := range e.schema.Columns {
		if decodedPos, ok := colID2DecodedPos[col.ID]; !ok {
			colID2DecodedPos[col.ID] = len(colID2DecodedPos)
			decodedPos2SchemaPos = append(decodedPos2SchemaPos, []int{schemaPos})
		} else {
			decodedPos2SchemaPos[decodedPos] = append(decodedPos2SchemaPos[decodedPos], schemaPos)
		}
	}
	decodedVals, err := tablecodec.CutRowNew(rowVal, colID2DecodedPos)
	if err != nil {
		return errors.Trace(err)
	}
	if decodedVals == nil {
		decodedVals = make([][]byte, len(colID2DecodedPos))
	}
	decoder := codec.NewDecoder(chk, e.ctx.GetSessionVars().Location())
	for id, decodedPos := range colID2DecodedPos {
		schemaPoses := decodedPos2SchemaPos[decodedPos]
		firstPos := schemaPoses[0]
		if e.tblInfo.PKIsHandle && mysql.HasPriKeyFlag(e.schema.Columns[firstPos].RetType.Flag) {
			chk.AppendInt64(firstPos, e.handle)
			// Fill other positions.
			for i := 1; i < len(schemaPoses); i++ {
				chk.MakeRef(firstPos, schemaPoses[i])
			}
			continue
		}
		// ExtraHandleID is added when building plan, we can make sure that there's only one column's ID is this.
		if id == model.ExtraHandleID {
			chk.AppendInt64(firstPos, e.handle)
			continue
		}
		if len(decodedVals[decodedPos]) == 0 {
			// This branch only entered for updating and deleting. It won't have one column in multiple positions.
			colInfo := getColInfoByID(e.tblInfo, id)
			d, err1 := table.GetColOriginDefaultValue(e.ctx, colInfo)
			if err1 != nil {
				return errors.Trace(err1)
			}
			chk.AppendDatum(firstPos, &d)
			continue
		}
		_, err = decoder.DecodeOne(decodedVals[decodedPos], firstPos, e.schema.Columns[firstPos].RetType)
		if err != nil {
			return errors.Trace(err)
		}
		// Fill other positions.
		for i := 1; i < len(schemaPoses); i++ {
			chk.MakeRef(firstPos, schemaPoses[i])
		}
	}
	return nil
}

func getColInfoByID(tbl *model.TableInfo, colID int64) *model.ColumnInfo {
	for _, col := range tbl.Columns {
		if col.ID == colID {
			return col
		}
	}
	return nil
}

// Schema implements the Executor interface.
func (e *PointGetExecutor) Schema() *expression.Schema {
	return e.schema
}

func (e *PointGetExecutor) retTypes() []*types.FieldType {
	if e.tps == nil {
		e.tps = make([]*types.FieldType, e.schema.Len())
		for i := range e.schema.Columns {
			e.tps[i] = e.schema.Columns[i].RetType
		}
	}
	return e.tps
}

func (e *PointGetExecutor) newFirstChunk() *chunk.Chunk {
	return chunk.New(e.retTypes(), 1, 1)
}
