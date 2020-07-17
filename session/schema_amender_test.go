// Copyright 2020 PingCAP, Inc.
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

package session

import (
	"context"
	"sort"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
)

var _ = SerialSuites(&testSchemaAmenderSuite{})

type testSchemaAmenderSuite struct {
}

func (s *testSchemaAmenderSuite) SetUpSuite(c *C) {
}

func (s *testSchemaAmenderSuite) TearDownSuite(c *C) {
}

func initTblColIdxID(metaInfo *model.TableInfo) {
	for i, col := range metaInfo.Columns {
		col.ID = int64(i + 1)
	}
	for i, idx := range metaInfo.Indices {
		idx.ID = int64(i + 1)
	}
	metaInfo.ID = 1
	metaInfo.State = model.StatePublic
}

func mutationsEqual(res *tikv.CommitterMutations, expected *tikv.CommitterMutations, c *C) {
	c.Assert(len(res.GetKeys()), Equals, len(expected.GetKeys()))
	for i := 0; i < len(res.GetKeys()); i++ {
		c.Assert(res.GetOps()[i], Equals, expected.GetOps()[i])
		c.Assert(res.GetKeys()[i], BytesEquals, expected.GetKeys()[i])
		c.Assert(res.GetValues()[i], BytesEquals, expected.GetValues()[i])
		c.Assert(res.GetPessimisticFlags()[i], Equals, expected.GetPessimisticFlags()[i])
	}
}

func (s *testSchemaAmenderSuite) TestAmendCollectAndGenMutations(c *C) {
	ctx := context.Background()
	store := newStore(c, "test_schema_amender")
	defer store.Close()
	se := &session{
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	startStates := []model.SchemaState{model.StateNone, model.StateDeleteOnly}
	for _, startState := range startStates {
		endStatMap := ConstOpAddIndex[startState]
		var endStates []model.SchemaState
		for st := range endStatMap {
			endStates = append(endStates, st)
		}
		sort.Slice(endStates, func(i, j int) bool { return endStates[i] < endStates[j] })
		for _, endState := range endStates {
			// column: a, b, c, d, e, c_str, d_str, e_str, f, g.
			// PK: a.
			// indices: c_d_e, e, f, g, f_g, c_d_e_str, c_d_e_str_prefix.
			oldTblMeta := core.MockSignedTable()
			initTblColIdxID(oldTblMeta)
			// Indices[0] does not exist at the start.
			oldTblMeta.Indices = oldTblMeta.Indices[1:]
			oldTbInfo, err := table.TableFromMeta(nil, oldTblMeta)
			c.Assert(err, IsNil)
			oldTblMeta.Indices[0].State = startState
			oldTblMeta.Indices[2].State = endState

			newTblMeta := core.MockSignedTable()
			initTblColIdxID(newTblMeta)
			// colh is newly added.
			colh := &model.ColumnInfo{
				State:     model.StatePublic,
				Offset:    12,
				Name:      model.NewCIStr("b"),
				FieldType: *(types.NewFieldType(mysql.TypeLong)),
				ID:        13,
			}
			newTblMeta.Columns = append(newTblMeta.Columns, colh)
			// The last index "c_d_e_str_prefix is dropped.
			newTblMeta.Indices = newTblMeta.Indices[:len(newTblMeta.Indices)-1]
			newTblMeta.Indices[0].Unique = false
			newTblInfo, err := table.TableFromMeta(nil, newTblMeta)
			c.Assert(err, IsNil)
			newTblMeta.Indices[0].State = endState
			// Indices[1] is newly created.
			newTblMeta.Indices[1].State = endState
			// Indices[3] is dropped
			newTblMeta.Indices[3].State = startState

			// Only the add index amend operations is collected in the results.
			collector := newAmendCollector()
			tblID := int64(1)
			err = collector.collectTblAmendOps(se, tblID, oldTbInfo, newTblInfo)
			c.Assert(err, IsNil)
			c.Assert(len(collector.tblAmendOpMap[tblID]), Equals, 2)
			expecetedAmendOps := []amendOperationAddIndex{
				{AmendOpType: ConstOpAddIndex[model.StateNone][endState],
					tblInfoAtStart:    oldTbInfo,
					tblInfoAtCommit:   newTblInfo,
					indexInfoAtStart:  nil,
					indexInfoAtCommit: newTblInfo.Indices()[0],
					relatedOldIdxCols: []*table.Column{oldTbInfo.Cols()[2], oldTbInfo.Cols()[3], oldTbInfo.Cols()[4]},
				},
				{AmendOpType: ConstOpAddIndex[startState][endState],
					tblInfoAtStart:    oldTbInfo,
					tblInfoAtCommit:   newTblInfo,
					indexInfoAtStart:  oldTbInfo.Indices()[0],
					indexInfoAtCommit: newTblInfo.Indices()[1],
					relatedOldIdxCols: []*table.Column{oldTbInfo.Cols()[4]},
				},
			}
			// Check collect results.
			for i, amendOp := range collector.tblAmendOpMap[tblID] {
				curOp, ok := amendOp.(*amendOperationAddIndex)
				c.Assert(ok, IsTrue)
				c.Assert(curOp.AmendOpType, Equals, expecetedAmendOps[i].AmendOpType)
				c.Assert(curOp.tblInfoAtStart, Equals, expecetedAmendOps[i].tblInfoAtStart)
				c.Assert(curOp.tblInfoAtCommit, Equals, expecetedAmendOps[i].tblInfoAtCommit)
				c.Assert(curOp.indexInfoAtStart, Equals, expecetedAmendOps[i].indexInfoAtStart)
				c.Assert(curOp.indexInfoAtCommit, Equals, expecetedAmendOps[i].indexInfoAtCommit)
				for j, col := range curOp.relatedOldIdxCols {
					c.Assert(col, Equals, expecetedAmendOps[i].relatedOldIdxCols[j])
				}
			}
			// Check mutation generations.
			txn, err := se.store.Begin()
			c.Assert(err, IsNil)
			se.txn.changeInvalidToValid(txn)
			txn, err = se.Txn(true)
			c.Assert(err, IsNil)

			schemaAmender := NewSchemaAmenderForTikvTxn(se)
			mutations := tikv.NewCommiterMutations(8)
			colIds := make([]int64, len(oldTbInfo.Meta().Columns))
			basicRowValue := make([]types.Datum, len(oldTbInfo.Meta().Columns))
			rd := rowcodec.Encoder{Enable: true}
			for i, col := range oldTbInfo.Meta().Columns {
				colIds[i] = oldTbInfo.Meta().Columns[col.Offset].ID
				if col.FieldType.Tp == mysql.TypeLong {
					basicRowValue[i] = types.NewIntDatum(int64(col.Offset))
				} else {
					basicRowValue[i] = types.NewStringDatum(strconv.Itoa(col.Offset))
				}
				c.Assert(err, IsNil)
			}
			expecteMutations := tikv.NewCommiterMutations(8)
			KeyOps := []kvrpcpb.Op{kvrpcpb.Op_Put, kvrpcpb.Op_Del, kvrpcpb.Op_Lock, kvrpcpb.Op_Insert}
			rowValues := make([][]types.Datum, len(KeyOps))
			for i := 0; i < len(KeyOps); i++ {
				rowKey := tablecodec.EncodeRowKeyWithHandle(oldTbInfo.Meta().ID, kv.IntHandle(i+1))
				thisRowValue := make([]types.Datum, len(basicRowValue))
				copy(thisRowValue, basicRowValue)
				thisRowValue[0] = types.NewIntDatum(int64(i + 1))
				thisRowValue[4] = types.NewIntDatum(int64(i + 1 + 4))
				rowValue, err := rd.Encode(se.sessionVars.StmtCtx, colIds, thisRowValue, nil)
				c.Assert(err, IsNil)
				err = txn.Set(rowKey, rowValue)
				c.Assert(err, IsNil)
				keyOp := KeyOps[i]
				mutations.Push(keyOp, rowKey, rowValue, true)
				rowValues[i] = thisRowValue
			}
			for _, op := range expecetedAmendOps {
				for rowIdx, curRow := range rowValues {
					keyOp := KeyOps[rowIdx]
					indexDatums := make([]types.Datum, len(op.relatedOldIdxCols))
					for colIdx, col := range op.relatedOldIdxCols {
						indexDatums[colIdx] = curRow[col.Offset]
					}
					kvHandle := kv.IntHandle(curRow[0].GetInt64())
					idxKey, _, err := tablecodec.GenIndexKey(se.sessionVars.StmtCtx, newTblInfo.Meta(),
						op.indexInfoAtCommit.Meta(), newTblInfo.Meta().ID, indexDatums, kvHandle, nil)
					c.Assert(err, IsNil)
					var idxVal []byte
					if (op.AmendOpType == AmendNeedAddDelete || op.AmendOpType == AmendNeedAddDeleteAndInsert) && isDeleteOp(keyOp) {
						expecteMutations.Push(keyOp, idxKey, idxVal, false)
					}
					if (op.AmendOpType == AmendNeedAddDeleteAndInsert || op.AmendOpType == AmendNeedAddInsert) && isInsertOp(keyOp) {
						idxVal, err = tablecodec.GenIndexValue(se.sessionVars.StmtCtx, newTblInfo.Meta(), op.indexInfoAtCommit.Meta(),
							false, op.indexInfoAtCommit.Meta().Unique, false, indexDatums, kvHandle)
						c.Assert(err, IsNil)
						expecteMutations.Push(keyOp, idxKey, idxVal, false)
					}
				}
			}
			for i := 0; i < 4; i++ {
				idxValue := []byte("idxValue")
				idxKey := tablecodec.EncodeIndexSeekKey(oldTbInfo.Meta().ID, oldTbInfo.Indices()[2].Meta().ID, idxValue)
				err = txn.Set(idxKey, idxKey)
				c.Assert(err, IsNil)
				mutations.Push(kvrpcpb.Op_Put, idxKey, idxValue, false)
			}

			res, err := schemaAmender.genAllAmendMutations(ctx, mutations, collector)
			c.Assert(err, IsNil)

			// Validate generated results.
			c.Assert(len(res.GetKeys()), Equals, len(res.GetOps()))
			c.Assert(len(res.GetValues()), Equals, len(res.GetOps()))
			c.Assert(len(res.GetPessimisticFlags()), Equals, len(res.GetOps()))
			mutationsEqual(res, &expecteMutations, c)
			err = txn.Rollback()
			c.Assert(err, IsNil)
		}
	}
}
