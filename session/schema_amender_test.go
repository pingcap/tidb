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

type data struct {
	keys   [][]byte
	values [][]byte
}

func prepareTestData(se *session, mutations *tikv.CommitterMutations, oldTblInfo table.Table, newTblInfo table.Table,
	expecetedAmendOps []amendOperationAddIndex, c *C) (*data, *data, tikv.CommitterMutations) {
	var err error
	// Generated test data.
	colIds := make([]int64, len(oldTblInfo.Meta().Columns))
	basicRowValue := make([]types.Datum, len(oldTblInfo.Meta().Columns))
	for i, col := range oldTblInfo.Meta().Columns {
		colIds[i] = oldTblInfo.Meta().Columns[col.Offset].ID
		if col.FieldType.Tp == mysql.TypeLong {
			basicRowValue[i] = types.NewIntDatum(int64(col.Offset))
		} else {
			basicRowValue[i] = types.NewStringDatum(strconv.Itoa(col.Offset))
		}
	}
	KeyOps := []kvrpcpb.Op{kvrpcpb.Op_Put, kvrpcpb.Op_Del, kvrpcpb.Op_Lock, kvrpcpb.Op_Insert}
	rowValues := make([][]types.Datum, len(KeyOps))
	rd := rowcodec.Encoder{Enable: true}
	newData := &data{}
	oldData := &data{}
	expecteMutations := tikv.NewCommiterMutations(8)
	for i := 0; i < len(KeyOps); i++ {
		keyOp := KeyOps[i]
		rowKey := tablecodec.EncodeRowKeyWithHandle(oldTblInfo.Meta().ID, kv.IntHandle(i+1))
		thisRowValue := make([]types.Datum, len(basicRowValue))
		copy(thisRowValue, basicRowValue)
		thisRowValue[0] = types.NewIntDatum(int64(i + 1))
		thisRowValue[4] = types.NewIntDatum(int64(i + 1 + 4))
		var rowValue []byte
		// Test using old row format, the amender will decode this row.
		if keyOp == kvrpcpb.Op_Insert {
			rowValue, err = tablecodec.EncodeOldRow(se.sessionVars.StmtCtx, thisRowValue, colIds, nil, nil)
		} else {
			rowValue, err = rd.Encode(se.sessionVars.StmtCtx, colIds, thisRowValue, nil)
		}
		if keyOp == kvrpcpb.Op_Del {
			oldData.keys = append(oldData.keys, rowKey)
			oldData.values = append(oldData.values, rowValue)
		} else {
			newData.keys = append(newData.keys, rowKey)
			newData.values = append(newData.values, rowValue)
		}
		c.Assert(err, IsNil)
		if keyOp == kvrpcpb.Op_Del {
			mutations.Push(keyOp, rowKey, []byte{}, true)
		} else {
			mutations.Push(keyOp, rowKey, rowValue, true)
		}
		rowValues[i] = thisRowValue
	}
	// Prepare expected results.
	for _, op := range expecetedAmendOps {
		for i := 0; i < len(KeyOps); i++ {
			keyOp := KeyOps[i]
			thisRowValue := rowValues[i]
			indexDatums := make([]types.Datum, len(op.relatedOldIdxCols))
			for colIdx, col := range op.relatedOldIdxCols {
				indexDatums[colIdx] = thisRowValue[col.Offset]
			}
			kvHandle := kv.IntHandle(thisRowValue[0].GetInt64())
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
	return newData, oldData, expecteMutations
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
			// Generated test data.
			mutations := tikv.NewCommiterMutations(8)
			newData, oldData, expectedMutations := prepareTestData(se, &mutations, oldTbInfo, newTblInfo, expecetedAmendOps, c)
			// Prepare old data in table.
			txnPrepare, err := se.store.Begin()
			c.Assert(err, IsNil)
			for i, key := range oldData.keys {
				err = txnPrepare.Set(key, oldData.values[i])
				c.Assert(err, IsNil)
			}
			err = txnPrepare.Commit(ctx)
			c.Assert(err, IsNil)
			txnCheck, err := se.store.Begin()
			c.Assert(err, IsNil)
			snapData, err := txnCheck.GetSnapshot().Get(ctx, oldData.keys[0])
			c.Assert(err, IsNil)
			c.Assert(oldData.values[0], BytesEquals, snapData)
			err = txnCheck.Rollback()
			c.Assert(err, IsNil)

			// Write data for this new transaction, its memory buffer will be used by schema amender.
			txn, err := se.store.Begin()
			c.Assert(err, IsNil)
			se.txn.changeInvalidToValid(txn)
			txn, err = se.Txn(true)
			c.Assert(err, IsNil)
			for i, key := range newData.keys {
				err = txn.Set(key, newData.values[i])
				c.Assert(err, IsNil)
			}
			for _, key := range oldData.keys {
				err = txn.Delete(key)
				c.Assert(err, IsNil)
			}
			c.Assert(err, IsNil)

			schemaAmender := NewSchemaAmenderForTikvTxn(se)
			// Some noisy index key values.
			for i := 0; i < 4; i++ {
				idxValue := []byte("idxValue")
				idxKey := tablecodec.EncodeIndexSeekKey(oldTbInfo.Meta().ID, oldTbInfo.Indices()[2].Meta().ID, idxValue)
				err = txn.Set(idxKey, idxValue)
				c.Assert(err, IsNil)
				mutations.Push(kvrpcpb.Op_Put, idxKey, idxValue, false)
			}

			res, err := schemaAmender.genAllAmendMutations(ctx, mutations, collector)
			c.Assert(err, IsNil)

			// Validate generated results.
			c.Assert(len(res.GetKeys()), Equals, len(res.GetOps()))
			c.Assert(len(res.GetValues()), Equals, len(res.GetOps()))
			c.Assert(len(res.GetPessimisticFlags()), Equals, len(res.GetOps()))
			mutationsEqual(res, &expectedMutations, c)
			err = txn.Rollback()
			c.Assert(err, IsNil)
		}
	}
}
