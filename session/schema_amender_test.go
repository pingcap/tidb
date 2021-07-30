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
	"bytes"
	"context"
	"sort"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
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
		if idx.Name.L == "f_g" {
			idx.Unique = true
		} else {
			idx.Unique = false
		}
	}
	metaInfo.ID = 1
	metaInfo.State = model.StatePublic
}

func mutationsEqual(res *tikv.PlainMutations, expected *tikv.PlainMutations, c *C) {
	c.Assert(len(res.GetKeys()), Equals, len(expected.GetKeys()))
	for i := 0; i < len(res.GetKeys()); i++ {
		foundIdx := -1
		for j := 0; j < len(expected.GetKeys()); j++ {
			if bytes.Equal(res.GetKeys()[i], expected.GetKeys()[j]) {
				foundIdx = j
				break
			}
		}
		c.Assert(foundIdx, GreaterEqual, 0)
		c.Assert(res.GetOps()[i], Equals, expected.GetOps()[foundIdx])
		c.Assert(res.GetPessimisticFlags()[i], Equals, expected.GetPessimisticFlags()[foundIdx])
		c.Assert(res.GetKeys()[i], BytesEquals, expected.GetKeys()[foundIdx])
		c.Assert(res.GetValues()[i], BytesEquals, expected.GetValues()[foundIdx])
	}
}

type data struct {
	ops      []kvrpcpb.Op
	keys     [][]byte
	values   [][]byte
	rowValue [][]types.Datum
}

// Generate exist old data and new data in transaction to be amended. Also generate the expected amend mutations
// according to the old and new data and the full generated expected mutations.
func prepareTestData(se *session, mutations *tikv.PlainMutations, oldTblInfo table.Table, newTblInfo table.Table,
	expectedAmendOps []amendOp, c *C) (*data, tikv.PlainMutations) {
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
	KeyOps := []kvrpcpb.Op{kvrpcpb.Op_Put, kvrpcpb.Op_Del, kvrpcpb.Op_Lock, kvrpcpb.Op_Insert, kvrpcpb.Op_Put,
		kvrpcpb.Op_Del, kvrpcpb.Op_Insert, kvrpcpb.Op_Lock}
	numberOfRows := len(KeyOps)
	oldRowValues := make([][]types.Datum, numberOfRows)
	newRowValues := make([][]types.Datum, numberOfRows)
	rd := rowcodec.Encoder{Enable: true}
	oldData := &data{}
	expecteMutations := tikv.NewPlainMutations(8)
	oldRowKvMap := make(map[string][]types.Datum)
	newRowKvMap := make(map[string][]types.Datum)

	// colIdx: 0, 1, 2, 3, 4, 5,     6,     7,     8, 9.
	// column: a, b, c, d, e, c_str, d_str, e_str, f, g.
	// Generate old data.
	for i := 0; i < numberOfRows; i++ {
		keyOp := KeyOps[i]
		thisRowValue := make([]types.Datum, len(basicRowValue))
		copy(thisRowValue, basicRowValue)
		thisRowValue[0] = types.NewIntDatum(int64(i + 1))
		thisRowValue[4] = types.NewIntDatum(int64(i + 1 + 4))
		// f_g has a unique index.
		thisRowValue[8] = types.NewIntDatum(int64(i + 1 + 8))

		// Save old data, they will be put into db first.
		rowKey := tablecodec.EncodeRowKeyWithHandle(oldTblInfo.Meta().ID, kv.IntHandle(int64(i+1)))
		var rowValue []byte
		rowValue, err = rd.Encode(se.sessionVars.StmtCtx, colIds, thisRowValue, nil)
		c.Assert(err, IsNil)
		if keyOp == kvrpcpb.Op_Del || keyOp == kvrpcpb.Op_Put || keyOp == kvrpcpb.Op_Lock {
			// Skip the last Op_put, it has no old row value.
			if i == 4 {
				oldRowValues[i] = nil
				continue
			}
			oldData.keys = append(oldData.keys, rowKey)
			oldData.values = append(oldData.values, rowValue)
			oldData.ops = append(oldData.ops, keyOp)
			oldData.rowValue = append(oldData.rowValue, thisRowValue)
			if keyOp == kvrpcpb.Op_Del {
				mutations.Push(keyOp, rowKey, []byte{}, true)
			}
		}
		oldRowValues[i] = thisRowValue
		oldRowKvMap[string(rowKey)] = thisRowValue
	}

	// Generate new data.
	for i := 0; i < numberOfRows; i++ {
		keyOp := KeyOps[i]
		thisRowValue := make([]types.Datum, len(basicRowValue))
		copy(thisRowValue, basicRowValue)
		thisRowValue[0] = types.NewIntDatum(int64(i + 1))
		// New column e value should be different from old row values.
		thisRowValue[4] = types.NewIntDatum(int64(i+1+4) * 20)
		// New column f value should be different since it has a related unique index.
		thisRowValue[8] = types.NewIntDatum(int64(i+1+4) * 20)

		var rowValue []byte
		// Save new data.
		rowKey := tablecodec.EncodeRowKeyWithHandle(oldTblInfo.Meta().ID, kv.IntHandle(int64(i+1)))
		if keyOp == kvrpcpb.Op_Insert {
			rowValue, err = tablecodec.EncodeOldRow(se.sessionVars.StmtCtx, thisRowValue, colIds, nil, nil)
		} else {
			rowValue, err = rd.Encode(se.sessionVars.StmtCtx, colIds, thisRowValue, nil)
		}
		c.Assert(err, IsNil)
		if keyOp == kvrpcpb.Op_Put || keyOp == kvrpcpb.Op_Insert {
			mutations.Push(keyOp, rowKey, rowValue, true)
		} else if keyOp == kvrpcpb.Op_Lock {
			mutations.Push(keyOp, rowKey, []byte{}, true)
		}
		newRowValues[i] = thisRowValue
		newRowKvMap[string(rowKey)] = thisRowValue
	}

	// Prepare expected result mutations.
	for _, op := range expectedAmendOps {
		var info *amendOperationAddIndexInfo
		expectedOp, ok := op.(*amendOperationAddIndex)
		c.Assert(ok, IsTrue)
		info = expectedOp.info
		var idxVal []byte
		genIndexKV := func(inputRow []types.Datum) ([]byte, []byte) {
			indexDatums := make([]types.Datum, len(info.relatedOldIdxCols))
			for colIdx, col := range info.relatedOldIdxCols {
				indexDatums[colIdx] = inputRow[col.Offset]
			}
			kvHandle := kv.IntHandle(inputRow[0].GetInt64())
			idxKey, _, err := tablecodec.GenIndexKey(se.sessionVars.StmtCtx, newTblInfo.Meta(),
				info.indexInfoAtCommit.Meta(), newTblInfo.Meta().ID, indexDatums, kvHandle, nil)
			c.Assert(err, IsNil)
			idxVal, err = tablecodec.GenIndexValuePortal(se.sessionVars.StmtCtx, newTblInfo.Meta(), info.indexInfoAtCommit.Meta(), false, info.indexInfoAtCommit.Meta().Unique, false, indexDatums, kvHandle, 0, nil)
			c.Assert(err, IsNil)
			return idxKey, idxVal
		}
		for i := 0; i < len(mutations.GetKeys()); i++ {
			oldIdxKeyMutation := tikv.PlainMutations{}
			newIdxKeyMutation := tikv.PlainMutations{}
			key := mutations.GetKeys()[i]
			keyOp := mutations.GetOps()[i]
			if addIndexNeedRemoveOp(info.AmendOpType) && mayGenDelIndexRowKeyOp(keyOp) {
				thisRowValue := oldRowKvMap[string(key)]
				if len(thisRowValue) > 0 {
					idxKey, _ := genIndexKV(thisRowValue)
					isPessimisticLock := false
					if info.indexInfoAtCommit.Meta().Unique {
						isPessimisticLock = true
					}
					oldIdxKeyMutation.Push(kvrpcpb.Op_Del, idxKey, []byte{}, isPessimisticLock)
				}
			}
			if addIndexNeedAddOp(info.AmendOpType) && mayGenPutIndexRowKeyOp(keyOp) {
				thisRowValue := newRowKvMap[string(key)]
				idxKey, idxVal := genIndexKV(thisRowValue)
				mutOp := kvrpcpb.Op_Put
				isPessimisticLock := false
				if info.indexInfoAtCommit.Meta().Unique {
					mutOp = kvrpcpb.Op_Insert
					isPessimisticLock = true
				}
				newIdxKeyMutation.Push(mutOp, idxKey, idxVal, isPessimisticLock)
			}
			skipMerge := false
			if info.AmendOpType == AmendNeedAddDeleteAndInsert {
				if len(oldIdxKeyMutation.GetKeys()) > 0 && len(newIdxKeyMutation.GetKeys()) > 0 {
					if bytes.Equal(oldIdxKeyMutation.GetKeys()[0], newIdxKeyMutation.GetKeys()[0]) {
						skipMerge = true
					}
				}
			}
			if !skipMerge {
				if len(oldIdxKeyMutation.GetKeys()) > 0 {
					expecteMutations.MergeMutations(oldIdxKeyMutation)
				}
				if len(newIdxKeyMutation.GetKeys()) > 0 {
					expecteMutations.MergeMutations(newIdxKeyMutation)
				}
			}
		}
	}

	return oldData, expecteMutations
}

func (s *testSchemaAmenderSuite) TestAmendCollectAndGenMutations(c *C) {
	ctx := context.Background()
	store := newStore(c, "test_schema_amender")
	defer store.Close()
	se := &session{
		store:       store,
		sessionVars: variable.NewSessionVars(),
	}
	startStates := []model.SchemaState{model.StateNone, model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization}
	for _, startState := range startStates {
		endStatMap := ConstOpAddIndex[startState]
		var endStates []model.SchemaState
		for st := range endStatMap {
			endStates = append(endStates, st)
		}
		sort.Slice(endStates, func(i, j int) bool { return endStates[i] < endStates[j] })
		for _, endState := range endStates {
			logutil.BgLogger().Info("[TEST]>>>>>>new round test", zap.Stringer("start", startState), zap.Stringer("end", endState))
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
			oldTblMeta.Indices[3].State = startState

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
			// Indices[4] is newly created unique index.
			newTblMeta.Indices[4].State = endState

			// Only the add index amend operations is collected in the results.
			collector := newAmendCollector()
			tblID := int64(1)
			err = collector.collectTblAmendOps(se, tblID, oldTbInfo, newTblInfo, 1<<model.ActionAddIndex)
			c.Assert(err, IsNil)
			logutil.BgLogger().Info("[TEST]amend ops", zap.Int("len", len(collector.tblAmendOpMap[tblID])))
			var expectedAmendOps []amendOp

			// For index 0.
			addIndexOpInfo := &amendOperationAddIndexInfo{
				AmendOpType:       ConstOpAddIndex[model.StateNone][endState],
				tblInfoAtStart:    oldTbInfo,
				tblInfoAtCommit:   newTblInfo,
				indexInfoAtStart:  nil,
				indexInfoAtCommit: newTblInfo.Indices()[0],
				relatedOldIdxCols: []*table.Column{oldTbInfo.Cols()[2], oldTbInfo.Cols()[3], oldTbInfo.Cols()[4]},
			}
			if addIndexNeedRemoveOp(addIndexOpInfo.AmendOpType) || addIndexNeedAddOp(addIndexOpInfo.AmendOpType) {
				expectedAmendOps = append(expectedAmendOps, &amendOperationAddIndex{
					info:                 addIndexOpInfo,
					insertedNewIndexKeys: make(map[string]struct{}),
					deletedOldIndexKeys:  make(map[string]struct{}),
				})
			}

			// For index 1.
			addIndexOpInfo1 := &amendOperationAddIndexInfo{
				AmendOpType:       ConstOpAddIndex[startState][endState],
				tblInfoAtStart:    oldTbInfo,
				tblInfoAtCommit:   newTblInfo,
				indexInfoAtStart:  oldTbInfo.Indices()[0],
				indexInfoAtCommit: newTblInfo.Indices()[1],
				relatedOldIdxCols: []*table.Column{oldTbInfo.Cols()[4]},
			}
			if addIndexNeedRemoveOp(addIndexOpInfo.AmendOpType) || addIndexNeedAddOp(addIndexOpInfo.AmendOpType) {
				expectedAmendOps = append(expectedAmendOps, &amendOperationAddIndex{
					info:                 addIndexOpInfo1,
					insertedNewIndexKeys: make(map[string]struct{}),
					deletedOldIndexKeys:  make(map[string]struct{}),
				})
			}

			// For index 3.
			addIndexOpInfo2 := &amendOperationAddIndexInfo{
				AmendOpType:       ConstOpAddIndex[startState][endState],
				tblInfoAtStart:    oldTbInfo,
				tblInfoAtCommit:   newTblInfo,
				indexInfoAtStart:  oldTbInfo.Indices()[3],
				indexInfoAtCommit: newTblInfo.Indices()[4],
				relatedOldIdxCols: []*table.Column{oldTbInfo.Cols()[8], oldTbInfo.Cols()[9]},
			}
			if addIndexNeedRemoveOp(addIndexOpInfo.AmendOpType) || addIndexNeedAddOp(addIndexOpInfo.AmendOpType) {
				expectedAmendOps = append(expectedAmendOps, &amendOperationAddIndex{
					info:                 addIndexOpInfo2,
					insertedNewIndexKeys: make(map[string]struct{}),
					deletedOldIndexKeys:  make(map[string]struct{}),
				})
			}

			// Check collect results.
			for i, amendOp := range collector.tblAmendOpMap[tblID] {
				op, ok := amendOp.(*amendOperationAddIndex)
				c.Assert(ok, IsTrue)
				expectedOp, ok := expectedAmendOps[i].(*amendOperationAddIndex)
				c.Assert(ok, IsTrue)
				expectedInfo := expectedOp.info
				info := op.info
				c.Assert(info.AmendOpType, Equals, expectedInfo.AmendOpType)
				c.Assert(info.tblInfoAtStart, Equals, expectedInfo.tblInfoAtStart)
				c.Assert(info.tblInfoAtCommit, Equals, expectedInfo.tblInfoAtCommit)
				c.Assert(info.indexInfoAtStart, Equals, expectedInfo.indexInfoAtStart)
				c.Assert(info.indexInfoAtCommit, Equals, expectedInfo.indexInfoAtCommit)
				for j, col := range expectedInfo.relatedOldIdxCols {
					c.Assert(col, Equals, info.relatedOldIdxCols[j])
				}
			}
			// Generated test data.
			mutations := tikv.NewPlainMutations(8)
			oldData, expectedMutations := prepareTestData(se, &mutations, oldTbInfo, newTblInfo, expectedAmendOps, c)
			// Prepare old data in table.
			txnPrepare, err := se.store.Begin()
			c.Assert(err, IsNil)
			var checkOldKeys []kv.Key
			for i, key := range oldData.keys {
				err = txnPrepare.Set(key, oldData.values[i])
				c.Assert(err, IsNil)
				checkOldKeys = append(checkOldKeys, key)
			}
			err = txnPrepare.Commit(ctx)
			c.Assert(err, IsNil)
			txnCheck, err := se.store.Begin()
			c.Assert(err, IsNil)
			snapData, err := txnCheck.GetSnapshot().Get(ctx, oldData.keys[0])
			c.Assert(err, IsNil)
			c.Assert(oldData.values[0], BytesEquals, snapData)
			snapBatchData, err := txnCheck.BatchGet(ctx, checkOldKeys)
			c.Assert(err, IsNil)
			c.Assert(len(snapBatchData), Equals, len(oldData.keys))
			err = txnCheck.Rollback()
			c.Assert(err, IsNil)

			logutil.BgLogger().Info("[TEST]finish to write old txn data")
			// Write data for this new transaction, its memory buffer will be used by schema amender.
			txn, err := se.store.Begin()
			c.Assert(err, IsNil)
			se.txn.changeInvalidToValid(txn)
			txn, err = se.Txn(true)
			c.Assert(err, IsNil)
			var checkKeys []kv.Key
			for i, key := range mutations.GetKeys() {
				val := mutations.GetValues()[i]
				keyOp := mutations.GetOps()[i]
				if mayGenPutIndexRowKeyOp(keyOp) {
					err = txn.Set(key, val)
					checkKeys = append(checkKeys, key)
				} else if keyOp == kvrpcpb.Op_Del {
					err = txn.Delete(key)
				}
				c.Assert(err, IsNil)
			}
			curVer, err := se.store.CurrentVersion(kv.GlobalTxnScope)
			c.Assert(err, IsNil)
			se.sessionVars.TxnCtx.SetForUpdateTS(curVer.Ver + 1)
			mutationVals, err := txn.BatchGet(ctx, checkKeys)
			c.Assert(err, IsNil)
			c.Assert(len(mutationVals), Equals, len(checkKeys))
			logutil.BgLogger().Info("[TEST]finish to write new txn data")

			schemaAmender := NewSchemaAmenderForTikvTxn(se)
			// Some noisy index key values.
			for i := 0; i < 4; i++ {
				idxValue := []byte("idxValue")
				idxKey := tablecodec.EncodeIndexSeekKey(oldTbInfo.Meta().ID, oldTbInfo.Indices()[i].Meta().ID, idxValue)
				err = txn.Set(idxKey, idxValue)
				c.Assert(err, IsNil)
				mutations.Push(kvrpcpb.Op_Put, idxKey, idxValue, false)
			}

			res, err := schemaAmender.genAllAmendMutations(ctx, &mutations, collector)
			c.Assert(err, IsNil)
			logutil.BgLogger().Info("[TEST]finish to amend and generate new mutations")

			// Validate generated results.
			c.Assert(len(res.GetKeys()), Equals, len(res.GetOps()))
			c.Assert(len(res.GetValues()), Equals, len(res.GetOps()))
			c.Assert(len(res.GetPessimisticFlags()), Equals, len(res.GetOps()))
			for i := 0; i < len(expectedMutations.GetKeys()); i++ {
				logutil.BgLogger().Info("[TEST] expected mutations",
					zap.Stringer("key", kv.Key(expectedMutations.GetKeys()[i])),
					zap.Stringer("val", kv.Key(expectedMutations.GetKeys()[i])),
					zap.Stringer("op_type", expectedMutations.GetOps()[i]),
					zap.Bool("is_pessimistic", expectedMutations.GetPessimisticFlags()[i]),
				)
			}
			for i := 0; i < len(res.GetKeys()); i++ {
				logutil.BgLogger().Info("[TEST] result mutations",
					zap.Stringer("key", kv.Key(res.GetKeys()[i])),
					zap.Stringer("val", kv.Key(res.GetKeys()[i])),
					zap.Stringer("op_type", res.GetOps()[i]),
					zap.Bool("is_pessimistic", res.GetPessimisticFlags()[i]),
				)
			}
			mutationsEqual(res, &expectedMutations, c)
			err = txn.Rollback()
			c.Assert(err, IsNil)
			logutil.BgLogger().Info("[TEST]<<<<<<end test round", zap.Stringer("start", startState), zap.Stringer("end", endState))
		}
	}
}
