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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

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

func mutationsEqual(res *transaction.PlainMutations, expected *transaction.PlainMutations, t *testing.T) {
	require.Len(t, res.GetKeys(), len(expected.GetKeys()))
	for i := 0; i < len(res.GetKeys()); i++ {
		foundIdx := -1
		for j := 0; j < len(expected.GetKeys()); j++ {
			if bytes.Equal(res.GetKeys()[i], expected.GetKeys()[j]) {
				foundIdx = j
				break
			}
		}
		require.GreaterOrEqual(t, foundIdx, 0)
		require.Equal(t, expected.GetOps()[foundIdx], res.GetOps()[i])
		require.Equal(t, expected.IsPessimisticLock(foundIdx), res.IsPessimisticLock(i))
		require.Equal(t, expected.GetKeys()[foundIdx], res.GetKeys()[i])
		require.Equal(t, expected.GetValues()[foundIdx], res.GetValues()[i])
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
func prepareTestData(
	se *session,
	mutations *transaction.PlainMutations,
	oldTblInfo table.Table,
	newTblInfo table.Table,
	expectedAmendOps []amendOp,
	t *testing.T,
) (*data, transaction.PlainMutations) {
	var err error
	// Generated test data.
	colIds := make([]int64, len(oldTblInfo.Meta().Columns))
	basicRowValue := make([]types.Datum, len(oldTblInfo.Meta().Columns))
	for i, col := range oldTblInfo.Meta().Columns {
		colIds[i] = oldTblInfo.Meta().Columns[col.Offset].ID
		if col.FieldType.GetType() == mysql.TypeLong {
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
	expectedMutations := transaction.NewPlainMutations(8)
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
		require.NoError(t, err)
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
				mutations.Push(keyOp, rowKey, []byte{}, true, false, false)
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
		require.NoError(t, err)
		if keyOp == kvrpcpb.Op_Put || keyOp == kvrpcpb.Op_Insert {
			mutations.Push(keyOp, rowKey, rowValue, true, false, false)
		} else if keyOp == kvrpcpb.Op_Lock {
			mutations.Push(keyOp, rowKey, []byte{}, true, false, false)
		}
		newRowValues[i] = thisRowValue
		newRowKvMap[string(rowKey)] = thisRowValue
	}

	// Prepare expected result mutations.
	for _, op := range expectedAmendOps {
		var info *amendOperationAddIndexInfo
		expectedOp, ok := op.(*amendOperationAddIndex)
		require.True(t, ok)
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
			require.NoError(t, err)
			idxVal, err = tablecodec.GenIndexValuePortal(se.sessionVars.StmtCtx, newTblInfo.Meta(), info.indexInfoAtCommit.Meta(), false, info.indexInfoAtCommit.Meta().Unique, false, indexDatums, kvHandle, 0, nil)
			require.NoError(t, err)
			return idxKey, idxVal
		}
		for i := 0; i < len(mutations.GetKeys()); i++ {
			oldIdxKeyMutation := transaction.PlainMutations{}
			newIdxKeyMutation := transaction.PlainMutations{}
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
					oldIdxKeyMutation.Push(kvrpcpb.Op_Del, idxKey, []byte{}, isPessimisticLock, false, false)
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
				newIdxKeyMutation.Push(mutOp, idxKey, idxVal, isPessimisticLock, false, false)
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
					expectedMutations.MergeMutations(oldIdxKeyMutation)
				}
				if len(newIdxKeyMutation.GetKeys()) > 0 {
					expectedMutations.MergeMutations(newIdxKeyMutation)
				}
			}
		}
	}

	return oldData, expectedMutations
}

func TestAmendCollectAndGenMutations(t *testing.T) {
	ctx := context.Background()
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
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
			require.NoError(t, err)
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
			require.NoError(t, err)
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
			require.NoError(t, err)
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
				require.True(t, ok)
				expectedOp, ok := expectedAmendOps[i].(*amendOperationAddIndex)
				require.True(t, ok)
				expectedInfo := expectedOp.info
				info := op.info
				require.Equal(t, expectedInfo.AmendOpType, info.AmendOpType)
				require.Equal(t, expectedInfo.tblInfoAtStart, info.tblInfoAtStart)
				require.Equal(t, expectedInfo.tblInfoAtCommit, info.tblInfoAtCommit)
				require.Equal(t, expectedInfo.indexInfoAtStart, info.indexInfoAtStart)
				require.Equal(t, expectedInfo.indexInfoAtCommit, info.indexInfoAtCommit)
				for j, col := range expectedInfo.relatedOldIdxCols {
					require.Equal(t, info.relatedOldIdxCols[j], col)
				}
			}
			// Generated test data.
			mutations := transaction.NewPlainMutations(8)
			oldData, expectedMutations := prepareTestData(se, &mutations, oldTbInfo, newTblInfo, expectedAmendOps, t)
			// Prepare old data in table.
			txnPrepare, err := se.store.Begin()
			require.NoError(t, err)
			var checkOldKeys []kv.Key
			for i, key := range oldData.keys {
				err = txnPrepare.Set(key, oldData.values[i])
				require.NoError(t, err)
				checkOldKeys = append(checkOldKeys, key)
			}
			err = txnPrepare.Commit(ctx)
			require.NoError(t, err)
			txnCheck, err := se.store.Begin()
			require.NoError(t, err)
			snapData, err := txnCheck.GetSnapshot().Get(ctx, oldData.keys[0])
			require.NoError(t, err)
			require.Equal(t, snapData, oldData.values[0])
			snapBatchData, err := txnCheck.BatchGet(ctx, checkOldKeys)
			require.NoError(t, err)
			require.Equal(t, len(oldData.keys), len(snapBatchData))
			err = txnCheck.Rollback()
			require.NoError(t, err)

			logutil.BgLogger().Info("[TEST]finish to write old txn data")
			// Write data for this new transaction, its memory buffer will be used by schema amender.
			txn, err := se.store.Begin()
			require.NoError(t, err)
			se.txn.changeInvalidToValid(txn)
			txn, err = se.Txn(true)
			require.NoError(t, err)
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
				require.NoError(t, err)
			}
			curVer, err := se.store.CurrentVersion(kv.GlobalTxnScope)
			require.NoError(t, err)
			se.sessionVars.TxnCtx.SetForUpdateTS(curVer.Ver + 1)
			mutationVals, err := txn.BatchGet(ctx, checkKeys)
			require.NoError(t, err)
			require.Equal(t, len(checkKeys), len(mutationVals))
			logutil.BgLogger().Info("[TEST]finish to write new txn data")

			schemaAmender := NewSchemaAmenderForTikvTxn(se)
			// Some noisy index key values.
			for i := 0; i < 4; i++ {
				idxValue := []byte("idxValue")
				idxKey := tablecodec.EncodeIndexSeekKey(oldTbInfo.Meta().ID, oldTbInfo.Indices()[i].Meta().ID, idxValue)
				err = txn.Set(idxKey, idxValue)
				require.NoError(t, err)
				mutations.Push(kvrpcpb.Op_Put, idxKey, idxValue, false, false, false)
			}

			res, err := schemaAmender.genAllAmendMutations(ctx, &mutations, collector)
			require.NoError(t, err)
			logutil.BgLogger().Info("[TEST]finish to amend and generate new mutations")

			// Validate generated results.
			require.Equal(t, len(res.GetOps()), len(res.GetKeys()))
			require.Equal(t, len(res.GetOps()), len(res.GetValues()))
			require.Equal(t, len(res.GetOps()), len(res.GetFlags()))
			for i := 0; i < len(expectedMutations.GetKeys()); i++ {
				logutil.BgLogger().Info("[TEST] expected mutations",
					zap.Stringer("key", kv.Key(expectedMutations.GetKeys()[i])),
					zap.Stringer("val", kv.Key(expectedMutations.GetKeys()[i])),
					zap.Stringer("op_type", expectedMutations.GetOps()[i]),
					zap.Bool("is_pessimistic", expectedMutations.IsPessimisticLock(i)),
				)
			}
			for i := 0; i < len(res.GetKeys()); i++ {
				logutil.BgLogger().Info("[TEST] result mutations",
					zap.Stringer("key", kv.Key(res.GetKeys()[i])),
					zap.Stringer("val", kv.Key(res.GetKeys()[i])),
					zap.Stringer("op_type", res.GetOps()[i]),
					zap.Bool("is_pessimistic", res.IsPessimisticLock(i)),
				)
			}
			mutationsEqual(res, &expectedMutations, t)
			err = txn.Rollback()
			require.NoError(t, err)
			logutil.BgLogger().Info("[TEST]<<<<<<end test round", zap.Stringer("start", startState), zap.Stringer("end", endState))
		}
	}
}
