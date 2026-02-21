// Copyright 2016 PingCAP, Inc.
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

package tables

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Delete removes the entry for handle h and indexedValues from KV index.
func (c *index) Delete(ctx table.MutateContext, txn kv.Transaction, indexedValue []types.Datum, h kv.Handle) error {
	indexedValues := c.getIndexedValue(indexedValue)
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	loc, ec := evalCtx.Location(), evalCtx.ErrCtx()
	for _, value := range indexedValues {
		key, distinct, err := c.GenIndexKey(ec, loc, value, h, nil)
		if err != nil {
			return err
		}

		key, tempKey, tempKeyVer := GenTempIdxKeyByState(c.idxInfo, key)
		doubleWrite := tempKeyVer == tablecodec.TempIndexKeyTypeMerge
		var originTempVal []byte
		if len(tempKey) > 0 && c.idxInfo.Unique {
			// Get the origin value of the unique temporary index key.
			// Append the new delete operations to the end of the origin value.
			originTempVal, err = getKeyInTxn(context.TODO(), txn, tempKey)
			if err != nil {
				return err
			}
		}

		tempValElem := tablecodec.TempIndexValueElem{Handle: h, KeyVer: tempKeyVer, Delete: true, Distinct: distinct}
		if c.idxInfo.Global {
			tempValElem.Global = true
			tempValElem.Handle = kv.NewPartitionHandle(c.phyTblID, h)
		}
		if distinct {
			if len(key) > 0 {
				okToDelete := true
				if c.idxInfo.BackfillState != model.BackfillStateInapplicable {
					// #52914: the delete key is covered by the new ingested key, which shouldn't be deleted.
					originVal, err := getKeyInTxn(context.TODO(), txn, key)
					if err != nil {
						return err
					}
					if len(originVal) > 0 {
						oh, err := tablecodec.DecodeHandleInIndexValue(originVal)
						if err != nil {
							return err
						}
						// The handle passed in may be a `PartitionHandle`,
						// so we can't directly do comparation with them.
						if !h.Equal(oh) {
							okToDelete = false
						}
					}
				}
				if okToDelete {
					err = txn.GetMemBuffer().DeleteWithFlags(key, kv.SetNeedLocked)
					if err != nil {
						return err
					}
				}
			}
			if len(tempKey) > 0 {
				// Append to the end of the origin value for distinct value.
				tempVal := tempValElem.Encode(originTempVal)
				err = txn.GetMemBuffer().Set(tempKey, tempVal)
				if err != nil {
					return err
				}
				metrics.DDLAddOneTempIndexWrite(ctx.ConnectionID(), c.tblInfo.ID, doubleWrite)
			}
		} else {
			if len(key) > 0 {
				if c.mayDDLMergingTempIndex() {
					// Here may have the situation:
					// DML: Deleting the normal index key.
					// DDL: Writing the same normal index key, but it does not lock primary record.
					// In this case, we should lock the index key in DML to grantee the serialization.
					err = txn.GetMemBuffer().DeleteWithFlags(key, kv.SetNeedLocked)
				} else {
					err = txn.GetMemBuffer().Delete(key)
				}
				if err != nil {
					return err
				}
			}
			if len(tempKey) > 0 {
				tempVal := tempValElem.Encode(nil)
				err = txn.GetMemBuffer().Set(tempKey, tempVal)
				if err != nil {
					return err
				}
				metrics.DDLAddOneTempIndexWrite(ctx.ConnectionID(), c.tblInfo.ID, doubleWrite)
			}
		}
		if c.idxInfo.State == model.StatePublic {
			// If the index is in public state, delete this index means it must exists.
			err = setAssertion(txn, key, kv.AssertExist)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// mayDDLMergingTempIndex checks whether the DDL worker may be merging the temporary index to the normal index.
// In most times, if an index is not unique, its primary record is assumed to be mutated and locked.
// The only exception is when the DDL worker is merging the temporary index in fast reorging,
// the DDL txn will not lock the primary record to reduce unnecessary conflicts.
// At this time, the index record should be locked in force
// to make sure the serialization between the DDL and DML transactions.
func (c *index) mayDDLMergingTempIndex() bool {
	return c.idxInfo.BackfillState == model.BackfillStateReadyToMerge ||
		c.idxInfo.BackfillState == model.BackfillStateMerging
}

func (c *index) GenIndexKVIter(ec errctx.Context, loc *time.Location, indexedValue []types.Datum,
	h kv.Handle, handleRestoreData []types.Datum) table.IndexKVGenerator {
	var mvIndexValues [][]types.Datum
	if c.Meta().MVIndex {
		mvIndexValues = c.getIndexedValue(indexedValue)
		return table.NewMultiValueIndexKVGenerator(c, ec, loc, h, handleRestoreData, mvIndexValues)
	}
	return table.NewPlainIndexKVGenerator(c, ec, loc, h, handleRestoreData, indexedValue)
}

// GenTempIdxKeyByState is used to get the key version and the temporary key.
// The tempKeyVer means the temp index key/value version.
func GenTempIdxKeyByState(indexInfo *model.IndexInfo, indexKey kv.Key) (key, tempKey kv.Key, tempKeyVer byte) {
	if indexInfo.State != model.StatePublic {
		switch indexInfo.BackfillState {
		case model.BackfillStateInapplicable:
			return indexKey, nil, tablecodec.TempIndexKeyTypeNone
		case model.BackfillStateRunning:
			// Write to the temporary index.
			tablecodec.IndexKey2TempIndexKey(indexKey)
			if indexInfo.State == model.StateDeleteOnly {
				return nil, indexKey, tablecodec.TempIndexKeyTypeDelete
			}
			return nil, indexKey, tablecodec.TempIndexKeyTypeBackfill
		case model.BackfillStateReadyToMerge, model.BackfillStateMerging:
			// Double write
			tmp := make([]byte, len(indexKey))
			copy(tmp, indexKey)
			tablecodec.IndexKey2TempIndexKey(tmp)
			return indexKey, tmp, tablecodec.TempIndexKeyTypeMerge
		}
	}
	return indexKey, nil, tablecodec.TempIndexKeyTypeNone
}

func (c *index) Exist(ec errctx.Context, loc *time.Location, txn kv.Transaction, indexedValue []types.Datum, h kv.Handle) (bool, kv.Handle, error) {
	indexedValues := c.getIndexedValue(indexedValue)
	for _, val := range indexedValues {
		key, distinct, err := c.GenIndexKey(ec, loc, val, h, nil)
		if err != nil {
			return false, nil, err
		}

		// If index current is in creating status and using ingest mode, we need first
		// check key exist status in temp index.
		key, tempKey, _ := GenTempIdxKeyByState(c.idxInfo, key)
		if len(tempKey) > 0 {
			key = tempKey
		}

		if distinct {
			dupHandle, err := FetchDuplicatedHandle(context.Background(), key, txn)
			if err != nil {
				return false, nil, err
			}
			if dupHandle == nil || !dupHandle.Equal(h) {
				return false, nil, nil
			}
			continue
		}

		val, err := getKeyInTxn(context.Background(), txn, key)
		if err != nil {
			return false, nil, err
		}
		if len(tempKey) > 0 {
			tempVal, err := tablecodec.DecodeTempIndexValue(val)
			if err != nil {
				return false, nil, err
			}
			if tempVal.IsEmpty() || tempVal.Current().Delete {
				return false, nil, nil
			}
			continue
		}
		if len(val) == 0 {
			return false, nil, nil
		}
		continue
	}
	return true, h, nil
}

// FetchDuplicatedHandle is used to find the duplicated row's handle for a given unique index key.
func FetchDuplicatedHandle(ctx context.Context, key kv.Key,
	txn kv.Transaction) (dupHandle kv.Handle, err error) {
	if tablecodec.IsTempIndexKey(key) {
		dupHandle, _, err := FetchDuplicatedHandleForTempIndexKey(ctx, key, txn)
		return dupHandle, err
	}
	// The index key is not from temp index.
	val, err := getKeyInTxn(ctx, txn, key)
	if err != nil || len(val) == 0 {
		return nil, err
	}
	return tablecodec.DecodeHandleInIndexValue(val)
}

// FetchDuplicatedHandleForTempIndexKey is used to find the duplicated row's handle for a given unique index key.
// It is used when the index key is from temp index.
// The function will check the temp index key first, if not found, it will check the origin index key.
func FetchDuplicatedHandleForTempIndexKey(ctx context.Context, tempKey kv.Key,
	txn kv.Transaction) (dupHandle kv.Handle, oldVal []byte, err error) {
	tempRawVal, err := getKeyInTxn(ctx, txn, tempKey)
	if err != nil {
		return nil, tempRawVal, err
	}
	if tempRawVal == nil {
		originKey := tempKey.Clone()
		tablecodec.TempIndexKey2IndexKey(originKey)
		originVal, err := getKeyInTxn(ctx, txn, originKey)
		if err != nil || originVal == nil {
			return nil, nil, err
		}
		originHandle, err := tablecodec.DecodeHandleInIndexValue(originVal)
		if err != nil {
			return nil, nil, err
		}
		return originHandle, nil, err
	}
	tempVal, err := tablecodec.DecodeTempIndexValue(tempRawVal)
	if err != nil {
		return nil, tempRawVal, err
	}
	curElem := tempVal.Current()
	if curElem.Delete {
		originKey := tempKey.Clone()
		tablecodec.TempIndexKey2IndexKey(originKey)
		originVal, err := getKeyInTxn(ctx, txn, originKey)
		if err != nil || originVal == nil {
			return nil, tempRawVal, err
		}
		originHandle, err := tablecodec.DecodeHandleInIndexValue(originVal)
		if err != nil {
			return nil, tempRawVal, err
		}
		if originHandle.Equal(curElem.Handle) {
			// The key has been deleted. This is not a duplicated key.
			return nil, tempRawVal, nil
		}
		// The inequality means multiple modifications happened in the same key.
		// We use the handle in origin index value to check if the row exists.
		tableID := tablecodec.DecodeTableID(tempKey)
		recPrefix := tablecodec.GenTableRecordPrefix(tableID)
		rowKey := tablecodec.EncodeRecordKey(recPrefix, originHandle)
		rowVal, err := getKeyInTxn(ctx, txn, rowKey)
		if err != nil || rowVal == nil {
			return nil, tempRawVal, err
		}
		// The row exists. This is the duplicated key.
		return originHandle, tempRawVal, nil
	}
	// The value in temp index is not the delete marker.
	h, err := tablecodec.DecodeHandleInIndexValue(curElem.Value)
	return h, tempRawVal, err
}

// getKeyInTxn gets the value of the key in the transaction, and ignore the ErrNotExist error.
func getKeyInTxn(ctx context.Context, txn kv.Transaction, key kv.Key) ([]byte, error) {
	val, err := kv.GetValue(ctx, txn, key)
	if err != nil {
		if kv.IsErrNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

// FetchValues implements table.Index interface.
func (c *index) FetchValues(r []types.Datum, vals []types.Datum) ([]types.Datum, error) {
	return fetchIndexRow(c.idxInfo, r, vals, nil)
}

func fetchIndexRow(idxInfo *model.IndexInfo, r, vals []types.Datum, opt table.IndexRowLayoutOption) ([]types.Datum, error) {
	needLength := len(idxInfo.Columns)
	if vals == nil || cap(vals) < needLength {
		vals = make([]types.Datum, needLength)
	}
	vals = vals[:needLength]
	// If the context has extra info, use the extra layout info to get index columns.
	if len(opt) != 0 {
		intest.Assert(len(opt) == len(idxInfo.Columns), "offsets length is not equal to index columns length, offset len: %d, index len: %d", len(opt), len(idxInfo.Columns))
		for i, offset := range opt {
			if offset < 0 || offset > len(r) {
				return nil, table.ErrIndexOutBound.GenWithStackByArgs(idxInfo.Name, offset, r)
			}
			vals[i] = r[offset]
		}
		return vals, nil
	}
	// Otherwise use the full column layout.
	for i, ic := range idxInfo.Columns {
		if ic.Offset < 0 || ic.Offset >= len(r) {
			return nil, table.ErrIndexOutBound.GenWithStackByArgs(ic.Name, ic.Offset, r)
		}
		vals[i] = r[ic.Offset]
	}
	return vals, nil
}

