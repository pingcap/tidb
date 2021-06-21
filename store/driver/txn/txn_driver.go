// Copyright 2021 PingCAP, Inc.
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

package txn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

type tikvTxn struct {
	*tikv.KVTxn
	idxNameCache map[int64]*model.TableInfo
}

// NewTiKVTxn returns a new Transaction.
func NewTiKVTxn(txn *tikv.KVTxn) kv.Transaction {
	return &tikvTxn{txn, make(map[int64]*model.TableInfo)}
}

func (txn *tikvTxn) GetTableInfo(id int64) *model.TableInfo {
	return txn.idxNameCache[id]
}

func (txn *tikvTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	txn.idxNameCache[id] = info
}

// lockWaitTime in ms, except that kv.LockAlwaysWait(0) means always wait lock, kv.LockNowait(-1) means nowait lock
func (txn *tikvTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keysInput ...kv.Key) error {
	err := txn.KVTxn.LockKeys(ctx, lockCtx, keysInput...)
	return txn.extractKeyErr(err)
}

func (txn *tikvTxn) Commit(ctx context.Context) error {
	err := txn.KVTxn.Commit(ctx)
	return txn.extractKeyErr(err)
}

func (txn *tikvTxn) extractKeyErr(err error) error {
	if e, ok := errors.Cause(err).(*tikv.ErrKeyExist); ok {
		return txn.extractKeyExistsErr(e.GetKey())
	}
	return errors.Trace(err)
}

func (txn *tikvTxn) extractKeyExistsErr(key kv.Key) error {
	tableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}

	tblInfo := txn.GetTableInfo(tableID)
	if tblInfo == nil {
		return genKeyExistsError("UNKNOWN", key.String(), errors.New("cannot find table info"))
	}

	value, err := txn.GetUnionStore().GetMemBuffer().SelectValueHistory(key, func(value []byte) bool { return len(value) != 0 })
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}

	if isRecord {
		return extractKeyExistsErrFromHandle(key, value, tblInfo)
	}
	return extractKeyExistsErrFromIndex(key, value, tblInfo, indexID)
}

func genKeyExistsError(name string, value string, err error) error {
	if err != nil {
		logutil.BgLogger().Info("extractKeyExistsErr meets error", zap.Error(err))
	}
	return kv.ErrKeyExists.FastGenByArgs(value, name)
}

func extractKeyExistsErrFromHandle(key kv.Key, value []byte, tblInfo *model.TableInfo) error {
	const name = "PRIMARY"
	_, handle, err := tablecodec.DecodeRecordKey(key)
	if err != nil {
		return genKeyExistsError(name, key.String(), err)
	}

	if handle.IsInt() {
		if pkInfo := tblInfo.GetPkColInfo(); pkInfo != nil {
			if mysql.HasUnsignedFlag(pkInfo.Flag) {
				handleStr := fmt.Sprintf("%d", uint64(handle.IntValue()))
				return genKeyExistsError(name, handleStr, nil)
			}
		}
<<<<<<< HEAD
		return genKeyExistsError(name, handle.String(), nil)
=======
	case kv.SchemaAmender:
		txn.SetSchemaAmender(val.(tikv.SchemaAmender))
	case kv.SampleStep:
		txn.KVTxn.GetSnapshot().SetSampleStep(val.(uint32))
	case kv.CommitHook:
		txn.SetCommitCallback(val.(func(string, error)))
	case kv.EnableAsyncCommit:
		txn.SetEnableAsyncCommit(val.(bool))
	case kv.Enable1PC:
		txn.SetEnable1PC(val.(bool))
	case kv.GuaranteeLinearizability:
		txn.SetCausalConsistency(!val.(bool))
	case kv.TxnScope:
		txn.SetScope(val.(string))
	case kv.IsStalenessReadOnly:
		txn.KVTxn.GetSnapshot().SetIsStatenessReadOnly(val.(bool))
	case kv.MatchStoreLabels:
		txn.KVTxn.GetSnapshot().SetMatchStoreLabels(val.([]*metapb.StoreLabel))
	case kv.ResourceGroupTag:
		txn.KVTxn.SetResourceGroupTag(val.([]byte))
	case kv.KVFilter:
		txn.KVTxn.SetKVFilter(val.(tikv.KVFilter))
>>>>>>> 9f18723e6... *: fix bug that write on temporary table send request to TiKV (#25535)
	}

	if len(value) == 0 {
		return genKeyExistsError(name, handle.String(), errors.New("missing value"))
	}

	idxInfo := tables.FindPrimaryIndex(tblInfo)
	if idxInfo == nil {
		return genKeyExistsError(name, handle.String(), errors.New("cannot find index info"))
	}

	cols := make(map[int64]*types.FieldType, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		cols[col.ID] = &col.FieldType
	}
	handleColIDs := make([]int64, 0, len(idxInfo.Columns))
	for _, col := range idxInfo.Columns {
		handleColIDs = append(handleColIDs, tblInfo.Columns[col.Offset].ID)
	}

	row, err := tablecodec.DecodeRowToDatumMap(value, cols, time.Local)
	if err != nil {
		return genKeyExistsError(name, handle.String(), err)
	}

	data, err := tablecodec.DecodeHandleToDatumMap(handle, handleColIDs, cols, time.Local, row)
	if err != nil {
		return genKeyExistsError(name, handle.String(), err)
	}

	valueStr := make([]string, 0, len(data))
	for _, col := range idxInfo.Columns {
		d := data[tblInfo.Columns[col.Offset].ID]
		str, err := d.ToString()
		if err != nil {
			return genKeyExistsError(name, key.String(), err)
		}
		valueStr = append(valueStr, str)
	}
	return genKeyExistsError(name, strings.Join(valueStr, "-"), nil)
}

func extractKeyExistsErrFromIndex(key kv.Key, value []byte, tblInfo *model.TableInfo, indexID int64) error {
	var idxInfo *model.IndexInfo
	for _, index := range tblInfo.Indices {
		if index.ID == indexID {
			idxInfo = index
		}
	}
	if idxInfo == nil {
		return genKeyExistsError("UNKNOWN", key.String(), errors.New("cannot find index info"))
	}
	name := idxInfo.Name.String()

	if len(value) == 0 {
		return genKeyExistsError(name, key.String(), errors.New("missing value"))
	}

	colInfo := tables.BuildRowcodecColInfoForIndexColumns(idxInfo, tblInfo)
	values, err := tablecodec.DecodeIndexKV(key, value, len(idxInfo.Columns), tablecodec.HandleNotNeeded, colInfo)
	if err != nil {
		return genKeyExistsError(name, key.String(), err)
	}
	valueStr := make([]string, 0, len(values))
	for i, val := range values {
		d, err := tablecodec.DecodeColumnValue(val, colInfo[i].Ft, time.Local)
		if err != nil {
			return genKeyExistsError(name, key.String(), err)
		}
		str, err := d.ToString()
		if err != nil {
			return genKeyExistsError(name, key.String(), err)
		}
		valueStr = append(valueStr, str)
	}
	return genKeyExistsError(name, strings.Join(valueStr, "-"), nil)
}
