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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
	"go.uber.org/zap"
)

// tikv error instance
var (
	// ErrTokenLimit is the error that token is up to the limit.
	ErrTokenLimit = dbterror.ClassTiKV.NewStd(errno.ErrTiKVStoreLimit)
	// ErrTiKVServerTimeout is the error when tikv server is timeout.
	ErrTiKVServerTimeout    = dbterror.ClassTiKV.NewStd(errno.ErrTiKVServerTimeout)
	ErrTiFlashServerTimeout = dbterror.ClassTiKV.NewStd(errno.ErrTiFlashServerTimeout)
	// ErrGCTooEarly is the error that GC life time is shorter than transaction duration
	ErrGCTooEarly = dbterror.ClassTiKV.NewStd(errno.ErrGCTooEarly)
	// ErrTiKVStaleCommand is the error that the command is stale in tikv.
	ErrTiKVStaleCommand = dbterror.ClassTiKV.NewStd(errno.ErrTiKVStaleCommand)
	// ErrTiKVMaxTimestampNotSynced is the error that tikv's max timestamp is not synced.
	ErrTiKVMaxTimestampNotSynced = dbterror.ClassTiKV.NewStd(errno.ErrTiKVMaxTimestampNotSynced)
	ErrResolveLockTimeout        = dbterror.ClassTiKV.NewStd(errno.ErrResolveLockTimeout)
	// ErrLockWaitTimeout is the error that wait for the lock is timeout.
	ErrLockWaitTimeout = dbterror.ClassTiKV.NewStd(errno.ErrLockWaitTimeout)
	// ErrTiKVServerBusy is the error when tikv server is busy.
	ErrTiKVServerBusy = dbterror.ClassTiKV.NewStd(errno.ErrTiKVServerBusy)
	// ErrTiFlashServerBusy is the error that tiflash server is busy.
	ErrTiFlashServerBusy = dbterror.ClassTiKV.NewStd(errno.ErrTiFlashServerBusy)
	// ErrPDServerTimeout is the error when pd server is timeout.
	ErrPDServerTimeout = dbterror.ClassTiKV.NewStd(errno.ErrPDServerTimeout)
	// ErrRegionUnavailable is the error when region is not available.
	ErrRegionUnavailable = dbterror.ClassTiKV.NewStd(errno.ErrRegionUnavailable)
)

// Registers error returned from TiKV.
var (
	_ = dbterror.ClassTiKV.NewStd(errno.ErrDataOutOfRange)
	_ = dbterror.ClassTiKV.NewStd(errno.ErrTruncatedWrongValue)
	_ = dbterror.ClassTiKV.NewStd(errno.ErrDivisionByZero)
)

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
		return genKeyExistsError(name, handle.String(), nil)
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

func extractKeyErr(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := errors.Cause(err).(*tikverr.ErrWriteConflict); ok {
		return newWriteConflictError(e.WriteConflict)
	}
	if e, ok := errors.Cause(err).(*tikverr.ErrRetryable); ok {
		notFoundDetail := prettyLockNotFoundKey(e.Retryable)
		return kv.ErrTxnRetryable.GenWithStackByArgs(e.Retryable + " " + notFoundDetail)
	}
	return ToTiDBErr(err)
}

// ToTiDBErr checks and converts a tikv error to a tidb error.
func ToTiDBErr(err error) error {
	originErr := err
	if err == nil {
		return nil
	}
	err = errors.Cause(err)
	if tikverr.IsErrNotFound(err) {
		return kv.ErrNotExist
	}

	if e, ok := err.(*tikverr.ErrWriteConflictInLatch); ok {
		return kv.ErrWriteConflictInTiDB.FastGenByArgs(e.StartTS)
	}

	if e, ok := err.(*tikverr.ErrTxnTooLarge); ok {
		return kv.ErrTxnTooLarge.GenWithStackByArgs(e.Size)
	}

	if errors.ErrorEqual(err, tikverr.ErrCannotSetNilValue) {
		return kv.ErrCannotSetNilValue
	}

	if e, ok := err.(*tikverr.ErrEntryTooLarge); ok {
		return kv.ErrEntryTooLarge.GenWithStackByArgs(e.Limit, e.Size)
	}

	if errors.ErrorEqual(err, tikverr.ErrInvalidTxn) {
		return kv.ErrInvalidTxn
	}

	if errors.ErrorEqual(err, tikverr.ErrTiKVServerTimeout) {
		return ErrTiKVServerTimeout
	}

	if e, ok := err.(*tikverr.ErrPDServerTimeout); ok {
		if len(e.Error()) == 0 {
			return ErrPDServerTimeout
		}
		return ErrPDServerTimeout.GenWithStackByArgs(e.Error())
	}

	if errors.ErrorEqual(err, tikverr.ErrTiFlashServerTimeout) {
		return ErrTiFlashServerTimeout
	}

	if errors.ErrorEqual(err, tikverr.ErrTiKVServerBusy) {
		return ErrTiKVServerBusy
	}

	if errors.ErrorEqual(err, tikverr.ErrTiFlashServerBusy) {
		return ErrTiFlashServerBusy
	}

	if e, ok := err.(*tikverr.ErrGCTooEarly); ok {
		return ErrGCTooEarly.GenWithStackByArgs(e.TxnStartTS, e.GCSafePoint)
	}

	if errors.ErrorEqual(err, tikverr.ErrTiKVStaleCommand) {
		return ErrTiKVStaleCommand
	}

	if errors.ErrorEqual(err, tikverr.ErrTiKVMaxTimestampNotSynced) {
		return ErrTiKVMaxTimestampNotSynced
	}

	if errors.ErrorEqual(err, tikverr.ErrResolveLockTimeout) {
		return ErrResolveLockTimeout
	}

	if errors.ErrorEqual(err, tikverr.ErrLockWaitTimeout) {
		return ErrLockWaitTimeout
	}

	if errors.ErrorEqual(err, tikverr.ErrRegionUnavailable) {
		return ErrRegionUnavailable
	}

	if e, ok := err.(*tikverr.ErrTokenLimit); ok {
		return ErrTokenLimit.GenWithStackByArgs(e.StoreID)
	}

	return errors.Trace(originErr)
}

func newWriteConflictError(conflict *kvrpcpb.WriteConflict) error {
	if conflict == nil {
		return kv.ErrWriteConflict
	}
	var buf bytes.Buffer
	prettyWriteKey(&buf, conflict.Key)
	buf.WriteString(" primary=")
	prettyWriteKey(&buf, conflict.Primary)
	return kv.ErrWriteConflict.FastGenByArgs(conflict.StartTs, conflict.ConflictTs, conflict.ConflictCommitTs, buf.String())
}

func prettyWriteKey(buf *bytes.Buffer, key []byte) {
	tableID, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
	if err == nil {
		_, err1 := fmt.Fprintf(buf, "{tableID=%d, indexID=%d, indexValues={", tableID, indexID)
		if err1 != nil {
			logutil.BgLogger().Error("error", zap.Error(err1))
		}
		for _, v := range indexValues {
			_, err2 := fmt.Fprintf(buf, "%s, ", v)
			if err2 != nil {
				logutil.BgLogger().Error("error", zap.Error(err2))
			}
		}
		buf.WriteString("}}")
		return
	}

	tableID, handle, err := tablecodec.DecodeRecordKey(key)
	if err == nil {
		_, err3 := fmt.Fprintf(buf, "{tableID=%d, handle=%d}", tableID, handle)
		if err3 != nil {
			logutil.BgLogger().Error("error", zap.Error(err3))
		}
		return
	}

	mKey, mField, err := tablecodec.DecodeMetaKey(key)
	if err == nil {
		_, err3 := fmt.Fprintf(buf, "{metaKey=true, key=%s, field=%s}", string(mKey), string(mField))
		if err3 != nil {
			logutil.Logger(context.Background()).Error("error", zap.Error(err3))
		}
		return
	}

	_, err4 := fmt.Fprintf(buf, "%#v", key)
	if err4 != nil {
		logutil.BgLogger().Error("error", zap.Error(err4))
	}
}

func prettyLockNotFoundKey(rawRetry string) string {
	if !strings.Contains(rawRetry, "TxnLockNotFound") {
		return ""
	}
	start := strings.Index(rawRetry, "[")
	if start == -1 {
		return ""
	}
	rawRetry = rawRetry[start:]
	end := strings.Index(rawRetry, "]")
	if end == -1 {
		return ""
	}
	rawRetry = rawRetry[:end+1]
	var key []byte
	err := json.Unmarshal([]byte(rawRetry), &key)
	if err != nil {
		return ""
	}
	var buf bytes.Buffer
	prettyWriteKey(&buf, key)
	return buf.String()
}
