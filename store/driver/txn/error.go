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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
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
			if mysql.HasUnsignedFlag(pkInfo.GetFlag()) {
				handleStr := strconv.FormatUint(uint64(handle.IntValue()), 10)
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
		if col.Length > 0 && len(str) > col.Length {
			str = str[:col.Length]
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
	return derr.ToTiDBErr(err)
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
