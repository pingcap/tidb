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

package consistency

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	// ErrAdminCheckInconsistent returns for data inconsistency for admin check.
	ErrAdminCheckInconsistent = dbterror.ClassAdmin.NewStd(errno.ErrDataInconsistent)
	// ErrLookupInconsistent returns for data inconsistency for index lookup.
	ErrLookupInconsistent = dbterror.ClassExecutor.NewStd(errno.ErrDataInconsistentMismatchCount)
	// ErrAdminCheckInconsistentWithColInfo returns for data inconsistency for admin check but with column info.
	ErrAdminCheckInconsistentWithColInfo = dbterror.ClassExecutor.NewStd(errno.ErrDataInconsistentMismatchIndex)
)

// GetMvccByKey gets the MVCC value by key, and returns a json string including decoded data
func GetMvccByKey(tikvStore helper.Storage, key kv.Key, decodeMvccFn func(kv.Key, *kvrpcpb.MvccGetByKeyResponse, map[string]any)) string {
	if key == nil {
		return ""
	}
	h := helper.NewHelper(tikvStore)
	data, err := h.GetMvccByEncodedKey(key)
	if err != nil {
		return ""
	}
	regionID := getRegionIDByKey(tikvStore, key)

	decodeKey := strings.ToUpper(hex.EncodeToString(key))

	resp := map[string]any{
		"key":      decodeKey,
		"regionID": regionID,
		"mvcc":     data,
	}

	if decodeMvccFn != nil {
		decodeMvccFn(key, data, resp)
	}

	rj, err := json.Marshal(resp)
	if err != nil {
		return ""
	}
	const maxMvccInfoLen = 5000
	s := string(rj)
	if len(s) > maxMvccInfoLen {
		s = s[:maxMvccInfoLen] + "[truncated]..."
	}

	return s
}

func getRegionIDByKey(tikvStore helper.Storage, encodedKey []byte) uint64 {
	keyLocation, err := tikvStore.GetRegionCache().LocateKey(tikv.NewBackofferWithVars(context.Background(), 500, nil), encodedKey)
	if err != nil {
		return 0
	}
	return keyLocation.Region.GetID()
}

// Reporter is a helper to generate report.
type Reporter struct {
	HandleEncode func(handle kv.Handle) kv.Key
	IndexEncode  func(idxRow *RecordData) kv.Key
	Tbl          *model.TableInfo
	Idx          *model.IndexInfo
	Sctx         sessionctx.Context
}

// DecodeRowMvccData creates a closure that captures the tableInfo to be used a decode function in GetMvccByKey.
func DecodeRowMvccData(tableInfo *model.TableInfo) func(kv.Key, *kvrpcpb.MvccGetByKeyResponse, map[string]any) {
	return func(_ kv.Key, respValue *kvrpcpb.MvccGetByKeyResponse, outMap map[string]any) {
		colMap := make(map[int64]*types.FieldType, 3)
		for _, col := range tableInfo.Columns {
			var fieldType = col.FieldType
			colMap[col.ID] = &fieldType
		}

		if respValue.Info != nil {
			var err error
			datas := make(map[string]map[string]string)
			for _, w := range respValue.Info.Writes {
				if len(w.ShortValue) > 0 {
					datas[strconv.FormatUint(w.StartTs, 10)], err = decodeMvccRecordValue(w.ShortValue, colMap, tableInfo)
				}
			}

			for _, v := range respValue.Info.Values {
				if len(v.Value) > 0 {
					datas[strconv.FormatUint(v.StartTs, 10)], err = decodeMvccRecordValue(v.Value, colMap, tableInfo)
				}
			}
			if len(datas) > 0 {
				outMap["decoded"] = datas
				if err != nil {
					outMap["decode_error"] = err.Error()
				}
			}
		}
	}
}

// DecodeIndexMvccData creates a closure that captures the indexInfo to be used a decode function in GetMvccByKey.
func DecodeIndexMvccData(indexInfo *model.IndexInfo) func(kv.Key, *kvrpcpb.MvccGetByKeyResponse, map[string]any) {
	return func(key kv.Key, respValue *kvrpcpb.MvccGetByKeyResponse, outMap map[string]any) {
		if respValue.Info != nil {
			var (
				hd    kv.Handle
				err   error
				datas = make(map[string]map[string]string)
			)
			for _, w := range respValue.Info.Writes {
				if len(w.ShortValue) > 0 {
					hd, err = tablecodec.DecodeIndexHandle(key, w.ShortValue, len(indexInfo.Columns))
					if err == nil {
						datas[strconv.FormatUint(w.StartTs, 10)] = map[string]string{"handle": hd.String()}
					}
				}
			}
			for _, v := range respValue.Info.Values {
				if len(v.Value) > 0 {
					hd, err = tablecodec.DecodeIndexHandle(key, v.Value, len(indexInfo.Columns))
					if err == nil {
						datas[strconv.FormatUint(v.StartTs, 10)] = map[string]string{"handle": hd.String()}
					}
				}
			}
			if len(datas) > 0 {
				outMap["decoded"] = datas
				if err != nil {
					outMap["decode_error"] = err.Error()
				}
			}
		}
	}
}

func decodeMvccRecordValue(bs []byte, colMap map[int64]*types.FieldType, tb *model.TableInfo) (map[string]string, error) {
	rs, err := tablecodec.DecodeRowToDatumMap(bs, colMap, time.UTC)
	record := make(map[string]string, len(tb.Columns))
	for _, col := range tb.Columns {
		if c, ok := rs[col.ID]; ok {
			data := "nil"
			if !c.IsNull() {
				data, err = c.ToString()
			}
			record[col.Name.O] = data
		}
	}
	return record, err
}

// ReportLookupInconsistent reports inconsistent when index rows is more than record rows.
func (r *Reporter) ReportLookupInconsistent(ctx context.Context, idxCnt, tblCnt int, missHd, fullHd []kv.Handle, missRowIdx []RecordData) error {
	rmode := r.Sctx.GetSessionVars().EnableRedactLog

	const maxFullHandleCnt = 50
	displayFullHdCnt := min(len(fullHd), maxFullHandleCnt)
	fs := []zap.Field{
		zap.String("table_name", r.Tbl.Name.O),
		zap.String("index_name", r.Idx.Name.O),
		zap.Int("index_cnt", idxCnt), zap.Int("table_cnt", tblCnt),
		zap.String("missing_handles", redact.String(rmode, fmt.Sprint(missHd))),
		zap.String("total_handles", redact.String(rmode, fmt.Sprint(fullHd[:displayFullHdCnt]))),
	}
	if rmode != errors.RedactLogEnable {
		store, ok := r.Sctx.GetStore().(helper.Storage)
		if ok {
			for i, hd := range missHd {
				fs = append(fs, zap.String("row_mvcc_"+strconv.Itoa(i), redact.String(rmode, GetMvccByKey(store, r.HandleEncode(hd), DecodeRowMvccData(r.Tbl)))))
			}
			for i := range missRowIdx {
				fs = append(fs, zap.String("index_mvcc_"+strconv.Itoa(i), redact.String(rmode, GetMvccByKey(store, r.IndexEncode(&missRowIdx[i]), DecodeIndexMvccData(r.Idx)))))
			}
		}
	}
	fs = append(fs, zap.Stack("stack"))
	logutil.Logger(ctx).Error("indexLookup found data inconsistency", fs...)
	return ErrLookupInconsistent.GenWithStackByArgs(r.Tbl.Name.O, r.Idx.Name.O, idxCnt, tblCnt)
}

// ReportAdminCheckInconsistentWithColInfo reports inconsistent when the value of index row is different from record row.
func (r *Reporter) ReportAdminCheckInconsistentWithColInfo(ctx context.Context, handle kv.Handle, colName string, idxDat, tblDat fmt.Stringer, err error, idxRow *RecordData) error {
	rmode := r.Sctx.GetSessionVars().EnableRedactLog
	fs := []zap.Field{
		zap.String("table_name", r.Tbl.Name.O),
		zap.String("index_name", r.Idx.Name.O),
		zap.String("col", colName),
		zap.Stringer("row_id", redact.Stringer(rmode, handle)),
		zap.Stringer("idxDatum", redact.Stringer(rmode, idxDat)),
		zap.Stringer("rowDatum", redact.Stringer(rmode, tblDat)),
	}
	if rmode != errors.RedactLogEnable {
		store, ok := r.Sctx.GetStore().(helper.Storage)
		if ok {
			fs = append(fs, zap.String("row_mvcc", redact.String(rmode, GetMvccByKey(store, r.HandleEncode(handle), DecodeRowMvccData(r.Tbl)))))
			fs = append(fs, zap.String("index_mvcc", redact.String(rmode, GetMvccByKey(store, r.IndexEncode(idxRow), DecodeIndexMvccData(r.Idx)))))
		}
	}
	fs = append(fs, zap.Error(err))
	fs = append(fs, zap.Stack("stack"))
	logutil.Logger(ctx).Error("admin check found data inconsistency", fs...)
	return ErrAdminCheckInconsistentWithColInfo.GenWithStackByArgs(r.Tbl.Name.O, r.Idx.Name.O, colName, fmt.Sprint(handle), fmt.Sprint(idxDat), fmt.Sprint(tblDat), err)
}

// RecordData is the record data composed of a handle and values.
type RecordData struct {
	Handle kv.Handle
	Values []types.Datum
}

func (r *RecordData) String() string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("handle: %s, values: %s", fmt.Sprint(r.Handle), fmt.Sprint(r.Values))
}

// ReportAdminCheckInconsistent reports inconsistent when single index row not found in record rows.
func (r *Reporter) ReportAdminCheckInconsistent(ctx context.Context, handle kv.Handle, idxRow, tblRow *RecordData) error {
	rmode := r.Sctx.GetSessionVars().EnableRedactLog
	fs := []zap.Field{
		zap.String("table_name", r.Tbl.Name.O),
		zap.String("index_name", r.Idx.Name.O),
		zap.Stringer("row_id", redact.Stringer(rmode, handle)),
		zap.Stringer("index", redact.Stringer(rmode, idxRow)),
		zap.Stringer("row", redact.Stringer(rmode, tblRow)),
	}
	if rmode != errors.RedactLogEnable {
		store, ok := r.Sctx.GetStore().(helper.Storage)
		if ok {
			fs = append(fs, zap.String("row_mvcc", redact.String(rmode, GetMvccByKey(store, r.HandleEncode(handle), DecodeRowMvccData(r.Tbl)))))
			if idxRow != nil {
				fs = append(fs, zap.String("index_mvcc", redact.String(rmode, GetMvccByKey(store, r.IndexEncode(idxRow), DecodeIndexMvccData(r.Idx)))))
			}
		}
	}
	fs = append(fs, zap.Stack("stack"))
	logutil.Logger(ctx).Error("admin check found data inconsistency", fs...)
	return ErrAdminCheckInconsistent.GenWithStackByArgs(r.Tbl.Name.O, r.Idx.Name.O, fmt.Sprint(handle), fmt.Sprint(idxRow), fmt.Sprint(tblRow))
}
