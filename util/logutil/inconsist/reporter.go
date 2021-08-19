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

package inconsist

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	// ErrAdminCheckInconsistent returns for data inconsistency for admin check.
	ErrAdminCheckInconsistent = dbterror.ClassAdmin.NewStd(errno.ErrDataInConsistent)
	// ErrLookupInconsistent returns for data inconsistency for index lookup.
	ErrLookupInconsistent = dbterror.ClassExecutor.NewStd(errno.ErrDataInconsistentMismatchCount)
	// ErrAdminCheckInconsistentWithColInfo returns for data inconsistency for admin check but with column info.
	ErrAdminCheckInconsistentWithColInfo = dbterror.ClassExecutor.NewStd(errno.ErrDataInconsistentMismatchIndex)
)

func getMvccByKey(sctx sessionctx.Context, key kv.Key, decodeMvccFn func(kv.Key, *kvrpcpb.MvccGetByKeyResponse, map[string]interface{})) string {
	if key == nil {
		return ""
	}
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return ""
	}
	h := helper.NewHelper(tikvStore)
	data, err := h.GetMvccByEncodedKey(key)
	if err != nil {
		return ""
	}
	regionID := getRegionIDByKey(tikvStore, key)

	decodeKey := strings.ToUpper(hex.EncodeToString(key))

	resp := map[string]interface{}{
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
	return string(rj)
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

func (r *Reporter) decodeRowMvccData(_ kv.Key, respValue *kvrpcpb.MvccGetByKeyResponse, outMap map[string]interface{}) {
	colMap := make(map[int64]*types.FieldType, 3)
	for _, col := range r.Tbl.Columns {
		colMap[col.ID] = &col.FieldType
	}

	if respValue.Info != nil {
		var err error
		datas := make(map[string][]map[string]string)
		for _, w := range respValue.Info.Writes {
			if len(w.ShortValue) > 0 {
				datas[strconv.FormatUint(w.StartTs, 10)], err = decodeMvccData(w.ShortValue, colMap, r.Tbl)
			}
		}

		for _, v := range respValue.Info.Values {
			if len(v.Value) > 0 {
				datas[strconv.FormatUint(v.StartTs, 10)], err = decodeMvccData(v.Value, colMap, r.Tbl)
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

func (r *Reporter) decodeIndexMvccData(key kv.Key, respValue *kvrpcpb.MvccGetByKeyResponse, outMap map[string]interface{}) {
	if respValue.Info != nil {
		var (
			hd    kv.Handle
			err   error
			datas = make(map[string]map[string]string)
		)
		for _, w := range respValue.Info.Writes {
			if len(w.ShortValue) > 0 {
				hd, err = tablecodec.DecodeIndexHandle(key, w.ShortValue, len(r.Idx.Columns))
				if err == nil {
					datas[strconv.FormatUint(w.StartTs, 10)] = map[string]string{"handle": hd.String()}
				}
			}
		}
		for _, v := range respValue.Info.Values {
			if len(v.Value) > 0 {
				hd, err = tablecodec.DecodeIndexHandle(key, v.Value, len(r.Idx.Columns))
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

func decodeMvccData(bs []byte, colMap map[int64]*types.FieldType, tb *model.TableInfo) ([]map[string]string, error) {
	rs, err := tablecodec.DecodeRowToDatumMap(bs, colMap, time.UTC)
	var record []map[string]string
	for _, col := range tb.Columns {
		if c, ok := rs[col.ID]; ok {
			data := "nil"
			if !c.IsNull() {
				data, err = c.ToString()
			}
			record = append(record, map[string]string{col.Name.O: data})
		}
	}
	return record, err
}

// ReportLookupInconsistent reports inconsistent when index rows is more than record rows.
func (r *Reporter) ReportLookupInconsistent(ctx context.Context, idxCnt, tblCnt int, missHd, fullHd []kv.Handle, missRowIdx []RecordData) error {
	if r.Sctx.GetSessionVars().EnableRedactLog {
		logutil.Logger(ctx).Error("indexLookup found data inconsistency",
			zap.String("table_name", r.Tbl.Name.O),
			zap.String("index", r.Idx.Name.O),
			zap.Int("index_cnt", idxCnt),
			zap.Int("table_cnt", tblCnt))
	} else {
		fs := []zap.Field{
			zap.String("table_name", r.Tbl.Name.O),
			zap.String("index_name", r.Idx.Name.O),
			zap.Int("index_cnt", idxCnt), zap.Int("table_cnt", tblCnt),
			zap.String("missing_handles", fmt.Sprint(missHd)),
			zap.String("total_handles", fmt.Sprint(fullHd)),
		}
		for i, hd := range missHd {
			fs = append(fs, zap.String("row_mvcc_"+strconv.Itoa(i), getMvccByKey(r.Sctx, r.HandleEncode(hd), r.decodeRowMvccData)))
		}
		for i, rowIdx := range missRowIdx {
			fs = append(fs, zap.String("index_mvcc_"+strconv.Itoa(i), getMvccByKey(r.Sctx, r.IndexEncode(&rowIdx), r.decodeIndexMvccData)))
		}
		logutil.Logger(ctx).Error("indexLookup found data inconsistency", fs...)
	}
	return ErrLookupInconsistent.GenWithStackByArgs(r.Tbl.Name.O, r.Idx.Name.O, idxCnt, tblCnt)
}

// ReportAdminCheckInconsistentWithColInfo reports inconsistent when the value of index row is different from record row.
func (r *Reporter) ReportAdminCheckInconsistentWithColInfo(ctx context.Context, handle kv.Handle, colName string, idxDat, tblDat fmt.Stringer, err error, idxRow *RecordData) error {
	if r.Sctx.GetSessionVars().EnableRedactLog {
		logutil.Logger(ctx).Error("admin check found data inconsistency",
			zap.String("table_name", r.Tbl.Name.O),
			zap.String("index", r.Idx.Name.O),
			zap.String("col", colName),
			zap.Error(err),
		)
	} else {
		fs := []zap.Field{
			zap.String("table_name", r.Tbl.Name.O),
			zap.String("index_name", r.Idx.Name.O),
			zap.String("col", colName),
			zap.Stringer("row_id", handle),
			zap.Stringer("idxDatum", idxDat),
			zap.Stringer("rowDatum", tblDat),
		}
		fs = append(fs, zap.String("row_mvcc", getMvccByKey(r.Sctx, r.HandleEncode(handle), r.decodeRowMvccData)))
		fs = append(fs, zap.String("index_mvcc", getMvccByKey(r.Sctx, r.IndexEncode(idxRow), r.decodeIndexMvccData)))
		fs = append(fs, zap.Error(err))
		logutil.Logger(ctx).Error("admin check found data inconsistency", fs...)
	}
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
	if r.Sctx.GetSessionVars().EnableRedactLog {
		logutil.Logger(ctx).Error("admin check found data inconsistency",
			zap.String("table_name", r.Tbl.Name.O),
			zap.String("index", r.Idx.Name.O),
		)
	} else {
		fs := []zap.Field{
			zap.String("table_name", r.Tbl.Name.O),
			zap.String("index_name", r.Idx.Name.O),
			zap.Stringer("row_id", handle),
			zap.Stringer("index", idxRow),
			zap.Stringer("row", tblRow),
		}
		fs = append(fs, zap.String("row_mvcc", getMvccByKey(r.Sctx, r.HandleEncode(handle), r.decodeRowMvccData)))
		if idxRow != nil {
			fs = append(fs, zap.String("index_mvcc", getMvccByKey(r.Sctx, r.IndexEncode(idxRow), r.decodeIndexMvccData)))
		}
		logutil.Logger(ctx).Error("admin check found data inconsistency", fs...)
	}
	return ErrAdminCheckInconsistent.GenWithStackByArgs(r.Tbl.Name.O, r.Idx.Name.O, fmt.Sprint(handle), fmt.Sprint(idxRow), fmt.Sprint(tblRow))
}
