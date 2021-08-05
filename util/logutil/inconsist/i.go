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

package inconsist

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	ErrDataInConsistentMisMatchIndex = dbterror.ClassExecutor.NewStd(errno.ErrDataInConsistentMisMatchIndex)
	ErrDataInConsistent              = dbterror.ClassAdmin.NewStd(errno.ErrDataInConsistent)
)

func getMvccByKey(sctx sessionctx.Context, key kv.Key) string {
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

// ReportHelper is a helper to generate report.
type ReportHelper struct {
	HandleEncode func(handle kv.Handle) kv.Key
	IndexEncode  func(idxRow *RecordData) kv.Key
}

// ReportCountMismatchInconsistent reports inconsistent when index rows is more than record rows.
func (r *ReportHelper) ReportCountMismatchInconsistent(ctx context.Context, sctx sessionctx.Context, tbl, idx string, idxCnt, tblCnt int, missHd, fullHd []kv.Handle, missRowIdx []RecordData) error {
	if sctx.GetSessionVars().EnableRedactLog {
		logutil.Logger(ctx).Error("inconsistent index handles",
			zap.String("table_name", tbl),
			zap.String("index", idx),
			zap.Int("index_cnt", idxCnt),
			zap.Int("table_cnt", tblCnt))
	} else {
		fs := []zap.Field{
			zap.String("table_name", tbl),
			zap.String("index", idx),
			zap.Int("index_cnt", idxCnt), zap.Int("table_cnt", tblCnt),
			zap.String("missing_handles", fmt.Sprint(missHd)),
			zap.String("total_handles", fmt.Sprint(fullHd)),
		}
		for i, hd := range missHd {
			fs = append(fs, zap.String("row_"+strconv.Itoa(i), getMvccByKey(sctx, r.HandleEncode(hd))))
		}
		for i, rowIdx := range missRowIdx {
			fs = append(fs, zap.String("index_"+strconv.Itoa(i), getMvccByKey(sctx, r.IndexEncode(&rowIdx))))
		}
		logutil.Logger(ctx).Error("inconsistent index handles", fs...)
	}
	return errors.Errorf("inconsistent found: table %, index %s, handle count %d isn't equal to value count %d",
		tbl, idx, idxCnt, tblCnt)
}

// ReportColValMismatchInconsistent reports inconsistent when the value of index row is different from record row.
func (r *ReportHelper) ReportColValMismatchInconsistent(ctx context.Context, sctx sessionctx.Context, tbl, idx string, handle kv.Handle, colName string, idxDat, tblDat fmt.Stringer, err error, idxRow *RecordData) error {
	if sctx.GetSessionVars().EnableRedactLog {
		logutil.Logger(ctx).Error("inconsistent index column value",
			zap.String("table_name", tbl),
			zap.String("index", idx),
			zap.String("col", colName),
			zap.Error(err),
		)
	} else {
		fs := []zap.Field{
			zap.String("table_name", tbl),
			zap.String("index", idx),
			zap.String("col", colName),
			zap.Stringer("handle", handle),
			zap.Stringer("idxDatum", idxDat),
			zap.Stringer("rowDatum", tblDat),
		}
		fs = append(fs, zap.String("row", getMvccByKey(sctx, r.HandleEncode(handle))))
		fs = append(fs, zap.String("index", getMvccByKey(sctx, r.IndexEncode(idxRow))))
		fs = append(fs, zap.Error(err))
		logutil.Logger(ctx).Error("inconsistent index column value", fs...)
	}
	return ErrDataInConsistentMisMatchIndex.GenWithStackByArgs(
		fmt.Sprintf("table: %s, index: %s, col: %s", tbl, idx, colName),
		fmt.Sprintf("%s(%v)", handle, handle), idxDat, tblDat, err)
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
	return fmt.Sprintf("handle: %s, values: %s", r.Handle, r.Values)
}

// ReportValueMismatchInconsistent reports inconsistent when single index row not found in record rows.
func (r *ReportHelper) ReportValueMismatchInconsistent(ctx context.Context, sctx sessionctx.Context, tbl, idx string, handle kv.Handle, idxRow, tblRow *RecordData) error {
	if sctx.GetSessionVars().EnableRedactLog {
		logutil.Logger(ctx).Error("inconsistent index values",
			zap.String("table_name", tbl),
			zap.String("index", idx),
		)
	} else {
		fs := []zap.Field{
			zap.String("table_name", tbl),
			zap.String("index", idx),
			zap.Stringer("handle", handle),
			zap.Stringer("index", idxRow),
			zap.Stringer("tbl", tblRow),
		}
		fs = append(fs, zap.String("row", getMvccByKey(sctx, r.HandleEncode(handle))))
		fs = append(fs, zap.String("index", getMvccByKey(sctx, r.IndexEncode(idxRow))))
		logutil.Logger(ctx).Error("inconsistent index values", fs...)
	}
	return ErrDataInConsistent.GenWithStackByArgs(idxRow, tblRow)
}
