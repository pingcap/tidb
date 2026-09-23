// Copyright 2015 PingCAP, Inc.
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

package admin

import (
	"context"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// RecordData is the record data composed of a handle and values.
type RecordData struct {
	Handle kv.Handle
	Values []types.Datum
}

func getCount(exec sqlexec.RestrictedSQLExecutor, snapshot uint64, sql string, args ...any) (int64, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnAdmin)
	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(snapshot)}, sql, args...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) != 1 {
		return 0, errors.Errorf("can not get count, rows count = %d", len(rows))
	}
	return rows[0].GetInt64(0), nil
}

// Count greater Types
const (
	// TblCntGreater means that the number of table rows is more than the number of index rows.
	TblCntGreater byte = 1
	// IdxCntGreater means that the number of index rows is more than the number of table rows.
	IdxCntGreater byte = 2
)

// CheckIndicesCount compares indices count with table count.
// It returns the count greater type, the index offset and an error.
// It returns nil if the count from the index is equal to the count from the table columns,
// otherwise it returns an error and the corresponding index's offset.
func CheckIndicesCount(ctx sessionctx.Context, dbName, tableName string, indices []string) (byte, int, error) {
	// Here we need check all indexes, includes invisible index
	originOptUseInvisibleIdx := ctx.GetSessionVars().OptimizerUseInvisibleIndexes
	ctx.GetSessionVars().OptimizerUseInvisibleIndexes = true
	defer func() {
		ctx.GetSessionVars().OptimizerUseInvisibleIndexes = originOptUseInvisibleIdx
	}()

	var snapshot uint64
	txn, err := ctx.Txn(false)
	if err != nil {
		return 0, 0, err
	}
	if txn.Valid() {
		snapshot = txn.StartTS()
	}
	if ctx.GetSessionVars().SnapshotTS != 0 {
		snapshot = ctx.GetSessionVars().SnapshotTS
	}

	// Add `` for some names like `table name`.
	exec := ctx.GetRestrictedSQLExecutor()
	tblCnt, err := getCount(exec, snapshot, "SELECT COUNT(*) FROM %n.%n USE INDEX()", dbName, tableName)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	for i, idx := range indices {
		idxCnt, err := getCount(exec, snapshot, "SELECT COUNT(*) FROM %n.%n USE INDEX(%n)", dbName, tableName, idx)
		if err != nil {
			return 0, i, errors.Trace(err)
		}
		logutil.Logger(context.Background()).Info("check indices count",
			zap.String("table", tableName), zap.Int64("tblCnt", tblCnt), zap.Reflect("index", idx), zap.Int64("idxCnt", idxCnt))
		if tblCnt == idxCnt {
			continue
		}

		var ret byte
		if tblCnt > idxCnt {
			ret = TblCntGreater
		} else if idxCnt > tblCnt {
			ret = IdxCntGreater
		}
		return ret, i, ErrAdminCheckTable.GenWithStack("table count %d != index(%s) count %d", tblCnt, idx, idxCnt)
	}
	return 0, 0, nil
}

// CheckRecordAndIndex is exported for testing.
func CheckRecordAndIndex(ctx context.Context, sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, idx table.Index) error {
	sc := sessCtx.GetSessionVars().StmtCtx
	cols := make([]*table.Column, len(idx.Meta().Columns))
	for i, col := range idx.Meta().Columns {
		cols[i] = t.Cols()[col.Offset]
	}

	ir := func() *consistency.Reporter {
		return &consistency.Reporter{
			HandleEncode: func(handle kv.Handle) kv.Key {
				return tablecodec.EncodeRecordKey(t.RecordPrefix(), handle)
			},
			IndexEncode: func(idxRow *consistency.RecordData) kv.Key {
				var matchingIdx table.Index
				for _, v := range t.Indices() {
					if strings.EqualFold(v.Meta().Name.String(), idx.Meta().Name.O) {
						matchingIdx = v
						break
					}
				}
				if matchingIdx == nil {
					return nil
				}
				k, _, err := matchingIdx.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), idxRow.Values, idxRow.Handle, nil)
				if err != nil {
					return nil
				}
				return k
			},
			Tbl:             t.Meta(),
			Idx:             idx.Meta(),
			EnableRedactLog: sessCtx.GetSessionVars().EnableRedactLog,
			Storage:         sessCtx.GetStore(),
		}
	}

	startKey := tablecodec.EncodeRecordKey(t.RecordPrefix(), kv.IntHandle(math.MinInt64))
	filterFunc := func(h1 kv.Handle, vals1 []types.Datum, cols []*table.Column) (bool, error) {
		for i, val := range vals1 {
			col := cols[i]
			if val.IsNull() {
				if mysql.HasNotNullFlag(col.GetFlag()) && col.ToInfo().GetOriginDefaultValue() == nil {
					return false, errors.Errorf("Column %v define as not null, but can't find the value where handle is %v", col.Name, h1)
				}
				// NULL value is regarded as its default value.
				colDefVal, err := table.GetColOriginDefaultValue(sessCtx.GetExprCtx(), col.ToInfo())
				if err != nil {
					return false, errors.Trace(err)
				}
				vals1[i] = colDefVal
			}
		}
		isExist, h2, err := idx.Exist(sc.ErrCtx(), sc.TimeZone(), txn, vals1, h1)
		if kv.ErrKeyExists.Equal(err) {
			record1 := &consistency.RecordData{Handle: h1, Values: vals1}
			record2 := &consistency.RecordData{Handle: h2, Values: vals1}
			return false, ir().ReportAdminCheckInconsistent(ctx, h1, record2, record1)
		}
		if err != nil {
			return false, errors.Trace(err)
		}
		if !isExist {
			record := &consistency.RecordData{Handle: h1, Values: vals1}
			return false, ir().ReportAdminCheckInconsistent(ctx, h1, nil, record)
		}

		return true, nil
	}
	err := iterRecords(sessCtx, txn, t, startKey, cols, idx.Meta().Global, filterFunc)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// CheckFullTextIndexAndRecord is the index-to-record direction of ADMIN CHECK
// for a FULLTEXT index built in TiKV: every entry must belong to a row that
// exists, and its term must be one the row's document analyzes to. The
// record-to-index direction is CheckRecordAndIndex, which for this index
// checks each row for an entry per term.
func CheckFullTextIndexAndRecord(ctx context.Context, sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, idx table.Index) error {
	idxInfo := idx.Meta()
	if len(idxInfo.Columns) != 1 {
		return errors.Errorf("fulltext index %s must index one column", idxInfo.Name.O)
	}
	col := t.Cols()[idxInfo.Columns[0].Offset]
	rowDecoder, err := makeRowDecoder(t, sessCtx)
	if err != nil {
		return err
	}
	report := func(handle kv.Handle, term string, row *consistency.RecordData) error {
		reporter := &consistency.Reporter{
			HandleEncode:    func(handle kv.Handle) kv.Key { return tablecodec.EncodeRecordKey(t.RecordPrefix(), handle) },
			IndexEncode:     func(*consistency.RecordData) kv.Key { return nil },
			Tbl:             t.Meta(),
			Idx:             idxInfo,
			EnableRedactLog: sessCtx.GetSessionVars().EnableRedactLog,
			Storage:         sessCtx.GetStore(),
		}
		entry := &consistency.RecordData{Handle: handle, Values: []types.Datum{types.NewStringDatum(term)}}
		return reporter.ReportAdminCheckInconsistent(ctx, handle, entry, row)
	}

	prefix := tablecodec.EncodeTableIndexPrefix(t.Meta().ID, idxInfo.ID)
	it, err := txn.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()
	// Entries of one row are spread over its terms, so rows are fetched and
	// tokenized once and remembered while their entries keep appearing.
	rowTerms := make(map[string]map[string]struct{})
	for it.Valid() {
		term, err := tables.DecodeTiKVFullTextIndexKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}
		handle, err := tablecodec.DecodeIndexHandle(it.Key(), it.Value(), 1)
		if err != nil {
			return errors.Trace(err)
		}
		if err := it.Next(); err != nil {
			return errors.Trace(err)
		}
		handleKey := string(handle.Encoded())
		terms, seen := rowTerms[handleKey]
		if !seen {
			rowValue, err := kv.GetValue(ctx, txn, tablecodec.EncodeRecordKey(t.RecordPrefix(), handle))
			if kv.ErrNotExist.Equal(err) {
				return report(handle, string(term), nil)
			}
			if err != nil {
				return errors.Trace(err)
			}
			rowMap, err := rowDecoder.DecodeAndEvalRowWithMap(sessCtx.GetExprCtx(), handle, rowValue, sessCtx.GetSessionVars().Location(), nil)
			if err != nil {
				return errors.Trace(err)
			}
			if terms, err = tables.FullTextIndexTerms(idx, rowMap[col.ID]); err != nil {
				return errors.Trace(err)
			}
			rowTerms[handleKey] = terms
		}
		if _, ok := terms[string(term)]; !ok {
			row := &consistency.RecordData{Handle: handle}
			return report(handle, string(term), row)
		}
	}
	return nil
}

func makeRowDecoder(t table.Table, sctx sessionctx.Context) (*decoder.RowDecoder, error) {
	dbName := ast.NewCIStr(sctx.GetSessionVars().CurrentDB)
	exprCols, _, err := expression.ColumnInfos2ColumnsAndNames(sctx.GetExprCtx(), dbName, t.Meta().Name, t.Meta().Cols(), t.Meta())
	if err != nil {
		return nil, err
	}
	mockSchema := expression.NewSchema(exprCols...)
	decodeColsMap := decoder.BuildFullDecodeColMap(t.Cols(), mockSchema)

	return decoder.NewRowDecoder(t, t.Cols(), decodeColsMap), nil
}

func iterRecords(sessCtx sessionctx.Context, retriever kv.Retriever, t table.Table, startKey kv.Key, cols []*table.Column, isGlobalIndex bool, fn table.RecordIterFunc) error {
	prefix := t.RecordPrefix()
	keyUpperBound := prefix.PrefixNext()

	it, err := retriever.Iter(startKey, keyUpperBound)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	logutil.BgLogger().Debug("record",
		zap.Stringer("startKey", startKey),
		zap.Stringer("key", it.Key()),
		zap.Binary("value", it.Value()))
	rowDecoder, err := makeRowDecoder(t, sessCtx)
	if err != nil {
		return err
	}
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}
		if isGlobalIndex {
			handle = kv.NewPartitionHandle(tablecodec.DecodeTableID(it.Key()), handle)
		}

		rowMap, err := rowDecoder.DecodeAndEvalRowWithMap(sessCtx.GetExprCtx(), handle, it.Value(), sessCtx.GetSessionVars().Location(), nil)
		if err != nil {
			return errors.Trace(err)
		}
		data := make([]types.Datum, 0, len(cols))
		for _, col := range cols {
			data = append(data, rowMap[col.ID])
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return errors.Trace(err)
		}

		rk := tablecodec.EncodeRecordKey(t.RecordPrefix(), handle)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

var (
	// ErrAdminCheckTable returns when the table records is inconsistent with the index values.
	ErrAdminCheckTable = dbterror.ClassAdmin.NewStd(errno.ErrAdminCheckTable)
)
