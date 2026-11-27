// Copyright 2023 PingCAP, Inc.
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

package kv

import (
	"context"
	"math/rand"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	maxLogLength = 512 * 1024
)

// ExtraHandleColumnInfo is the column info of extra handle column.
var ExtraHandleColumnInfo = model.NewExtraHandleColInfo()

// GeneratedCol generated column info.
type GeneratedCol struct {
	// index of the column in the table
	Index int
	Expr  expression.Expression
}

// AutoIDConverterFn is a function to convert auto id.
type AutoIDConverterFn func(int64) int64

// RowArrayMarshaller wraps a slice of types.Datum for logging the content into zap.
type RowArrayMarshaller []types.Datum

var kindStr = [...]string{
	types.KindNull:          "null",
	types.KindInt64:         "int64",
	types.KindUint64:        "uint64",
	types.KindFloat32:       "float32",
	types.KindFloat64:       "float64",
	types.KindString:        "string",
	types.KindBytes:         "bytes",
	types.KindBinaryLiteral: "binary",
	types.KindMysqlDecimal:  "decimal",
	types.KindMysqlDuration: "duration",
	types.KindMysqlEnum:     "enum",
	types.KindMysqlBit:      "bit",
	types.KindMysqlSet:      "set",
	types.KindMysqlTime:     "time",
	types.KindInterface:     "interface",
	types.KindMinNotNull:    "min",
	types.KindMaxValue:      "max",
	types.KindRaw:           "raw",
	types.KindMysqlJSON:     "json",
}

// MarshalLogArray implements the zapcore.ArrayMarshaler interface
func (row RowArrayMarshaller) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	var totalLength = 0
	for _, datum := range row {
		kind := datum.Kind()
		var str string
		var err error
		switch kind {
		case types.KindNull:
			str = "NULL"
		case types.KindMinNotNull:
			str = "-inf"
		case types.KindMaxValue:
			str = "+inf"
		default:
			str, err = datum.ToString()
			if err != nil {
				return err
			}
		}
		if len(str) > maxLogLength {
			str = str[0:1024] + " (truncated)"
		}
		totalLength += len(str)
		if totalLength >= maxLogLength {
			encoder.AppendString("The row has been truncated, and the log has exited early.")
			return nil
		}
		if err := encoder.AppendObject(zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
			enc.AddString("kind", kindStr[kind])
			enc.AddString("val", redact.Value(str))
			return nil
		})); err != nil {
			return err
		}
	}
	return nil
}

// GroupCollector receives encoded data/index KVs directly grouped by index ID.
type GroupCollector interface {
	AddDataKV(pair common.KvPair, filledBuf *BytesBuf, memBuf *MemBuf)
	AddIndexKV(indexID int64, pair common.KvPair, filledBuf *BytesBuf, memBuf *MemBuf)
}

type bulkEncodeCol struct {
	colID      int64
	offset     int
	skipIfNull bool
}

type bulkEncodeIndex struct {
	index            table.Index
	idxInfo          *model.IndexInfo
	id               int64
	colOffsets       []int
	hasCondition     bool
	needRestoredData bool
}

// bulkRowEncoder encodes table records and secondary indexes directly using
// tablecodec (matching ADD INDEX's direct path) without going through
// Table.AddRecord's transactional DML scaffolding.
type bulkRowEncoder struct {
	tblInfo            *model.TableInfo
	phyTblID           int64
	useNewCollate      bool
	keyEncoder         codec.Encoder
	recordPrefix       kv.Key
	colCount           int
	pkHandleOffset     int
	cols               []bulkEncodeCol
	indices            []bulkEncodeIndex
	rowEncoder         *rowcodec.Encoder
	idxRowEncoder      rowcodec.Encoder
	rowChecksumEnabled bool

	intHandle     kv.IntHandle
	handleRef     kv.Handle
	recKeyBuf     []byte
	rowValBuf     []byte
	idxKeyBuf     []byte
	idxValBuf     []byte
	idxValsBuf    []types.Datum
	colIDsBuf     []int64
	rowDatumsBuf  []types.Datum
	addRowValsBuf []types.Datum
}

func newBulkRowEncoder(tbl table.Table, se *Session) *bulkRowEncoder {
	if _, ok := tbl.(*tables.TableCommon); !ok {
		return nil
	}
	meta := tbl.Meta()
	if meta.Partition != nil || meta.TempTableType != model.TempTableNone {
		return nil
	}
	if len(tbl.WritableConstraint()) > 0 || len(meta.Constraints) > 0 {
		return nil
	}
	if se.GetTableCtx().EnableMutationChecker() {
		return nil
	}
	pkHandleOffset := -1
	if meta.PKIsHandle {
		pkCol := meta.GetPkColInfo()
		if pkCol == nil {
			return nil
		}
		pkHandleOffset = pkCol.Offset
	} else if meta.IsCommonHandle || !common.TableHasAutoRowID(meta) {
		return nil
	}

	cols := tbl.Cols()
	writableCols := tbl.WritableCols()
	if len(meta.Columns) != len(cols) || len(cols) != len(writableCols) {
		return nil
	}
	encCols := make([]bulkEncodeCol, 0, len(cols))
	for _, col := range writableCols {
		if col.State != model.StatePublic || col.ChangeStateInfo != nil || col.ChangingFieldType != nil {
			return nil
		}
		if col.IsPKHandleColumn(meta) || col.IsVirtualGenerated() {
			continue
		}
		encCols = append(encCols, bulkEncodeCol{
			colID:      col.ID,
			offset:     col.Offset,
			skipIfNull: col.GetDefaultValue() == nil && col.GetOriginDefaultValue() == nil,
		})
	}

	useNewCollate := tbl.UseNewCollate()
	rawIndices := tbl.Indices()
	encIndices := make([]bulkEncodeIndex, 0, len(rawIndices))
	maxIdxCols := 0
	for _, idx := range rawIndices {
		idxMeta := idx.Meta()
		if idxMeta.State == model.StateNone || idxMeta.State == model.StateDeleteOnly || idxMeta.State == model.StateDeleteReorganization {
			continue
		}
		if idxMeta.State != model.StatePublic || idxMeta.Global || idxMeta.MVIndex {
			return nil
		}
		if idxMeta.IsColumnarIndex() {
			continue
		}
		if meta.IsCommonHandle && idxMeta.Primary {
			continue
		}
		offsets := make([]int, len(idxMeta.Columns))
		for i, ic := range idxMeta.Columns {
			offsets[i] = ic.Offset
		}
		if len(offsets) > maxIdxCols {
			maxIdxCols = len(offsets)
		}
		encIndices = append(encIndices, bulkEncodeIndex{
			index:            idx,
			idxInfo:          idxMeta,
			id:               idxMeta.ID,
			colOffsets:       offsets,
			hasCondition:     idxMeta.HasCondition(),
			needRestoredData: tables.NeedRestoredData(useNewCollate, idxMeta.Columns, meta.Columns),
		})
	}

	phyTblID := meta.ID
	if pt, ok := tbl.(table.PhysicalTable); ok {
		phyTblID = pt.GetPhysicalID()
	}

	rowCfg := se.GetTableCtx().GetRowEncodingConfig()
	b := &bulkRowEncoder{
		tblInfo:            meta,
		phyTblID:           phyTblID,
		useNewCollate:      useNewCollate,
		keyEncoder:         codec.NewEncoder(useNewCollate),
		recordPrefix:       tbl.RecordPrefix(),
		colCount:           len(cols),
		pkHandleOffset:     pkHandleOffset,
		cols:               encCols,
		indices:            encIndices,
		rowEncoder:         rowCfg.RowEncoder,
		idxRowEncoder:      rowcodec.Encoder{Enable: true},
		rowChecksumEnabled: rowCfg.IsRowLevelChecksumEnabled,
		recKeyBuf:          make([]byte, 0, tablecodec.RecordRowKeyLen),
		rowValBuf:          make([]byte, 0, 256),
		idxKeyBuf:          make([]byte, 0, 128),
		idxValBuf:          make([]byte, 0, 128),
		idxValsBuf:         make([]types.Datum, maxIdxCols),
		colIDsBuf:          make([]int64, 0, len(encCols)),
		rowDatumsBuf:       make([]types.Datum, 0, len(encCols)),
	}
	b.handleRef = &b.intHandle
	return b
}

func (b *bulkRowEncoder) encodeRow(
	se *Session,
	record []types.Datum,
	dst GroupCollector,
) (kvBytes int64, err error) {
	var handleID int64
	if len(record) > b.colCount {
		handleID = record[len(record)-1].GetInt64()
	} else {
		handleID = record[b.pkHandleOffset].GetInt64()
	}
	b.intHandle = kv.IntHandle(handleID)

	evalCtx := se.GetExprCtx().GetEvalCtx()
	loc, ec := evalCtx.Location(), evalCtx.ErrCtx()
	memBuf := &se.txn.MemBuf

	b.recKeyBuf = tablecodec.AppendRecordKey(b.recKeyBuf[:0], b.recordPrefix, b.handleRef)
	colIDs := b.colIDsBuf[:0]
	rowDatums := b.rowDatumsBuf[:0]
	for _, c := range b.cols {
		v := record[c.offset]
		if c.skipIfNull && v.IsNull() {
			continue
		}
		colIDs = append(colIDs, c.colID)
		rowDatums = append(rowDatums, v)
	}
	b.colIDsBuf = colIDs
	b.rowDatumsBuf = rowDatums

	var checksum rowcodec.Checksum
	if b.rowChecksumEnabled {
		checksum = rowcodec.RawChecksum{Handle: b.handleRef}
	}
	if b.rowEncoder == nil || !b.rowEncoder.Enable {
		need := len(rowDatums) * 2
		if cap(b.addRowValsBuf) < need {
			b.addRowValsBuf = make([]types.Datum, need)
		} else {
			b.addRowValsBuf = b.addRowValsBuf[:need]
		}
	}
	encodedRow, err := tablecodec.EncodeRow(loc, rowDatums, colIDs, b.rowValBuf, b.addRowValsBuf, checksum, b.rowEncoder)
	if err = ec.HandleError(err); err != nil {
		return 0, err
	}
	b.rowValBuf = encodedRow
	if dst != nil {
		k, v, filled := memBuf.CopyKV(b.recKeyBuf, encodedRow)
		dst.AddDataKV(common.KvPair{Key: k, Val: v}, filled, memBuf)
		kvBytes += int64(len(k) + len(v))
	} else {
		_ = memBuf.Set(b.recKeyBuf, encodedRow)
	}

	for i := range b.indices {
		idx := &b.indices[i]
		if idx.hasCondition {
			ok, err := idx.index.MeetPartialCondition(record)
			if err != nil {
				return 0, err
			}
			if !ok {
				continue
			}
		}
		idxVals := b.idxValsBuf[:len(idx.colOffsets)]
		for j, off := range idx.colOffsets {
			idxVals[j] = record[off]
		}
		key, distinct, err := tablecodec.GenIndexKey(b.keyEncoder, loc, b.tblInfo, idx.idxInfo, b.phyTblID, idxVals, b.handleRef, b.idxKeyBuf)
		if err = ec.HandleError(err); err != nil {
			return 0, err
		}
		b.idxKeyBuf = key
		val, err := tablecodec.GenIndexValuePortalWithEncoder(
			b.useNewCollate, loc, b.tblInfo, idx.idxInfo, idx.needRestoredData,
			distinct, false, idxVals, b.handleRef, b.phyTblID, nil, b.idxValBuf, &b.idxRowEncoder,
		)
		if err = ec.HandleError(err); err != nil {
			return 0, err
		}
		b.idxValBuf = val
		if dst != nil {
			k, v, filled := memBuf.CopyKV(key, val)
			dst.AddIndexKV(idx.id, common.KvPair{Key: k, Val: v}, filled, memBuf)
			kvBytes += int64(len(k) + len(v))
		} else {
			_ = memBuf.Set(key, val)
		}
	}
	return kvBytes, nil
}

// BaseKVEncoder encodes a row into a KV pair.
type BaseKVEncoder struct {
	GenCols    []GeneratedCol
	SessionCtx *Session
	table      table.Table
	// public columns
	Columns         []*table.Column
	AutoRandomColID int64
	// convert auto id for shard rowid or auto random id base on row id generated by lightning
	AutoIDFn AutoIDConverterFn

	logger          *zap.Logger
	recordCache     []types.Datum
	skipEncodeRowID bool
	bulkEncoder     *bulkRowEncoder
}

// NewBaseKVEncoder creates a new BaseKVEncoder.
func NewBaseKVEncoder(config *encode.EncodingConfig) (*BaseKVEncoder, error) {
	meta := config.Table.Meta()
	cols := config.Table.Cols()
	se, err := NewSession(&config.SessionOptions, config.Logger)
	if err != nil {
		return nil, err
	}
	se.exprCtx.setNewCollationEnabled(config.Table.UseNewCollate())

	var autoRandomColID int64
	autoIDFn := func(id int64) int64 { return id }
	if !config.UseIdentityAutoRowID {
		if meta.ContainsAutoRandomBits() {
			col := common.GetAutoRandomColumn(meta)
			autoRandomColID = col.ID

			shardFmt := autoid.NewShardIDFormat(&col.FieldType, meta.AutoRandomBits, meta.AutoRandomRangeBits)
			shard := rand.New(rand.NewSource(config.AutoRandomSeed)).Int63()
			autoIDFn = func(id int64) int64 {
				return shardFmt.Compose(shard, id)
			}
		} else if meta.ShardRowIDBits > 0 {
			rd := rand.New(rand.NewSource(config.AutoRandomSeed)) // nolint:gosec
			mask := int64(1)<<meta.ShardRowIDBits - 1
			shift := autoid.RowIDBitLength - meta.ShardRowIDBits - 1
			autoIDFn = func(id int64) int64 {
				rd.Seed(id)
				shardBits := (int64(rd.Uint32()) & mask) << shift
				return shardBits | id
			}
		}
	}

	// collect expressions for evaluating stored generated columns
	genCols, err := CollectGeneratedColumns(se, config.Table)
	if err != nil {
		return nil, errors.Annotate(err, "failed to parse generated column expressions")
	}
	return &BaseKVEncoder{
		GenCols:         genCols,
		SessionCtx:      se,
		table:           config.Table,
		Columns:         cols,
		AutoRandomColID: autoRandomColID,
		AutoIDFn:        autoIDFn,
		logger:          config.Logger.Logger,
		skipEncodeRowID: config.SkipEncodeRowID,
		bulkEncoder:     newBulkRowEncoder(config.Table, se),
	}, nil
}

// CanEncodeDirect reports whether Record2KVGroup can encode directly without
// creating intermediate kv.Pairs or decoding index IDs back from keys.
func (e *BaseKVEncoder) CanEncodeDirect() bool {
	return e.skipEncodeRowID && e.bulkEncoder != nil
}

// GetOrCreateRecord returns a record slice from the cache if possible, otherwise creates a new one.
func (e *BaseKVEncoder) GetOrCreateRecord() []types.Datum {
	if e.recordCache != nil {
		return e.recordCache
	}
	return make([]types.Datum, 0, len(e.Columns)+1)
}

// Record2KVGroup encodes a row directly into a grouped KV collector.
func (e *BaseKVEncoder) Record2KVGroup(record, originalRow []types.Datum, dst GroupCollector) (int64, error) {
	kvBytes, err := e.bulkEncoder.encodeRow(e.SessionCtx, record, dst)
	if err != nil {
		e.logger.Error("kv encode failed",
			zap.Array("originalRow", RowArrayMarshaller(originalRow)),
			zap.Array("convertedRow", RowArrayMarshaller(record)),
			log.ShortError(err),
		)
		return 0, errors.Trace(err)
	}
	e.recordCache = record[:0]
	return kvBytes, nil
}

// Record2KV converts a row into a KV pair.
func (e *BaseKVEncoder) Record2KV(record, originalRow []types.Datum, rowID int64) (*Pairs, error) {
	var err error
	if e.bulkEncoder != nil {
		_, err = e.bulkEncoder.encodeRow(e.SessionCtx, record, nil)
	} else {
		_, err = e.AddRecord(record)
	}
	if err != nil {
		e.logger.Error("kv encode failed",
			zap.Array("originalRow", RowArrayMarshaller(originalRow)),
			zap.Array("convertedRow", RowArrayMarshaller(record)),
			log.ShortError(err),
		)
		return nil, errors.Trace(err)
	}
	kvPairs := e.SessionCtx.TakeKvPairs()
	if !e.skipEncodeRowID {
		for i := range kvPairs.Pairs {
			var encoded [9]byte // The max length of encoded int64 is 9.
			kvPairs.Pairs[i].RowID = codec.EncodeComparableVarint(encoded[:0], rowID)
		}
	}
	e.recordCache = record[:0]
	return kvPairs, nil
}

// AddRecord adds a record into encoder
func (e *BaseKVEncoder) AddRecord(record []types.Datum) (kv.Handle, error) {
	return e.table.AddRecord(e.SessionCtx.GetTableCtx(), e.SessionCtx.Txn(), record, table.DupKeyCheckSkip)
}

// TableAllocators returns the allocators of the table
func (e *BaseKVEncoder) TableAllocators() autoid.Allocators {
	return e.table.Allocators(e.SessionCtx.GetTableCtx())
}

// TableMeta returns the meta of the table
func (e *BaseKVEncoder) TableMeta() *model.TableInfo {
	return e.table.Meta()
}

// ProcessColDatum processes the datum of a column.
func (e *BaseKVEncoder) ProcessColDatum(col *table.Column, rowID int64, inputDatum *types.Datum, needCast bool) (types.Datum, error) {
	value, err := e.getActualDatum(col, rowID, inputDatum, needCast)
	if err != nil {
		return value, err
	}

	if e.IsAutoRandomCol(col.ToInfo()) {
		meta := e.table.Meta()
		shardFmt := autoid.NewShardIDFormat(&col.FieldType, meta.AutoRandomBits, meta.AutoRandomRangeBits)
		// this allocator is the same as the allocator in table importer, i.e. PanickingAllocators. below too.
		alloc := e.TableAllocators().Get(autoid.AutoRandomType)
		if err := alloc.Rebase(context.Background(), value.GetInt64()&shardFmt.IncrementalMask(), false); err != nil {
			return value, errors.Trace(err)
		}
	}
	if IsAutoIncCol(col.ToInfo()) {
		// same as RowIDAllocType, since SepAutoInc is always false when initializing allocators of Table.
		alloc := e.TableAllocators().Get(autoid.AutoIncrementType)
		if err := alloc.Rebase(context.Background(), GetAutoRecordID(value, &col.FieldType), false); err != nil {
			return value, errors.Trace(err)
		}
	}
	return value, nil
}

func (e *BaseKVEncoder) getActualDatum(col *table.Column, rowID int64, inputDatum *types.Datum, needCast bool) (types.Datum, error) {
	var (
		value types.Datum
		err   error
	)

	isBadNullValue := false
	exprCtx := e.SessionCtx.GetExprCtx()
	errCtx := exprCtx.GetEvalCtx().ErrCtx()
	if inputDatum != nil {
		if needCast {
			value, err = table.CastColumnValue(exprCtx, *inputDatum, col.ToInfo(), false, false)
			if err != nil {
				return value, err
			}
		} else {
			value = *inputDatum
		}
		if err := col.CheckNotNull(&value, 0); err == nil {
			return value, nil // the most normal case
		}
		isBadNullValue = true
	}
	// handle special values
	switch {
	case IsAutoIncCol(col.ToInfo()):
		// we still need a conversion, e.g. to catch overflow with a TINYINT column.
		value, err = table.CastColumnValue(exprCtx,
			types.NewIntDatum(rowID), col.ToInfo(), false, false)
	case e.IsAutoRandomCol(col.ToInfo()):
		var val types.Datum
		realRowID := e.AutoIDFn(rowID)
		if mysql.HasUnsignedFlag(col.GetFlag()) {
			val = types.NewUintDatum(uint64(realRowID))
		} else {
			val = types.NewIntDatum(realRowID)
		}
		value, err = table.CastColumnValue(exprCtx, val, col.ToInfo(), false, false)
	case col.IsGenerated():
		// inject some dummy value for gen col so that MutRowFromDatums below sees a real value instead of nil.
		// if MutRowFromDatums sees a nil it won't initialize the underlying storage and cause SetDatum to panic.
		value = types.GetMinValue(&col.FieldType)
	case isBadNullValue:
		err = col.HandleBadNull(errCtx, &value, 0)
	default:
		value, err = table.GetColDefaultValue(e.SessionCtx.GetExprCtx(), col.ToInfo())
	}
	return value, err
}

// IsAutoRandomCol checks if the column is auto random column.
func (e *BaseKVEncoder) IsAutoRandomCol(col *model.ColumnInfo) bool {
	return e.table.Meta().ContainsAutoRandomBits() && col.ID == e.AutoRandomColID
}

// EvalGeneratedColumns evaluates the generated columns.
func (e *BaseKVEncoder) EvalGeneratedColumns(record []types.Datum,
	cols []*table.Column) (errCol *model.ColumnInfo, err error) {
	return evalGeneratedColumns(e.SessionCtx, record, cols, e.GenCols)
}

func datumToValueStringForCastError(d types.Datum) string {
	if d.Kind() == types.KindBytes {
		// types.DatumsToStringSmart only applies its printability detection to KindString.
		// Treat bytes as a string to get a stable, printable representation (or hex literal).
		d = types.NewStringDatum(d.GetString())
	}
	s, err := types.DatumsToStringSmart([]types.Datum{d}, true)
	if err != nil {
		return "<unprintable>"
	}
	return s
}

// LogKVConvertFailed logs the error when converting a row to KV pair failed.
func (e *BaseKVEncoder) LogKVConvertFailed(row []types.Datum, j int, colInfo *model.ColumnInfo, err error) error {
	var original types.Datum
	if 0 <= j && j < len(row) {
		original = row[j]
		row = row[j : j+1]
	}

	e.logger.Error("kv convert failed",
		zap.Array("original", RowArrayMarshaller(row)),
		zap.Int("originalCol", j),
		zap.String("colName", colInfo.Name.O),
		zap.Stringer("colType", &colInfo.FieldType),
		log.ShortError(err),
	)

	if len(original.GetString()) >= maxLogLength {
		originalPrefix := original.GetString()[0:1024] + " (truncated)"
		e.logger.Error("failed to convert kv value", logutil.RedactAny("origVal", originalPrefix),
			zap.Stringer("fieldType", &colInfo.FieldType), zap.String("column", colInfo.Name.O),
			zap.Int("columnID", j+1))
	} else {
		e.logger.Error("failed to convert kv value", logutil.RedactAny("origVal", original.GetValue()),
			zap.Stringer("fieldType", &colInfo.FieldType), zap.String("column", colInfo.Name.O),
			zap.Int("columnID", j+1))
	}

	if err == nil {
		return nil
	}

	badValue := datumToValueStringForCastError(original)
	reason := err.Error()
	return common.ErrCastValue.GenWithStackByArgs(colInfo.Name.O, &colInfo.FieldType, badValue, reason)
}

// LogEvalGenExprFailed logs the error when evaluating the generated column expression failed.
func (e *BaseKVEncoder) LogEvalGenExprFailed(row []types.Datum, colInfo *model.ColumnInfo, err error) error {
	e.logger.Error("kv convert failed: cannot evaluate generated column expression",
		zap.Array("original", RowArrayMarshaller(row)),
		zap.String("colName", colInfo.Name.O),
		log.ShortError(err),
	)

	return errors.Annotatef(
		err,
		"failed to evaluate generated column expression for column `%s`",
		colInfo.Name.O,
	)
}

// TruncateWarns resets the warnings in session context.
func (e *BaseKVEncoder) TruncateWarns() {
	e.SessionCtx.GetExprCtx().GetEvalCtx().TruncateWarnings(0)
}

func evalGeneratedColumns(se *Session, record []types.Datum, cols []*table.Column,
	genCols []GeneratedCol) (errCol *model.ColumnInfo, err error) {
	mutRow := chunk.MutRowFromDatums(record)
	for _, gc := range genCols {
		col := cols[gc.Index].ToInfo()
		evaluated, err := gc.Expr.Eval(se.GetExprCtx().GetEvalCtx(), mutRow.ToRow())
		if err != nil {
			return col, err
		}
		value, err := table.CastColumnValue(se.GetExprCtx(), evaluated, col, false, false)
		if err != nil {
			return col, err
		}
		mutRow.SetDatum(gc.Index, value)
		record[gc.Index] = value
	}
	return nil, nil
}
