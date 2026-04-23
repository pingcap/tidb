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

package mydump

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/xitongsys/parquet-go/parquet"
	preader "github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"go.uber.org/zap"
)

const (
	batchReadRowSize = 32

	// if a parquet if small than this threshold, parquet will load the whole file in a byte slice to
	// optimize the read performance
	smallParquetFileThreshold = 256 * 1024 * 1024
	// jan011970 is the date of unix epoch in julian day,
	jan011970 = 2440588
	secPerDay = 24 * 60 * 60

	utcTimeLayout = "2006-01-02 15:04:05.999999Z"
	timeLayout    = "2006-01-02 15:04:05.999999"
)

<<<<<<< HEAD
=======
var (
	unsupportedParquetTypes = map[schema.ConvertedType]struct{}{
		// TODO(joechenrh): support read list type as vector
		schema.ConvertedTypes.List: {},
		// The below types are not used for data exported from aurora/snowflake,
		// so we don't support them for now.
		schema.ConvertedTypes.Map:         {},
		schema.ConvertedTypes.MapKeyValue: {},
		schema.ConvertedTypes.Interval:    {},
		schema.ConvertedTypes.NA:          {},
	}

	// readBatchSize is the number of rows to read in a single batch
	// from parquet column reader. Modified in test.
	readBatchSize = 128
)

func estimateRowSize(row []types.Datum) int {
	length := 0
	for _, v := range row {
		if v.IsNull() {
			continue
		}
		if v.Kind() == types.KindString {
			length += len(v.GetBytes())
		} else {
			length += 8
		}
	}
	return length
}

// innerReader defines the interface for reading value with given type T from parquet column reader.
type innerReader[T parquet.ColumnTypes] interface {
	ReadBatchInPage(batchSize int64, values []T, defLvls, repLvls []int16) (int64, int, error)
}

type iterator interface {
	SetReader(colReader file.ColumnChunkReader)

	Next(*types.Datum) error

	Close() error
}

type columnIterator[T parquet.ColumnTypes, R innerReader[T]] struct {
	baseReader file.ColumnChunkReader
	reader     R

	batchSize      int64
	valueOffset    int
	valuesBuffered int

	levelOffset    int64
	levelsBuffered int64
	defLevels      []int16
	repLevels      []int16
	values         []T

	setter setter[T]
}

// newColumnIterator creates a new generic column iterator
// The iterator should not be used in parallel.
func newColumnIterator[T parquet.ColumnTypes, R innerReader[T]](
	batchSize int, getter setter[T],
) *columnIterator[T, R] {
	return &columnIterator[T, R]{
		batchSize: int64(batchSize),
		defLevels: make([]int16, batchSize),
		repLevels: make([]int16, batchSize),
		values:    make([]T, batchSize),
		setter:    getter,
	}
}

// SetReader sets the column reader for the iterator.
// Remember to call Close() before setting a new reader.
func (it *columnIterator[T, R]) SetReader(colReader file.ColumnChunkReader) {
	it.baseReader = colReader
	it.reader, _ = colReader.(R)
}

func (it *columnIterator[T, R]) Close() error {
	if it.baseReader == nil {
		return nil
	}

	err := it.baseReader.Close()
	it.baseReader = nil
	return err
}

func (it *columnIterator[T, R]) readNextBatch() error {
	// ReadBatchInPage reads a batch of values from the current page.
	// And the values returned may be shallow copies from the internal page buffer.
	var err error
	it.levelsBuffered, it.valuesBuffered, err = it.reader.ReadBatchInPage(
		it.batchSize,
		it.values,
		it.defLevels,
		it.repLevels,
	)

	it.valueOffset = 0
	it.levelOffset = 0
	return err
}

// Next reads the next value with proper level handling.
func (it *columnIterator[T, R]) Next(d *types.Datum) error {
	if it.levelOffset == it.levelsBuffered {
		err := it.readNextBatch()
		if err != nil {
			return errors.Trace(err)
		}
		if it.levelsBuffered == 0 {
			return io.EOF
		}
	}

	// Check definition level for NULL handling
	defLevel := it.defLevels[it.levelOffset]
	it.levelOffset++

	if defLevel < it.baseReader.Descriptor().MaxDefinitionLevel() {
		d.SetNull()
		return nil
	}

	value := it.values[it.valueOffset]
	it.valueOffset++
	return it.setter(value, d)
}

func createColumnIterator(tp parquet.Type, converted *convertedType, loc *time.Location, batchSize int) iterator {
	switch tp {
	case parquet.Types.Boolean:
		return newColumnIterator[bool, *file.BooleanColumnChunkReader](batchSize, getBoolDataSetter)
	case parquet.Types.Int32:
		return newColumnIterator[int32, *file.Int32ColumnChunkReader](batchSize, getInt32Setter(converted, loc))
	case parquet.Types.Int64:
		return newColumnIterator[int64, *file.Int64ColumnChunkReader](batchSize, getInt64Setter(converted, loc))
	case parquet.Types.Float:
		return newColumnIterator[float32, *file.Float32ColumnChunkReader](batchSize, setFloat32Data)
	case parquet.Types.Double:
		return newColumnIterator[float64, *file.Float64ColumnChunkReader](batchSize, setFloat64Data)
	case parquet.Types.Int96:
		return newColumnIterator[parquet.Int96, *file.Int96ColumnChunkReader](batchSize, getInt96Setter(converted, loc))
	case parquet.Types.ByteArray:
		return newColumnIterator[parquet.ByteArray, *file.ByteArrayColumnChunkReader](batchSize, getByteArraySetter(converted))
	case parquet.Types.FixedLenByteArray:
		return newColumnIterator[parquet.FixedLenByteArray, *file.FixedLenByteArrayColumnChunkReader](batchSize, getFixedLenByteArraySetter(converted))
	default:
		return nil
	}
}

// convertedType is older representation of the logical type in parquet
// ref: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
type convertedType struct {
	converted   schema.ConvertedType
	decimalMeta schema.DecimalMetadata

	// See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#temporal-types
	IsAdjustedToUTC bool

	// sparkRebaseMicros is non-empty when the file footer says the column was
	// written by a Spark release that used the legacy hybrid Julian/Gregorian
	// calendar for ancient DATE/TIMESTAMP values. It also caches the generated
	// Spark timezone rebase table for TIMESTAMP and INT96 value conversion.
	sparkRebaseMicros sparkRebaseMicrosLookup
}

// rowGroupParser parses rows from one parquet row group.
type rowGroupParser struct {
	rowGroup  int
	readRows  int64
	totalRows int64

	readers   []*file.Reader
	iterators []iterator
}

// init creates column iterators for each column.
func (rgp *rowGroupParser) init(colTypes []convertedType, loc *time.Location) (err error) {
	meta := rgp.readers[0].MetaData()
	numCols := meta.Schema.NumColumns()
	rgp.iterators = make([]iterator, numCols)

	defer func() {
		if err != nil {
			for _, iter := range rgp.iterators {
				if iter != nil {
					_ = iter.Close()
				}
			}
		}
	}()

	for idx := range numCols {
		tp := meta.Schema.Column(idx).PhysicalType()
		iter := createColumnIterator(tp, &colTypes[idx], loc, readBatchSize)
		if iter == nil {
			return errors.Errorf("unsupported parquet type %s", tp.String())
		}

		rowGroup := rgp.readers[idx].RowGroup(rgp.rowGroup)
		colReader, err := rowGroup.Column(idx)
		if err != nil {
			return errors.Trace(err)
		}
		iter.SetReader(colReader)
		rgp.iterators[idx] = iter
	}
	rgp.totalRows = meta.RowGroups[rgp.rowGroup].NumRows
	return nil
}

func (rgp *rowGroupParser) isDone() bool {
	return rgp == nil || rgp.readRows == rgp.totalRows
}

func (rgp *rowGroupParser) readRow(row []types.Datum) error {
	if rgp.isDone() {
		return io.EOF
	}

	for col, iter := range rgp.iterators {
		if err := iter.Next(&row[col]); err != nil {
			return errors.Annotate(err, "parquet read column failed")
		}
	}
	rgp.readRows++
	return nil
}

func (rgp *rowGroupParser) Close() error {
	var onceErr common.OnceError
	for _, iter := range rgp.iterators {
		err := iter.Close()
		onceErr.Set(err)
	}
	for _, r := range rgp.readers {
		err := r.Close()
		onceErr.Set(err)
	}
	return onceErr.Get()
}

>>>>>>> 73b01d05c64 (import: honor Spark legacy Parquet datetime metadata (#67908))
// ParquetParser parses a parquet file for import
// It implements the Parser interface.
type ParquetParser struct {
	Reader      *preader.ParquetReader
	columns     []string
	columnMetas []*parquet.SchemaElement
	rows        []any
	readRows    int64
	curStart    int64
	curIndex    int
	lastRow     Row
	logger      log.Logger

	readSeekCloser ReadSeekCloser
}

// readerWrapper is a used for implement `source.ParquetFile`
type readerWrapper struct {
	ReadSeekCloser
	store storage.ExternalStorage
	ctx   context.Context
	// current file path
	path string
}

func (*readerWrapper) Write(_ []byte) (n int, err error) {
	return 0, errors.New("unsupported operation")
}

func (r *readerWrapper) Open(name string) (source.ParquetFile, error) {
	if len(name) == 0 {
		name = r.path
	}
	reader, err := r.store.Open(r.ctx, name, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &readerWrapper{
		ReadSeekCloser: reader,
		store:          r.store,
		ctx:            r.ctx,
		path:           name,
	}, nil
}

func (*readerWrapper) Create(_ string) (source.ParquetFile, error) {
	return nil, errors.New("unsupported operation")
}

// bytesReaderWrapper is a wrapper of bytes.Reader used for implement `source.ParquetFile`
type bytesReaderWrapper struct {
	*bytes.Reader
	rawBytes []byte
	// current file path
	path string
}

func (*bytesReaderWrapper) Close() error {
	return nil
}

func (*bytesReaderWrapper) Create(_ string) (source.ParquetFile, error) {
	return nil, errors.New("unsupported operation")
}

func (*bytesReaderWrapper) Write(_ []byte) (n int, err error) {
	return 0, errors.New("unsupported operation")
}

func (r *bytesReaderWrapper) Open(name string) (source.ParquetFile, error) {
	if len(name) > 0 && name != r.path {
		panic(fmt.Sprintf("Open with a different name is not supported! current: '%s', new: '%s'", r.path, name))
	}
	return &bytesReaderWrapper{
		Reader:   bytes.NewReader(r.rawBytes),
		rawBytes: r.rawBytes,
		path:     r.path,
	}, nil
}

// OpenParquetReader opens a parquet file and returns a handle that can at least read the file.
func OpenParquetReader(
	ctx context.Context,
	store storage.ExternalStorage,
	path string,
	size int64,
) (source.ParquetFile, error) {
	if size <= smallParquetFileThreshold {
		fileBytes, err := store.ReadFile(ctx, path)
		if err != nil {
			return nil, err
		}
		return &bytesReaderWrapper{
			Reader:   bytes.NewReader(fileBytes),
			rawBytes: fileBytes,
			path:     path,
		}, nil
	}

	r, err := store.Open(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	return &readerWrapper{
		ReadSeekCloser: r,
		store:          store,
		ctx:            ctx,
		path:           path,
	}, nil
}

// ReadParquetFileRowCountByFile reads the parquet file row count.
// It is a special func to fetch parquet file row count fast.
func ReadParquetFileRowCountByFile(
	ctx context.Context,
	store storage.ExternalStorage,
	fileMeta SourceFileMeta,
) (int64, error) {
	r, err := store.Open(ctx, fileMeta.Path, nil)
	if err != nil {
		return 0, errors.Trace(err)
	}

	wrapper := &readerWrapper{
		ReadSeekCloser: r,
		store:          store,
		ctx:            ctx,
		path:           fileMeta.Path,
	}

	//nolint: errcheck
	defer wrapper.Close()

	res := new(preader.ParquetReader)
	res.NP = 1
	res.PFile = wrapper
	if err = res.ReadFooter(); err != nil {
		return 0, err
	}

	return res.Footer.NumRows, nil
}

// NewParquetParser generates a parquet parser.
func NewParquetParser(
	ctx context.Context,
	store storage.ExternalStorage,
	r storage.ReadSeekCloser,
	path string,
) (*ParquetParser, error) {
	// check to avoid wrapping twice
	wrapper, ok := r.(source.ParquetFile)
	if !ok {
		wrapper = &readerWrapper{
			ReadSeekCloser: r,
			store:          store,
			ctx:            ctx,
			path:           path,
		}
	}

	// FIXME: need to bench what the best value for the concurrent reader number
	reader, err := preader.NewParquetReader(wrapper, nil, 2)
	if err != nil {
		return nil, errors.Trace(err)
	}

	columns := make([]string, 0, len(reader.Footer.Schema)-1)
	columnMetas := make([]*parquet.SchemaElement, 0, len(reader.Footer.Schema)-1)
	for i, c := range reader.SchemaHandler.SchemaElements {
		if c.GetNumChildren() == 0 {
			// we need to use the raw name, SchemaElement.Name might be prefixed with PARGO_PERFIX_
			columns = append(columns, strings.ToLower(reader.SchemaHandler.GetExName(i)))
			// transfer old ConvertedType to LogicalType
			columnMeta := c
			if c.ConvertedType != nil && c.LogicalType == nil {
				newMeta := *c
				columnMeta = &newMeta
				if err := convertToLogicType(columnMeta); err != nil {
					return nil, err
				}
			}
			columnMetas = append(columnMetas, columnMeta)
		}
	}

	return &ParquetParser{
		Reader:         reader,
		columns:        columns,
		columnMetas:    columnMetas,
		logger:         log.Wrap(logutil.Logger(ctx)),
		readSeekCloser: wrapper,
	}, nil
}

func convertToLogicType(se *parquet.SchemaElement) error {
	logicalType := &parquet.LogicalType{}
	switch *se.ConvertedType {
	case parquet.ConvertedType_UTF8:
		logicalType.STRING = &parquet.StringType{}
	case parquet.ConvertedType_ENUM:
		logicalType.ENUM = &parquet.EnumType{}
	case parquet.ConvertedType_DECIMAL:
		logicalType.DECIMAL = &parquet.DecimalType{
			Scale:     *se.Scale,
			Precision: *se.Precision,
		}
	case parquet.ConvertedType_DATE:
		logicalType.DATE = &parquet.DateType{}
	case parquet.ConvertedType_TIME_MILLIS:
		logicalType.TIME = &parquet.TimeType{
			IsAdjustedToUTC: true,
			Unit: &parquet.TimeUnit{
				MILLIS: parquet.NewMilliSeconds(),
			},
		}
	case parquet.ConvertedType_TIME_MICROS:
		logicalType.TIME = &parquet.TimeType{
			IsAdjustedToUTC: true,
			Unit: &parquet.TimeUnit{
				MICROS: parquet.NewMicroSeconds(),
			},
		}
	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		logicalType.TIMESTAMP = &parquet.TimestampType{
			IsAdjustedToUTC: true,
			Unit: &parquet.TimeUnit{
				MILLIS: parquet.NewMilliSeconds(),
			},
		}
	case parquet.ConvertedType_TIMESTAMP_MICROS:
		logicalType.TIMESTAMP = &parquet.TimestampType{
			IsAdjustedToUTC: true,
			Unit: &parquet.TimeUnit{
				MICROS: parquet.NewMicroSeconds(),
			},
		}
	case parquet.ConvertedType_UINT_8:
		logicalType.INTEGER = &parquet.IntType{
			BitWidth: 8,
			IsSigned: false,
		}
	case parquet.ConvertedType_UINT_16:
		logicalType.INTEGER = &parquet.IntType{
			BitWidth: 16,
			IsSigned: false,
		}
	case parquet.ConvertedType_UINT_32:
		logicalType.INTEGER = &parquet.IntType{
			BitWidth: 32,
			IsSigned: false,
		}
	case parquet.ConvertedType_UINT_64:
		logicalType.INTEGER = &parquet.IntType{
			BitWidth: 64,
			IsSigned: false,
		}
	case parquet.ConvertedType_INT_8:
		logicalType.INTEGER = &parquet.IntType{
			BitWidth: 8,
			IsSigned: true,
		}
	case parquet.ConvertedType_INT_16:
		logicalType.INTEGER = &parquet.IntType{
			BitWidth: 16,
			IsSigned: true,
		}
	case parquet.ConvertedType_INT_32:
		logicalType.INTEGER = &parquet.IntType{
			BitWidth: 32,
			IsSigned: true,
		}
	case parquet.ConvertedType_INT_64:
		logicalType.INTEGER = &parquet.IntType{
			BitWidth: 64,
			IsSigned: true,
		}
	case parquet.ConvertedType_JSON:
		logicalType.JSON = &parquet.JsonType{}
	case parquet.ConvertedType_BSON:
		logicalType.BSON = &parquet.BsonType{}
	// case parquet.ConvertedType_INTERVAL, parquet.ConvertedType_MAP, parquet.ConvertedType_MAP_KEY_VALUE, parquet.ConvertedType_LIST:
	default:
		return errors.Errorf("unsupported type: '%s'", *se.ConvertedType)
	}
	se.LogicalType = logicalType
	return nil
}

// Pos returns the currently row number of the parquet file
func (pp *ParquetParser) Pos() (pos int64, rowID int64) {
	return pp.curStart + int64(pp.curIndex), pp.lastRow.RowID
}

// SetPos sets the position in a parquet file.
// It implements the Parser interface.
func (pp *ParquetParser) SetPos(pos int64, rowID int64) error {
	if pos < pp.curStart {
		panic("don't support seek back yet")
	}
	pp.lastRow.RowID = rowID

	if pos < pp.curStart+int64(len(pp.rows)) {
		pp.curIndex = int(pos - pp.curStart)
		pp.readRows = pos
		return nil
	}

	if pos > pp.curStart+int64(len(pp.rows)) {
		if err := pp.Reader.SkipRows(pos - pp.curStart - int64(len(pp.rows))); err != nil {
			return errors.Trace(err)
		}
	}
	pp.curStart = pos
	pp.readRows = pos
	pp.curIndex = 0
	if len(pp.rows) > 0 {
		pp.rows = pp.rows[:0]
	}

	return nil
}

// ScannedPos implements the Parser interface.
// For parquet it's parquet file's reader current position.
func (pp *ParquetParser) ScannedPos() (int64, error) {
	return pp.readSeekCloser.Seek(0, io.SeekCurrent)
}

// Close closes the parquet file of the parser.
// It implements the Parser interface.
func (pp *ParquetParser) Close() error {
	pp.Reader.ReadStop()
	return pp.Reader.PFile.Close()
}

// ReadRow reads a row in the parquet file by the parser.
// It implements the Parser interface.
func (pp *ParquetParser) ReadRow() error {
	pp.lastRow.RowID++
	pp.lastRow.Length = 0
	if pp.curIndex >= len(pp.rows) {
		if pp.readRows >= pp.Reader.GetNumRows() {
			return io.EOF
		}
		count := batchReadRowSize
		if pp.Reader.GetNumRows()-pp.readRows < int64(count) {
			count = int(pp.Reader.GetNumRows() - pp.readRows)
		}

		var err error
		pp.rows, err = pp.Reader.ReadByNumber(count)
		if err != nil {
			return errors.Trace(err)
		}
		pp.curStart = pp.readRows
		pp.readRows += int64(len(pp.rows))
		pp.curIndex = 0
	}

	row := pp.rows[pp.curIndex]
	pp.curIndex++

	v := reflect.ValueOf(row)
	length := v.NumField()
	if cap(pp.lastRow.Row) < length {
		pp.lastRow.Row = make([]types.Datum, length)
	} else {
		pp.lastRow.Row = pp.lastRow.Row[:length]
	}
	for i := range length {
		pp.lastRow.Length += getDatumLen(v.Field(i))
		if err := setDatumValue(&pp.lastRow.Row[i], v.Field(i), pp.columnMetas[i], pp.logger); err != nil {
			return err
		}
	}
	return nil
}

func getDatumLen(v reflect.Value) int {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return 0
		}
		return getDatumLen(v.Elem())
	}
	if v.Kind() == reflect.String {
		return len(v.String())
	}
	return 8
}

// convert a parquet value to Datum
//
// See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
func setDatumValue(d *types.Datum, v reflect.Value, meta *parquet.SchemaElement, logger log.Logger) error {
	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			d.SetUint64(1)
		} else {
			d.SetUint64(0)
		}
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		d.SetUint64(v.Uint())
	case reflect.Int8, reflect.Int16:
		d.SetInt64(v.Int())
	case reflect.Int32, reflect.Int64:
		return setDatumByInt(d, v.Int(), meta)
	case reflect.String:
		setDatumByString(d, v.String(), meta)
	case reflect.Float32, reflect.Float64:
		d.SetFloat64(v.Float())
	case reflect.Ptr:
		if !v.IsNil() {
			return setDatumValue(d, v.Elem(), meta, logger)
		}
		d.SetNull()
	default:
		logger.Error("unknown value", zap.Stringer("kind", v.Kind()),
			zap.String("type", v.Type().Name()), zap.Reflect("value", v.Interface()))
		return errors.Errorf("unknown value: %v", v)
	}
	return nil
}

func setDatumByString(d *types.Datum, v string, meta *parquet.SchemaElement) {
	if meta.LogicalType != nil && meta.LogicalType.DECIMAL != nil {
		v = binaryToDecimalStr([]byte(v), int(meta.LogicalType.DECIMAL.Scale))
	}
	if meta.Type != nil && *meta.Type == parquet.Type_INT96 && len(v) == 96/8 {
		ts := int96ToTime([]byte(v))
		ts = ts.UTC()
		v = ts.Format(utcTimeLayout)
	}
	d.SetString(v, "utf8mb4_bin")
}

func binaryToDecimalStr(rawBytes []byte, scale int) string {
	negative := rawBytes[0] > 127
	if negative {
		for i := range rawBytes {
			rawBytes[i] = ^rawBytes[i]
		}
		for i := len(rawBytes) - 1; i >= 0; i-- {
			rawBytes[i]++
			if rawBytes[i] != 0 {
				break
			}
		}
	}

	intValue := big.NewInt(0)
	intValue = intValue.SetBytes(rawBytes)
	val := fmt.Sprintf("%0*d", scale, intValue)
	dotIndex := len(val) - scale
	var res strings.Builder
	if negative {
		res.WriteByte('-')
	}
	if dotIndex == 0 {
		res.WriteByte('0')
	} else {
		res.WriteString(val[:dotIndex])
	}
	if scale > 0 {
		res.WriteByte('.')
		res.WriteString(val[dotIndex:])
	}
	return res.String()
}

// when the value type is int32/int64, convert to value to target logical type in tidb
func setDatumByInt(d *types.Datum, v int64, meta *parquet.SchemaElement) error {
	if meta.ConvertedType == nil && meta.LogicalType == nil {
		d.SetInt64(v)
		return nil
	}

	logicalType := meta.LogicalType
	switch {
	case logicalType.DECIMAL != nil:
		if logicalType.DECIMAL.Scale == 0 {
			d.SetInt64(v)
			return nil
		}
		minLen := logicalType.DECIMAL.Scale + 1
		if v < 0 {
			minLen++
		}
		val := fmt.Sprintf("%0*d", minLen, v)
		dotIndex := len(val) - int(*meta.Scale)
		d.SetString(val[:dotIndex]+"."+val[dotIndex:], "utf8mb4_bin")
	case logicalType.DATE != nil:
		dateStr := time.Unix(v*86400, 0).Format(time.DateOnly)
		d.SetString(dateStr, "utf8mb4_bin")
	case logicalType.TIMESTAMP != nil:
		// convert all timestamp types (datetime/timestamp) to string
		timeStr := formatTime(v, logicalType.TIMESTAMP.Unit, timeLayout,
			utcTimeLayout, logicalType.TIMESTAMP.IsAdjustedToUTC)
		d.SetString(timeStr, "utf8mb4_bin")
	case logicalType.TIME != nil:
		// convert all timestamp types (datetime/timestamp) to string
		timeStr := formatTime(v, logicalType.TIME.Unit, "15:04:05.999999", "15:04:05.999999Z",
			logicalType.TIME.IsAdjustedToUTC)
		d.SetString(timeStr, "utf8mb4_bin")
	default:
		d.SetInt64(v)
	}
	return nil
}

func formatTime(v int64, units *parquet.TimeUnit, format, utcFormat string, utc bool) string {
	var t time.Time
	if units.MICROS != nil {
		t = time.UnixMicro(v)
	} else if units.MILLIS != nil {
		t = time.UnixMilli(v)
	} else {
		// nano
		t = time.Unix(0, v)
	}
	t = t.UTC()
	if utc {
		return t.Format(utcFormat)
	}
	return t.Format(format)
}

// LastRow gets the last row parsed by the parser.
// It implements the Parser interface.
func (pp *ParquetParser) LastRow() Row {
	return pp.lastRow
}

// RecycleRow implements the Parser interface.
func (*ParquetParser) RecycleRow(_ Row) {
}

// Columns returns the _lower-case_ column names corresponding to values in
// the LastRow.
func (pp *ParquetParser) Columns() []string {
	return pp.columns
}

// SetColumns set restored column names to parser
func (*ParquetParser) SetColumns(_ []string) {
	// just do nothing
}

// SetLogger sets the logger used in the parser.
// It implements the Parser interface.
func (pp *ParquetParser) SetLogger(l log.Logger) {
	pp.logger = l
}

// SetRowID sets the rowID in a parquet file when we start a compressed file.
// It implements the Parser interface.
func (pp *ParquetParser) SetRowID(rowID int64) {
	pp.lastRow.RowID = rowID
}

func jdToTime(jd int32, nsec int64) time.Time {
	sec := int64(jd-jan011970) * secPerDay
	// it's fine not to check the value of nsec
	// because it's legall even though it exceeds the maximum.
	// See TestNsecOutSideRange.
	return time.Unix(sec, nsec)
}

<<<<<<< HEAD
// FYI: https://github.com/apache/spark/blob/d66a4e82eceb89a274edeb22c2fb4384bed5078b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetWriteSupport.scala#L171-L178
// INT96 timestamp layout
// --------------------------
// |   64 bit   |   32 bit   |
// ---------------------------
// |  nano sec  |  julian day  |
// ---------------------------
// NOTE: parquet date can be less than 1970-01-01 that is not supported by TiDB,
// where dt is a negative number but still legal in the context of Go.
// But it will cause errors or potential data inconsistency when importing.
func int96ToTime(parquetDate []byte) time.Time {
	nano := binary.LittleEndian.Uint64(parquetDate[:8])
	dt := binary.LittleEndian.Uint32(parquetDate[8:])
	return jdToTime(int32(dt), int64(nano))
=======
// NewParquetParser generates a parquet parser.
func NewParquetParser(
	ctx context.Context,
	store storeapi.Storage,
	r storeapi.ReadSeekCloser,
	path string,
	meta ParquetFileMeta,
) (*ParquetParser, error) {
	logger := log.Wrap(logutil.Logger(ctx))
	wrapper := &parquetWrapper{ReadSeekCloser: r}
	defer func() {
		_ = r.Close()
	}()

	allocator := meta.allocator
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}
	prop := parquet.NewReaderProperties(allocator)
	prop.BufferedStreamEnabled = true
	prop.BufferSize = 1024

	reader, err := file.NewParquetReader(wrapper, file.WithReadProps(prop))
	if err != nil {
		return nil, errors.Trace(err)
	}

	fileMeta := reader.MetaData()
	fileSchema := fileMeta.Schema
	colTypes := make([]convertedType, fileSchema.NumColumns())
	colNames := make([]string, 0, fileSchema.NumColumns())
	effectiveLoc := meta.Loc
	if effectiveLoc == nil {
		effectiveLoc = timeutil.SystemLocation()
	}

	for i := range colTypes {
		desc := fileSchema.Column(i)
		colNames = append(colNames, strings.ToLower(desc.Name()))

		logicalType := desc.LogicalType()
		if logicalType.IsValid() {
			colTypes[i].converted, colTypes[i].decimalMeta = logicalType.ToConvertedType()
			if t, ok := logicalType.(*schema.TimeLogicalType); ok {
				colTypes[i].IsAdjustedToUTC = t.IsAdjustedToUTC()
			} else {
				colTypes[i].IsAdjustedToUTC = true
			}
		} else {
			colTypes[i].converted = desc.ConvertedType()
			colTypes[i].IsAdjustedToUTC = true
			pnode, _ := desc.SchemaNode().(*schema.PrimitiveNode)
			colTypes[i].decimalMeta = pnode.DecimalMetadata()
		}

		if _, ok := unsupportedParquetTypes[colTypes[i].converted]; ok {
			return nil, errors.Errorf("unsupported parquet logical type %s", colTypes[i].converted.String())
		}

		switch desc.PhysicalType() {
		case parquet.Types.Int32:
			if colTypes[i].converted == schema.ConvertedTypes.Date {
				colTypes[i].sparkRebaseMicros, err = sparkRebaseMicrosFromMetadata(
					fileMeta, sparkDatetimeRebaseCutoff, sparkLegacyDateTimeMetadataKey, effectiveLoc)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		case parquet.Types.Int64:
			if colTypes[i].converted == schema.ConvertedTypes.TimestampMillis ||
				colTypes[i].converted == schema.ConvertedTypes.TimestampMicros {
				colTypes[i].sparkRebaseMicros, err = sparkRebaseMicrosFromMetadata(
					fileMeta, sparkDatetimeRebaseCutoff, sparkLegacyDateTimeMetadataKey, effectiveLoc)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		case parquet.Types.Int96:
			colTypes[i].sparkRebaseMicros, err = sparkRebaseMicrosFromMetadata(
				fileMeta, sparkINT96RebaseCutoff, sparkLegacyINT96MetadataKey, effectiveLoc)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	numColumns := len(colTypes)
	pool := zeropool.New(func() []types.Datum {
		return make([]types.Datum, numColumns)
	})

	parser := &ParquetParser{
		fileMeta: fileMeta,
		colTypes: colTypes,
		colNames: colNames,
		ctx:      ctx,
		store:    store,
		path:     path,
		prop:     prop,
		alloc:    allocator,
		logger:   logger,
		rowPool:  &pool,
	}
	if err := parser.Init(effectiveLoc); err != nil {
		return nil, errors.Trace(err)
	}

	return parser, nil
}

// SampleStatisticsFromParquet samples row size of the parquet file.
func SampleStatisticsFromParquet(
	ctx context.Context,
	path string,
	store storeapi.Storage,
) (
	rowCount int64,
	avgRowSize float64,
	err error,
) {
	r, err := store.Open(ctx, path, nil)
	if err != nil {
		return 0, 0, err
	}

	parser, err := NewParquetParser(ctx, store, r, path, ParquetFileMeta{})
	if err != nil {
		return 0, 0, err
	}

	//nolint: errcheck
	defer parser.Close()

	var rowSize int64

	meta := parser.fileMeta
	if meta.NumRowGroups() == 0 || meta.RowGroups[0].NumRows == 0 {
		return 0, 0, nil
	}

	totalReadRows := meta.NumRows
	readRows := min(totalReadRows, int64(1024))
	for range readRows {
		err = parser.ReadRow()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				break
			}
			return 0, 0, err
		}
		lastRow := parser.LastRow()
		rowSize += int64(lastRow.Length)
		parser.RecycleRow(lastRow)
		rowCount++
	}

	avgRowSize = float64(rowSize) / float64(rowCount)
	return totalReadRows, avgRowSize, err
}

// addressOf returns the address of a buffer, return 0 if the buffer is nil or
// empty. It's used to create unique identifiers for tracking buffer allocations.
func addressOf(buf []byte) uintptr {
	if buf == nil || cap(buf) == 0 {
		return 0
	}
	buf = buf[:1]
	return uintptr(unsafe.Pointer(&buf[0]))
}

// trackingAllocator is a simple memory allocator that tracks current and peak
// memory allocation. It's used to estimate the memory consumption of parquet
// parser.
type trackingAllocator struct {
	currentAllocation atomic.Int64
	peakAllocation    atomic.Int64
	allocMap          sync.Map // uintptr -> allocated bytes
}

const allocatorAlignment = 64

func roundUpToAlignment(addr uintptr) uintptr {
	return (addr + allocatorAlignment - 1) &^ (allocatorAlignment - 1)
}

func (a *trackingAllocator) allocateAligned(size int) []byte {
	if size <= 0 {
		return make([]byte, 0)
	}

	// Allocate extra bytes to align returned slice to 64 bytes.
	buf := make([]byte, size+allocatorAlignment)
	addr := addressOf(buf)
	next := roundUpToAlignment(addr)
	shift := int(next - addr)

	allocBytes := size + allocatorAlignment
	a.updateAllocation(int64(allocBytes))
	a.allocMap.Store(next, allocBytes)
	return buf[shift : shift+size : shift+size]
}

func (a *trackingAllocator) updateAllocation(delta int64) {
	current := a.currentAllocation.Add(delta)
	if delta <= 0 {
		return
	}

	for {
		oldPeak := a.peakAllocation.Load()
		if current <= oldPeak {
			return
		}
		if a.peakAllocation.CompareAndSwap(oldPeak, current) {
			return
		}
	}
}

func (a *trackingAllocator) Allocate(n int) []byte {
	return a.allocateAligned(n)
}

func (a *trackingAllocator) Free(b []byte) {
	addr := addressOf(b)
	if v, ok := a.allocMap.LoadAndDelete(addr); ok {
		bytes, _ := v.(int)
		a.currentAllocation.Add(-int64(bytes))
	}
}

func (a *trackingAllocator) Reallocate(size int, b []byte) []byte {
	if cap(b) >= size {
		return b[:size]
	}

	nb := a.allocateAligned(size)
	copy(nb, b)
	a.Free(b)
	return nb
}

func estimateInMemoryRowGroupBufferBytes(fileMeta *metadata.FileMetaData) (int64, error) {
	if fileMeta == nil || fileMeta.NumRowGroups() == 0 {
		return 0, nil
	}

	rgRange, err := rowGroupRangeFromMeta(fileMeta, 0)
	if err != nil {
		return 0, err
	}
	preloadBytes := rgRange.end - rgRange.start
	if preloadBytes <= 0 || preloadBytes > int64(rowGroupInMemoryThreshold) {
		return 0, nil
	}
	return preloadBytes, nil
}

// EstimateParquetReaderMemory estimates the peak memory usage for parsing a
// single parquet file by reading through the first row group with a tracking
// allocator. Returns the peak memory in bytes.
func EstimateParquetReaderMemory(
	ctx context.Context,
	store storeapi.Storage,
	path string,
) (int64, error) {
	r, err := store.Open(ctx, path, nil)
	if err != nil {
		return 0, err
	}

	allocator := &trackingAllocator{}
	parser, err := NewParquetParser(ctx, store, r, path, ParquetFileMeta{allocator: allocator})
	if err != nil {
		_ = r.Close()
		return 0, err
	}

	//nolint: errcheck
	defer parser.Close()

	meta := parser.fileMeta
	if meta.NumRowGroups() == 0 {
		return 0, nil
	}

	preloadBufferBytes, err := estimateInMemoryRowGroupBufferBytes(meta)
	if err != nil {
		return 0, err
	}

	for range meta.RowGroups[0].NumRows {
		if err = ctx.Err(); err != nil {
			return 0, err
		}
		if err = parser.ReadRow(); err != nil {
			if errors.Cause(err) == io.EOF {
				break
			}
			return 0, err
		}
		parser.RecycleRow(parser.LastRow())
	}

	peak := allocator.peakAllocation.Load() + preloadBufferBytes
	logutil.Logger(ctx).Info("estimated parquet reader memory",
		zap.String("path", path),
		zap.Int64("in-memory-preload-bytes", preloadBufferBytes),
		zap.Int64("peak-memory-bytes", peak),
	)
	return peak, nil
>>>>>>> 73b01d05c64 (import: honor Spark legacy Parquet datetime metadata (#67908))
}
