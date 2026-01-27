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
	"context"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tidb/pkg/util/zeropool"
	"go.uber.org/zap"
)

const (
	// defaultBufSize specifies the default size of skip buffer.
	// Skip buffer is used when reading data from the cloud.If there is a gap
	// between the current read position and the last read position, these
	// data is stored in this buffer to avoid potentially reopening the
	// underlying file when the gap size is less than the buffer size.
	defaultBufSize = 64 * 1024
)

var (
	unsupportedParquetTypes = map[schema.ConvertedType]struct{}{
		schema.ConvertedTypes.Map:         {},
		schema.ConvertedTypes.MapKeyValue: {},
		// TODO(joechenrh): support read list type as vector
		schema.ConvertedTypes.List:     {},
		schema.ConvertedTypes.Interval: {},
		schema.ConvertedTypes.NA:       {},
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
}

// parquetFileWrapper is a wrapper for storage.ReadSeekCloser
// It implements io.ReaderAt interface to read parquet file using arrow-go.
type parquetFileWrapper struct {
	ctx context.Context

	storeapi.ReadSeekCloser
	lastOff int64
	skipBuf []byte

	// current file path and store, used to open file
	store storeapi.Storage
	path  string
}

func (pf *parquetFileWrapper) readNBytes(p []byte) (int, error) {
	n, err := io.ReadFull(pf, p)
	if err != nil && err != io.EOF {
		return 0, errors.Trace(err)
	}
	if n != len(p) {
		return n, errors.Errorf("error reading %d bytes, only read %d bytes", len(p), n)
	}
	return n, nil
}

// ReadAt implement ReaderAt interface
func (pf *parquetFileWrapper) ReadAt(p []byte, off int64) (int, error) {
	// We want to minimize the number of Seek call as much as possible,
	// since the underlying reader may require reopening the file.
	gap := int(off - pf.lastOff)
	if gap < 0 || gap > cap(pf.skipBuf) {
		if _, err := pf.Seek(off, io.SeekStart); err != nil {
			return 0, err
		}
	} else {
		pf.skipBuf = pf.skipBuf[:gap]
		if read, err := pf.readNBytes(pf.skipBuf); err != nil {
			return read, err
		}
	}

	read, err := pf.readNBytes(p)
	if err != nil {
		return read, err
	}
	pf.lastOff = off + int64(read)

	return len(p), nil
}

// Seek implemement Seeker interface
func (pf *parquetFileWrapper) Seek(offset int64, whence int) (int64, error) {
	newOffset, err := pf.ReadSeekCloser.Seek(offset, whence)
	pf.lastOff = newOffset
	return newOffset, err
}

func (*parquetFileWrapper) Write(_ []byte) (n int, err error) {
	return 0, errors.New("unsupported operation")
}

func (pf *parquetFileWrapper) Open() (parquet.ReaderAtSeekerOpener, error) {
	reader, err := pf.store.Open(pf.ctx, pf.path, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newPf := &parquetFileWrapper{
		ReadSeekCloser: reader,
		store:          pf.store,
		ctx:            pf.ctx,
		path:           pf.path,
		skipBuf:        make([]byte, defaultBufSize),
	}
	return newPf, nil
}

// ParquetParser parses a parquet file for import
// It implements the Parser interface.
type ParquetParser struct {
	reader   *file.Reader
	colTypes []convertedType
	colNames []string

	alloc memory.Allocator

	iterators []iterator

	rowPool *zeropool.Pool[[]types.Datum]

	curRowGroup   int
	totalRowGroup int

	readRowInGroup   int   // number of rows read in current group
	totalRowsInGroup int   // total rows in current group
	totalRows        int   // total rows in this file
	totalReadRows    int64 // total rows read
	totalReadBytes   int   // total bytes read, estimated by all the read datum.

	lastRow Row
	logger  log.Logger
}

// Init initializes the Parquet parser and allocate necessary buffers
func (pp *ParquetParser) Init(loc *time.Location) error {
	meta := pp.reader.MetaData()

	pp.curRowGroup, pp.totalRowGroup, pp.totalRows = -1, pp.reader.NumRowGroups(), int(meta.NumRows)

	numCols := meta.Schema.NumColumns()
	pp.iterators = make([]iterator, numCols)

	if loc == nil {
		loc = timeutil.SystemLocation()
	}
	for i := range numCols {
		pp.iterators[i] = createColumnIterator(
			meta.Schema.Column(i).PhysicalType(), &pp.colTypes[i], loc, readBatchSize)
		if pp.iterators[i] == nil {
			return errors.Errorf("unsupported parquet type %s", meta.Schema.Column(i).PhysicalType().String())
		}
	}

	return nil
}

// resetIterators is used to reclaim the memory used by the column reader.
func (pp *ParquetParser) resetIterators() error {
	var err error
	for _, d := range pp.iterators {
		err2 := d.Close()
		if err2 != nil && err == nil {
			err = err2
		}
	}

	return err
}

// readSingleRow read one row internally and store them in the row buffer.
// The data read is shallow copied from the internal buffer of parquet reader,
// so copy it if you need to keep the data before the next read.
func (pp *ParquetParser) readSingleRow(row []types.Datum) error {
	// Move to next row group
	if pp.readRowInGroup == pp.totalRowsInGroup {
		if pp.curRowGroup >= 0 {
			if err := pp.resetIterators(); err != nil {
				return err
			}
		}
		pp.curRowGroup++
		if pp.curRowGroup >= pp.totalRowGroup {
			return io.EOF
		}

		rowGroup := pp.reader.RowGroup(pp.curRowGroup)
		for c := range len(pp.iterators) {
			colReader, err := rowGroup.Column(c)
			if err != nil {
				return errors.Trace(err)
			}
			pp.iterators[c].SetReader(colReader)
		}
		pp.readRowInGroup, pp.totalRowsInGroup = 0, int(pp.reader.MetaData().RowGroups[pp.curRowGroup].NumRows)
	}

	// Read in this group
	for col, iter := range pp.iterators {
		if err := iter.Next(&row[col]); err != nil {
			return errors.Annotate(err, "parquet read column failed")
		}
	}

	pp.totalReadBytes += estimateRowSize(row)
	pp.totalReadRows++
	pp.readRowInGroup++
	return nil
}

// Pos returns the currently row number of the parquet file
func (pp *ParquetParser) Pos() (pos int64, rowID int64) {
	return pp.totalReadRows, pp.lastRow.RowID
}

// SetPos implements the Parser interface.
// For parquet file, this interface will read and discard the first `pos` rows,
// and set the current row ID to `rowID`
func (pp *ParquetParser) SetPos(pos int64, rowID int64) error {
	row := pp.rowPool.Get()
	defer pp.rowPool.Put(row)

	// TODO(joechenrh): skip rows use underlying SkipRow interface
	// For now it's ok, since only UTs use this interface
	toRead := pos - pp.lastRow.RowID
	for range toRead {
		if err := pp.readSingleRow(row); err != nil {
			return err
		}
	}

	pp.lastRow.RowID = rowID
	return nil
}

// ScannedPos implements the Parser interface.
// For parquet we use the size of all read datum to estimate the scanned position.
func (pp *ParquetParser) ScannedPos() (int64, error) {
	return int64(pp.totalReadBytes), nil
}

// Close closes the parquet file of the parser.
// It implements the Parser interface.
func (pp *ParquetParser) Close() error {
	defer func() {
		if a, ok := pp.alloc.(interface{ Close() }); ok {
			a.Close()
		}
	}()

	if err := pp.resetIterators(); err != nil {
		pp.logger.Warn("Close parquet parser get error", zap.Error(err))
	}
	return pp.reader.Close()
}

// ReadRow reads a row in the parquet file by the parser.
// The read data is shallow copied from the internal buffer of parquet reader,
// so it's only valid before the next ReadRow call.
func (pp *ParquetParser) ReadRow() error {
	pp.lastRow.RowID++
	pp.lastRow.Length = 0

	row := pp.rowPool.Get()
	if err := pp.readSingleRow(row); err != nil {
		pp.rowPool.Put(row)
		return err
	}

	pp.lastRow.Row = row
	pp.lastRow.Length = estimateRowSize(row)
	return nil
}

// LastRow gets the last row parsed by the parser.
// It implements the Parser interface.
func (pp *ParquetParser) LastRow() Row {
	return pp.lastRow
}

// RecycleRow implements the Parser interface.
func (pp *ParquetParser) RecycleRow(row Row) {
	pp.rowPool.Put(row.Row)
}

// Columns returns the _lower-case_ column names corresponding to values in
// the LastRow.
func (pp *ParquetParser) Columns() []string {
	return pp.colNames
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

// OpenParquetReader opens a parquet file and returns a handle that can at least read the file.
func OpenParquetReader(
	ctx context.Context,
	store storeapi.Storage,
	path string,
) (storeapi.ReadSeekCloser, error) {
	r, err := store.Open(ctx, path, nil)
	if err != nil {
		return nil, err
	}

	pf := &parquetFileWrapper{
		ReadSeekCloser: r,
		store:          store,
		ctx:            ctx,
		path:           path,
		skipBuf:        make([]byte, defaultBufSize),
	}
	return pf, nil
}

// ReadParquetFileRowCountByFile reads the parquet file row count through fileMeta.
func ReadParquetFileRowCountByFile(
	ctx context.Context,
	store storeapi.Storage,
	fileMeta SourceFileMeta,
) (int64, error) {
	r, err := store.Open(ctx, fileMeta.Path, nil)
	if err != nil {
		return 0, errors.Trace(err)
	}

	reader, err := file.NewParquetReader(&parquetFileWrapper{ReadSeekCloser: r})
	if err != nil {
		return 0, errors.Trace(err)
	}

	return reader.MetaData().NumRows, nil
}

// NewParquetParser generates a parquet parser.
func NewParquetParser(
	ctx context.Context,
	store storeapi.Storage,
	r storeapi.ReadSeekCloser,
	path string,
	meta ParquetFileMeta,
) (*ParquetParser, error) {
	logger := log.Wrap(logutil.Logger(ctx))
	wrapper, ok := r.(*parquetFileWrapper)
	if !ok {
		wrapper = &parquetFileWrapper{
			ReadSeekCloser: r,
			store:          store,
			ctx:            ctx,
			path:           path,
			skipBuf:        make([]byte, defaultBufSize),
		}
	}

	allocator := memory.NewGoAllocator()
	prop := parquet.NewReaderProperties(allocator)
	prop.BufferedStreamEnabled = true
	prop.BufferSize = 1024

	reader, err := file.NewParquetReader(wrapper, file.WithReadProps(prop), file.WithPrefetch(8))
	if err != nil {
		return nil, errors.Trace(err)
	}

	fileSchema := reader.MetaData().Schema
	colTypes := make([]convertedType, fileSchema.NumColumns())
	colNames := make([]string, 0, fileSchema.NumColumns())

	for i := range colTypes {
		desc := reader.MetaData().Schema.Column(i)
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
	}

	numColumns := len(colTypes)
	pool := zeropool.New(func() []types.Datum {
		return make([]types.Datum, numColumns)
	})

	parser := &ParquetParser{
		reader:   reader,
		colTypes: colTypes,
		colNames: colNames,
		alloc:    allocator,
		logger:   logger,
		rowPool:  &pool,
	}
	if err := parser.Init(meta.Loc); err != nil {
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

	reader := parser.reader
	if reader.NumRowGroups() == 0 || reader.MetaData().RowGroups[0].NumRows == 0 {
		return 0, 0, nil
	}

	totalReadRows := reader.MetaData().NumRows
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
