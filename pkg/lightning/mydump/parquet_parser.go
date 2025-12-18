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
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tidb/pkg/util/zeropool"
	"go.uber.org/zap"
)

const (
	// defaultBufSize specifies the default size of skip buffer.
	// Skip buffer is used when reading data from the cloud. If there is a gap between the current
	// read position and the last read position, these data is stored in this buffer to avoid
	// potentially reopening the underlying file when the gap size is less than the buffer size.
	defaultBufSize = 64 * 1024
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

type innerReader[T parquet.ColumnTypes] interface {
	Type() parquet.Type
	Descriptor() *schema.Column

	ReadBatchInPage(batchSize int64, values []T, defLvls, repLvls []int16) (int64, int, error)
	HasNext() bool

	Close() error
}

type columnDumper interface {
	Type() parquet.Type
	SetReader(colReader file.ColumnChunkReader)

	Next(*types.Datum) bool
	ReadNextBatch() int

	Close() error
}

type generalColumnDumper[T parquet.ColumnTypes, R innerReader[T]] struct {
	reader         R
	batchSize      int64
	valueOffset    int
	valuesBuffered int

	levelOffset    int64
	levelsBuffered int64
	defLevels      []int16
	repLevels      []int16
	values         []T

	closed bool

	setter setter[T]
}

// newGeneralColumnDumper creates a new generic column dumper
func newGeneralColumnDumper[T parquet.ColumnTypes, R innerReader[T]](
	batchSize int, getter setter[T],
) *generalColumnDumper[T, R] {
	return &generalColumnDumper[T, R]{
		batchSize: int64(batchSize),
		defLevels: make([]int16, batchSize),
		repLevels: make([]int16, batchSize),
		values:    make([]T, batchSize),
		setter:    getter,
	}
}

func (dump *generalColumnDumper[T, R]) SetReader(colReader file.ColumnChunkReader) {
	dump.reader, _ = colReader.(R)
}

func (dump *generalColumnDumper[T, R]) Type() parquet.Type {
	return dump.reader.Type()
}

func (dump *generalColumnDumper[T, R]) Close() error {
	if dump.closed {
		return nil
	}

	err := dump.reader.Close()
	dump.closed = true
	return err
}

func (dump *generalColumnDumper[T, R]) ReadNextBatch() int {
	// ReadBatchInPage reads a batch of values from the current page.
	// And the values returned may be shallow copies from the internal page buffer.
	//nolint: errcheck
	dump.levelsBuffered, dump.valuesBuffered, _ = dump.reader.ReadBatchInPage(
		dump.batchSize,
		dump.values,
		dump.defLevels,
		dump.repLevels,
	)

	dump.valueOffset = 0
	dump.levelOffset = 0
	return int(dump.levelsBuffered)
}

// Next reads the next value with proper level handling
func (dump *generalColumnDumper[T, R]) Next(d *types.Datum) bool {
	if dump.levelOffset == dump.levelsBuffered {
		if !dump.reader.HasNext() {
			return false
		}
		dump.ReadNextBatch()
		if dump.levelsBuffered == 0 {
			return false
		}
	}

	// Check definition level for NULL handling
	defLevel := dump.defLevels[dump.levelOffset]
	dump.levelOffset++

	if defLevel < dump.reader.Descriptor().MaxDefinitionLevel() {
		d.SetNull()
		return true
	}

	value := dump.values[dump.valueOffset]
	dump.valueOffset++
	dump.setter(value, d)
	return true
}

func createColumnDumper(tp parquet.Type, converted *convertedType, loc *time.Location, batchSize int) columnDumper {
	switch tp {
	case parquet.Types.Boolean:
		return newGeneralColumnDumper[bool, *file.BooleanColumnChunkReader](batchSize, getBoolData)
	case parquet.Types.Int32:
		return newGeneralColumnDumper[int32, *file.Int32ColumnChunkReader](batchSize, getInt32Getter(converted, loc))
	case parquet.Types.Int64:
		return newGeneralColumnDumper[int64, *file.Int64ColumnChunkReader](batchSize, getInt64Getter(converted, loc))
	case parquet.Types.Float:
		return newGeneralColumnDumper[float32, *file.Float32ColumnChunkReader](batchSize, getFloat32Data)
	case parquet.Types.Double:
		return newGeneralColumnDumper[float64, *file.Float64ColumnChunkReader](batchSize, getFloat64Data)
	case parquet.Types.Int96:
		return newGeneralColumnDumper[parquet.Int96, *file.Int96ColumnChunkReader](batchSize, getInt96Getter(converted, loc))
	case parquet.Types.ByteArray:
		return newGeneralColumnDumper[parquet.ByteArray, *file.ByteArrayColumnChunkReader](batchSize, getByteArrayGetter(converted))
	case parquet.Types.FixedLenByteArray:
		return newGeneralColumnDumper[parquet.FixedLenByteArray, *file.FixedLenByteArrayColumnChunkReader](batchSize, getFixedLenByteArrayGetter(converted))
	default:
		return nil
	}
}

// convertedType is older representation of the logical type in parquet
// ref: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
type convertedType struct {
	converted   schema.ConvertedType
	decimalMeta schema.DecimalMetadata
}

// parquetFileWrapper is a wrapper for storage.ReadSeekCloser
// It implements io.ReaderAt interface to read parquet file using arrow-go.
type parquetFileWrapper struct {
	ctx context.Context

	storage.ReadSeekCloser
	lastOff int64
	skipBuf []byte

	// current file path and store, used to open file
	store storage.ExternalStorage
	path  string
}

func (pf *parquetFileWrapper) readNBytes(p []byte) (int, error) {
	n, err := io.ReadFull(pf, p)
	if err != nil && err != io.EOF {
		return 0, errors.Trace(err)
	}
	if n != len(p) {
		return n, errors.Errorf("Error reading %d bytes, only read %d bytes", len(p), n)
	}
	return n, nil
}

// ReadAt implemement ReaderAt interface
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

	dumpers []columnDumper

	rowPool *zeropool.Pool[[]types.Datum]

	curRowGroup   int
	totalRowGroup int

	curRowInGroup    int   // number of rows read in current group
	totalRowsInGroup int   // total rows in current group
	totalRows        int   // total rows in this file
	totalRowsRead    int64 // total rows read
	totalBytesRead   int   // total bytes read, estimated by all the read datum.

	lastRow Row
	logger  log.Logger
}

// Init initializes the Parquet parser and allocate necessary buffers
func (pp *ParquetParser) Init(loc *time.Location) error {
	meta := pp.reader.MetaData()

	pp.curRowGroup, pp.totalRowGroup, pp.totalRows = -1, pp.reader.NumRowGroups(), int(meta.NumRows)

	numCols := meta.Schema.NumColumns()
	pp.dumpers = make([]columnDumper, numCols)

	if loc == nil {
		loc = timeutil.SystemLocation()
	}
	for i := range numCols {
		pp.dumpers[i] = createColumnDumper(meta.Schema.Column(i).PhysicalType(), &pp.colTypes[i], loc, 128)
	}

	return nil
}

// resetDumpers is used to reclaim the memory used by the column reader.
func (pp *ParquetParser) resetDumpers() error {
	var err error
	for _, d := range pp.dumpers {
		err2 := d.Close()
		if err2 != nil && err == nil {
			err = err2
		}
	}

	return err
}

// ReadRows read several rows internally and store them in the row buffer.
func (pp *ParquetParser) readSingleRows(row []types.Datum) error {
	// Move to next row group
	if pp.curRowInGroup == pp.totalRowsInGroup {
		if pp.curRowGroup >= 0 {
			if err := pp.resetDumpers(); err != nil {
				return err
			}
		}
		pp.curRowGroup++
		if pp.curRowGroup >= pp.totalRowGroup {
			return io.EOF
		}

		rowGroup := pp.reader.RowGroup(pp.curRowGroup)
		for c := range len(pp.dumpers) {
			colReader, err := rowGroup.Column(c)
			if err != nil {
				return errors.Trace(err)
			}
			pp.dumpers[c].SetReader(colReader)
		}
		pp.curRowInGroup, pp.totalRowsInGroup = 0, int(pp.reader.MetaData().RowGroups[pp.curRowGroup].NumRows)
	}

	// Read in this group
	for col, dumper := range pp.dumpers {
		if ok := dumper.Next(&row[col]); !ok {
			return errors.New("error get data")
		}
	}

	pp.totalBytesRead += estimateRowSize(row)
	pp.totalRowsRead++
	pp.curRowInGroup++
	return nil
}

// Pos returns the currently row number of the parquet file
func (pp *ParquetParser) Pos() (pos int64, rowID int64) {
	return int64(pp.totalRowsRead), pp.lastRow.RowID
}

// SetPos implements the Parser interface.
// For parquet file, this interface will read and discard the first `pos` rows,
// and set the current row ID to `rowID`
func (pp *ParquetParser) SetPos(pos int64, rowID int64) error {
	pp.lastRow.RowID = rowID

	row := pp.rowPool.Get()
	defer pp.rowPool.Put(row)

	// TODO(joechenrh): skip rows use underlying SkipRow interface
	// For now it's ok, since only UTs use this interface
	for range pos {
		if err := pp.readSingleRows(row); err != nil {
			return err
		}
	}

	return nil
}

// ScannedPos implements the Parser interface.
// For parquet we use the size of all read datum to estimate the scanned position.
func (pp *ParquetParser) ScannedPos() (int64, error) {
	return int64(pp.totalBytesRead), nil
}

// Close closes the parquet file of the parser.
// It implements the Parser interface.
func (pp *ParquetParser) Close() error {
	defer func() {
		if a, ok := pp.alloc.(interface{ Close() }); ok {
			a.Close()
		}
	}()

	if err := pp.resetDumpers(); err != nil {
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
	if err := pp.readSingleRows(row); err != nil {
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
	store storage.ExternalStorage,
	path string,
) (storage.ReadSeekCloser, error) {
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
	store storage.ExternalStorage,
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
	store storage.ExternalStorage,
	r storage.ReadSeekCloser,
	path string,
	meta ParquetFileMeta,
) (*ParquetParser, error) {
	logger := log.Wrap(logutil.Logger(ctx))
	logger.Info("Get memory usage of parquet reader",
		zap.String("file", path),
		zap.String("memory usage", fmt.Sprintf("%d MB", meta.MemoryUsage>>20)),
	)

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

	var allocator memory.Allocator
	allocator = memory.NewGoAllocator()
	if meta.MemoryPool != nil {
		allocator = NewParquetAllocator(meta.MemoryPool, meta.MemoryUsage)
	}

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
		} else {
			colTypes[i].converted = desc.ConvertedType()
			pnode, _ := desc.SchemaNode().(*schema.PrimitiveNode)
			colTypes[i].decimalMeta = pnode.DecimalMetadata()
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

// SampleStatisticsFromParquet samples row size and memory usage of the parquet file.
func SampleStatisticsFromParquet(
	ctx context.Context,
	fileMeta SourceFileMeta,
	store storage.ExternalStorage,
) (
	rowCount int64,
	avgRowSize float64,
	memoryUsage int,
	err error,
) {
	r, err := store.Open(ctx, fileMeta.Path, nil)
	if err != nil {
		return 0, 0, 0, err
	}

	meta := ParquetFileMeta{
		MemoryPool: GetPool(10 << 30), // use up to 4GiB memory for sampling
	}

	parser, err := NewParquetParser(ctx, store, r, fileMeta.Path, meta)
	if err != nil {
		return 0, 0, 0, err
	}

	//nolint: errcheck
	defer parser.Close()

	var rowSize int64

	reader := parser.reader
	if reader.NumRowGroups() == 0 || reader.MetaData().RowGroups[0].NumRows == 0 {
		return 0, 0, 0, nil
	}

	totalReadRows := reader.MetaData().RowGroups[0].NumRows
	for range totalReadRows {
		err = parser.ReadRow()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				break
			}
			return 0, 0, 0, err
		}
		lastRow := parser.LastRow()
		rowSize += int64(lastRow.Length)
		parser.RecycleRow(lastRow)
		rowCount++
	}

	avgRowSize = float64(rowSize) / float64(rowCount)

	alloc := parser.alloc
	a, _ := alloc.(*parquetAllocator)
	memoryUsage = a.Allocated()

	parser.logger.Info("Get memory usage of parquet reader",
		zap.String("memory usage", fmt.Sprintf("%d MB", memoryUsage>>20)),
	)

	return rowCount, avgRowSize, memoryUsage, err
}
