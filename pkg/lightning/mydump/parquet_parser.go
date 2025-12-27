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
	goerrors "errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
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
	// ParquetParserMemoryLimit is the memory limit for the parquet parser
	// during precheck. It is used to validate that parsing a parquet file
	// does not consume excessive memory. If the peak memory usage exceeds this
	// limit, the precheck will fail to prevent potential OOM during the actual
	// import. This value is chosen as an experimental value since we don't know
	// the actual memory for the worker. Here we assume that worker nodes has
	// >= 2GiB/core, where half of the memory is allocated to the external writer.
	// Exported for test.
	ParquetParserMemoryLimit int64 = 512 << 20 // 512MB
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

// innerReader defines the interface for reading value with given type T from
// parquet column reader.
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
	it.setter(value, d)
	return nil
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

func (pf *parquetFileWrapper) Open() (*parquetFileWrapper, error) {
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
	readers  []*file.Reader
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
	meta := pp.readers[0].MetaData()

	pp.curRowGroup, pp.totalRowGroup, pp.totalRows = -1, pp.readers[0].NumRowGroups(), int(meta.NumRows)

	numCols := meta.Schema.NumColumns()
	pp.iterators = make([]iterator, numCols)

	if loc == nil {
		loc = timeutil.SystemLocation()
	}
	for i := range numCols {
		pp.iterators[i] = createColumnIterator(
			meta.Schema.Column(i).PhysicalType(), &pp.colTypes[i], loc, readBatchSize)
		if pp.iterators[i] == nil {
			return common.ErrParquetSchemaInvalid.FastGenByArgs(
				fmt.Sprintf("%s has invalid physical type", meta.Schema.Column(i).Name()))
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

		for c := range len(pp.iterators) {
			rowGroup := pp.readers[c].RowGroup(pp.curRowGroup)
			colReader, err := rowGroup.Column(c)
			if err != nil {
				return errors.Trace(err)
			}
			pp.iterators[c].SetReader(colReader)
		}
		pp.readRowInGroup, pp.totalRowsInGroup = 0, int(pp.readers[0].MetaData().RowGroups[pp.curRowGroup].NumRows)
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
	for _, r := range pp.readers {
		if err := r.Close(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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

	allocator := meta.allocator
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}
	prop := parquet.NewReaderProperties(allocator)
	prop.BufferedStreamEnabled = true
	prop.BufferSize = 1024

	reader, err := file.NewParquetReader(wrapper, file.WithReadProps(prop))
	if err != nil {
		_ = r.Close()
		return nil, errors.Trace(err)
	}

	fileSchema := reader.MetaData().Schema
	colTypes := make([]convertedType, fileSchema.NumColumns())
	colNames := make([]string, 0, fileSchema.NumColumns())
	subreaders := make([]*file.Reader, 0, fileSchema.NumColumns())
	subreaders = append(subreaders, reader)

	defer func() {
		if err != nil {
			for _, subreader := range subreaders {
				_ = subreader.Close()
			}
		}
	}()

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
			return nil, common.ErrParquetSchemaInvalid.FastGenByArgs(
				fmt.Sprintf("%s is not supported", colTypes[i].converted.String()))
		}
	}

	numColumns := len(colTypes)
	pool := zeropool.New(func() []types.Datum {
		return make([]types.Datum, numColumns)
	})

	for i := 1; i < fileSchema.NumColumns(); i++ {
		var newWrapper *parquetFileWrapper
		// Open file for each column.
		newWrapper, err = wrapper.Open()
		if err != nil {
			return nil, errors.Trace(err)
		}

		reader, err := file.NewParquetReader(newWrapper,
			file.WithReadProps(prop),
			file.WithMetadata(reader.MetaData()),
		)
		if err != nil {
			_ = newWrapper.Close()
			return nil, errors.Trace(err)
		}
		subreaders = append(subreaders, reader)
	}

	parser := &ParquetParser{
		readers:  subreaders,
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

// ParquetPrecheckResult is the result of parquet import check.
type ParquetPrecheckResult struct {
	AvgRowSize         float64
	SizeExpansionRatio float64
}

// PrecheckParquet checks whether the import file is valid to import.
func PrecheckParquet(
	ctx context.Context,
	store storage.ExternalStorage,
	path string,
) (*ParquetPrecheckResult, error) {
	failpoint.Inject("skipCheckForParquet", func() {
		failpoint.Return(&ParquetPrecheckResult{1.0, 1.0}, nil)
	})

	r, err := store.Open(ctx, path, nil)
	if err != nil {
		return nil, err
	}

	allocator := &trackingAllocator{}
	parser, err := NewParquetParser(ctx, store, r, path, ParquetFileMeta{allocator: allocator})

	if err != nil {
		if goerrors.Is(err, common.ErrParquetSchemaInvalid) {
			return nil, exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(
				"parquet file schema invalid")
		}
		return nil, err
	}

	//nolint: errcheck
	defer parser.Close()

	reader := parser.readers[0]

	// The file is empty, use 1.0 and 2.0 as row size and ratio respectively.
	if len(reader.MetaData().RowGroups) == 0 ||
		reader.MetaData().RowGroups[0].NumRows == 0 {
		return &ParquetPrecheckResult{1.0, 2.0}, nil
	}

	var (
		rowSize  int64
		rowCount int64
	)
	for range reader.MetaData().RowGroups[0].NumRows {
		err = parser.ReadRow()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				break
			}
			return nil, err
		}
		rowCount++
		lastRow := parser.LastRow()
		rowSize += int64(lastRow.Length)
		parser.RecycleRow(lastRow)
		if allocator.peakAllocation.Load() >= ParquetParserMemoryLimit {
			return nil, exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(
				"parquet files are too large and may cause OOM, consider changing to CSV format")
		}
	}

	fileSize := reader.MetaData().GetSourceFileSize()
	avgRowSize := float64(rowSize) / float64(rowCount)
	sizeExpansionRatio := (float64)(rowSize) / float64(fileSize)

	return &ParquetPrecheckResult{avgRowSize, sizeExpansionRatio}, nil
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
// parser during precheck.
type trackingAllocator struct {
	currentAllocation atomic.Int64
	peakAllocation    atomic.Int64
	allocMap          sync.Map // uintptr -> size
}

func (a *trackingAllocator) Allocate(n int) []byte {
	b := make([]byte, n)
	a.allocMap.Store(addressOf(b), n)

	current := a.currentAllocation.Add(int64(n))
	for {
		oldPeak := a.peakAllocation.Load()
		if current <= oldPeak {
			break
		}
		if a.peakAllocation.CompareAndSwap(oldPeak, current) {
			break
		}
	}
	return b
}

func (a *trackingAllocator) Free(b []byte) {
	addr := addressOf(b)
	v, ok := a.allocMap.Load(addr)
	size, _ := v.(int)
	if ok {
		a.currentAllocation.Add(-int64(size))
		a.allocMap.Delete(addr)
	}
}

func (a *trackingAllocator) Reallocate(size int, b []byte) []byte {
	a.Free(b)
	return a.Allocate(size)
}
