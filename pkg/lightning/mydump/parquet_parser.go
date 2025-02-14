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
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/joechenrh/arrow-go/v18/arrow/memory"
	"github.com/joechenrh/arrow-go/v18/parquet"
	"github.com/joechenrh/arrow-go/v18/parquet/file"
	"github.com/joechenrh/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

const (
	defaultBatchSize = 128

	// if a parquet if small than this threshold, parquet will load the whole file in a byte slice to
	// optimize the read performance
	smallParquetFileThreshold = 256 * 1024 * 1024
	defaultBufSize            = 64 * 1024

	utcTimeLayout = "2006-01-02 15:04:05.999999Z"
	timeLayout    = "2006-01-02 15:04:05.999999"
)

type allocatorWithStats struct {
	baseAllocator memory.Allocator
	allocated     atomic.Int64
}

func (a *allocatorWithStats) Allocate(size int) []byte {
	b := a.baseAllocator.Allocate(size)
	a.allocated.Add(int64(cap(b)))
	return b
}

func (a *allocatorWithStats) Reallocate(size int, b []byte) []byte {
	return a.baseAllocator.Reallocate(size, b)
}

func (a *allocatorWithStats) Free(b []byte) {
	a.baseAllocator.Free(b)
}

func (a *allocatorWithStats) Allocated() int64 {
	return a.allocated.Load()
}

type columnDumper struct {
	reader         file.ColumnChunkReader
	batchSize      int64
	valueOffset    int
	valuesBuffered int

	levelOffset    int64
	levelsBuffered int64
	defLevels      []int16
	repLevels      []int16
	values         []interface{}

	valueBuffer any
}

func createcolumnDumper(tp parquet.Type) *columnDumper {
	batchSize := 128

	var valueBuffer any
	switch tp {
	case parquet.Types.Boolean:
		valueBuffer = make([]bool, batchSize)
	case parquet.Types.Int32:
		valueBuffer = make([]int32, batchSize)
	case parquet.Types.Int64:
		valueBuffer = make([]int64, batchSize)
	case parquet.Types.Float:
		valueBuffer = make([]float32, batchSize)
	case parquet.Types.Double:
		valueBuffer = make([]float64, batchSize)
	case parquet.Types.Int96:
		valueBuffer = make([]parquet.Int96, batchSize)
	case parquet.Types.ByteArray:
		valueBuffer = make([]parquet.ByteArray, batchSize)
	case parquet.Types.FixedLenByteArray:
		valueBuffer = make([]parquet.FixedLenByteArray, batchSize)
	}

	return &columnDumper{
		batchSize:   int64(batchSize),
		defLevels:   make([]int16, batchSize),
		repLevels:   make([]int16, batchSize),
		values:      make([]interface{}, batchSize),
		valueBuffer: valueBuffer,
	}
}

// Type returns the column type of this dumper
func (dump *columnDumper) Type() parquet.Type {
	return dump.reader.Type()
}

// SetReader sets the reader
func (dump *columnDumper) SetReader(colReader file.ColumnChunkReader) {
	dump.reader = colReader
	dump.valueOffset = 0
	dump.levelOffset = 0
	dump.levelsBuffered = 0
	dump.valuesBuffered = 0
}

func (dump *columnDumper) readNextBatch() int {
	switch reader := dump.reader.(type) {
	case *file.BooleanColumnChunkReader:
		values, _ := dump.valueBuffer.([]bool)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int32ColumnChunkReader:
		values, _ := dump.valueBuffer.([]int32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int64ColumnChunkReader:
		values, _ := dump.valueBuffer.([]int64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float32ColumnChunkReader:
		values, _ := dump.valueBuffer.([]float32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float64ColumnChunkReader:
		values, _ := dump.valueBuffer.([]float64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int96ColumnChunkReader:
		values, _ := dump.valueBuffer.([]parquet.Int96)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.ByteArrayColumnChunkReader:
		values, _ := dump.valueBuffer.([]parquet.ByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.FixedLenByteArrayColumnChunkReader:
		values, _ := dump.valueBuffer.([]parquet.FixedLenByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	}

	dump.valueOffset = 0
	dump.levelOffset = 0
	return int(dump.levelsBuffered)
}

func (dump *columnDumper) hasNext() bool {
	return dump.levelOffset < dump.levelsBuffered || dump.reader.HasNext()
}

// Next reads next value from the reader
func (dump *columnDumper) Next() (interface{}, bool) {
	if dump.levelOffset == dump.levelsBuffered {
		if !dump.hasNext() {
			return nil, false
		}
		dump.readNextBatch()
		if dump.levelsBuffered == 0 {
			return nil, false
		}
	}

	defLevel := dump.defLevels[int(dump.levelOffset)]
	// repLevel := dump.repLevels[int(dump.levelOffset)]
	dump.levelOffset++

	if defLevel < dump.reader.Descriptor().MaxDefinitionLevel() {
		return nil, true
	}

	vb := reflect.ValueOf(dump.valueBuffer)
	v := vb.Index(dump.valueOffset).Interface()
	dump.valueOffset++

	return v, true
}

// convertedType is older representation of the logical type in parquet
// ref: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
type convertedType struct {
	converted   schema.ConvertedType
	decimalMeta schema.DecimalMetadata
}

func binaryToDecimalStr(rawBytes []byte, scale int) string {
	negative := rawBytes[0] > 127
	if negative {
		for i := 0; i < len(rawBytes); i++ {
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

func formatTime(v int64, unit string, format, utcFormat string, utc bool) string {
	var t time.Time
	switch unit {
	case "MICROS":
		t = time.UnixMicro(v)
	case "MILLIS":
		t = time.UnixMilli(v)
	default:
		t = time.Unix(0, v)
	}

	t = t.UTC()
	if utc {
		return t.Format(utcFormat)
	}
	return t.Format(format)
}

// bytesReaderWrapper is a wrapper of bytes.Reader.
type bytesReaderWrapper struct {
	*bytes.Reader
	rawBytes []byte
	// current file path
	path string
}

func (*bytesReaderWrapper) Close() error {
	return nil
}

func (*bytesReaderWrapper) Write(_ []byte) (n int, err error) {
	return 0, errors.New("unsupported operation")
}

func (r *bytesReaderWrapper) Open(name string) (parquet.ReaderAtSeeker, error) {
	if len(name) > 0 && name != r.path {
		panic(fmt.Sprintf("Open with a different name is not supported! current: '%s', new: '%s'", r.path, name))
	}
	return &bytesReaderWrapper{
		Reader:   bytes.NewReader(r.rawBytes),
		rawBytes: r.rawBytes,
		path:     r.path,
	}, nil
}

// parquetFileWrapper is a wrapper for storage.ReadSeekCloser
// It implements io.ReaderAt interface to read parquet file using arrow-go.
type parquetFileWrapper struct {
	ctx context.Context

	storage.ReadSeekCloser
	lastOff int64
	bufSize int
	buf     []byte

	// current file path and store, used to open file
	store storage.ExternalStorage
	path  string
}

func (pf *parquetFileWrapper) InitBuffer(bufSize int) {
	pf.bufSize = bufSize
	pf.buf = make([]byte, bufSize)
}

func (pf *parquetFileWrapper) readNBytes(p []byte) (int, error) {
	read := 0
	for read < len(p) {
		n, err := pf.Read(p[read:])
		read += n
		if err != nil {
			return read, err
		}
	}
	if read != len(p) {
		return read, errors.Errorf("Error reading %d bytes, only read %d bytes", len(p), read)
	}
	return read, nil
}

// ReadAt implemement ReaderAt interface
func (pf *parquetFileWrapper) ReadAt(p []byte, off int64) (int, error) {
	// We want to minimize the number of Seek call as much as possible,
	// since the underlying reader may require reopening the file.
	gap := int(off - pf.lastOff)
	if gap < 0 || gap > pf.bufSize {
		if _, err := pf.Seek(off, io.SeekStart); err != nil {
			return 0, err
		}
	} else {
		pf.buf = pf.buf[:gap]
		if read, err := pf.readNBytes(pf.buf); err != nil {
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

func (pf *parquetFileWrapper) Open(name string) (parquet.ReaderAtSeeker, error) {
	if len(name) == 0 {
		name = pf.path
	}
	reader, err := pf.store.Open(pf.ctx, name, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newPf := &parquetFileWrapper{
		ReadSeekCloser: reader,
		store:          pf.store,
		ctx:            pf.ctx,
		path:           name,
	}
	newPf.InitBuffer(defaultBufSize)
	return newPf, nil
}

// ParquetParser parses a parquet file for import
// It implements the Parser interface.
type ParquetParser struct {
	readers     []*file.Reader
	colMetas    []convertedType
	columnNames []string

	alloc memory.Allocator

	// colBuffers is used to store raw data read from parquet columns.
	// rows stores the actual data after parsing.
	dumpers []*columnDumper
	rows    [][]types.Datum

	// curIdx and avail is the current index and total number of rows in rows buffer
	curIdx int
	avail  int

	curRowGroup   int
	totalRowGroup int

	curRowInGroup    int // number of rows read in current group
	totalRowsInGroup int // total rows in current group
	curRows          int // number of rows read in total
	totalRows        int // total rows in this file

	lastRow Row
	logger  log.Logger

	memoryUsage int
	memLimiter  *membuf.Limiter
}

// GetMemoryUsage estimate the memory usage for this file.
func (pp *ParquetParser) GetMemoryUsage() (memoryUsageStream, memoryUsageNonStream int) {
	// Initialize column reader
	if pp.dumpers[0].reader == nil {
		if err := pp.ReadRow(); err != nil {
			return math.MaxInt, math.MaxInt
		}
	}

	// All the columns share the same data page size,
	// so we only need to read one column chunk.
	dumper := pp.dumpers[0]
	for {
		if _, ok := dumper.Next(); !ok {
			break
		}
	}

	alloc, ok := pp.alloc.(*sampleAllocator)
	if !ok {
		return 0, 0
	}
	bufSizes := alloc.allocated

	/*
	 * We have collected all the allocations, and the allocation order are:
	 * read buffer(repeat n times), decompressed dict buffer, compressed buffer, decompressed data page buffer, compressed data page buffer, ...
	 * since the compressed buffer is released after decompression, we estimate the memory usage as:
	 * (AllocSize(decompressed dict buffer) + AllocSize(decompressed data page buffer) + AllocSize(read buffer) + AllocSize(parquet read buffer)) * num_cols
	 */

	numColumns := len(pp.columnNames)
	dictUsage := 0
	dataPageUsage := 0
	readBufferUsageStream := (AllocSize(bufSizes[0]) + AllocSize(defaultBufSize)) * numColumns

	readBufferUsageNonStream := 0
	meta := pp.readers[0].MetaData()
	for _, rg := range meta.RowGroups {
		currUsage := 0
		for _, c := range rg.Columns {
			currUsage += AllocSize(int(c.MetaData.GetTotalCompressedSize()))
		}
		readBufferUsageNonStream = max(readBufferUsageNonStream, currUsage)
	}
	readBufferUsageNonStream += AllocSize(defaultBufSize) * len(pp.columnNames)

	hasDict := true
	if 5*numColumns > len(bufSizes) {
		hasDict = false
	}

	if hasDict {
		for i := numColumns; i < 5*numColumns; i += 4 {
			dictUsage = max(dictUsage, AllocSize(bufSizes[i]))
			dataPageUsage = max(dataPageUsage, AllocSize(bufSizes[i+2]))
		}
		for i := 5 * numColumns; i < len(bufSizes); i += 2 {
			dataPageUsage = max(dataPageUsage, AllocSize(bufSizes[i]))
		}
	} else {
		for i := numColumns; i < len(bufSizes); i += 2 {
			dataPageUsage = max(dataPageUsage, AllocSize(bufSizes[i]))
		}
	}

	pageUsage := (dataPageUsage + dictUsage) * numColumns

	return roundUp(pageUsage+readBufferUsageStream, defaultArenaSize),
		roundUp(pageUsage+readBufferUsageNonStream, defaultArenaSize)
}

func (pp *ParquetParser) setStringData(row, col int, val interface{}) {
	vba, _ := val.(parquet.ByteArray)
	pp.rows[row][col].SetString(string(vba), "utf8mb4_bin")
}

func (pp *ParquetParser) setInt32Data(row, col int, val interface{}) {
	v32, _ := val.(int32)
	pp.rows[row][col].SetInt64(int64(v32))
}

func (pp *ParquetParser) setUint32Data(row, col int, val interface{}) {
	v64, _ := val.(int64)
	pp.rows[row][col].SetUint64(uint64(v64))
}

func (pp *ParquetParser) setInt64Data(row, col int, val interface{}) {
	v64, _ := val.(int64)
	pp.rows[row][col].SetInt64(v64)
}

func (pp *ParquetParser) setUint64Data(row, col int, val interface{}) {
	v64, _ := val.(int64)
	pp.rows[row][col].SetUint64(uint64(v64))
}

func (pp *ParquetParser) setTimeMillisData(row, col int, val interface{}) {
	v32, _ := val.(int32)
	timeStr := formatTime(int64(v32), "MILLIS", "15:04:05.999999", "15:04:05.999999Z", true)
	pp.rows[row][col].SetString(timeStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setTimeMicrosData(row, col int, val interface{}) {
	v64, _ := val.(int64)
	timeStr := formatTime(v64, "MICROS", "15:04:05.999999", "15:04:05.999999Z", true)
	pp.rows[row][col].SetString(timeStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setTimestampMillisData(row, col int, val interface{}) {
	v64, _ := val.(int64)
	timeStr := formatTime(v64, "MILLIS", timeLayout, utcTimeLayout, true)
	pp.rows[row][col].SetString(timeStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setTimestampMicrosData(row, col int, val interface{}) {
	v64, _ := val.(int64)
	timeStr := formatTime(v64, "MICROS", timeLayout, utcTimeLayout, true)
	pp.rows[row][col].SetString(timeStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setDateData(row, col int, val interface{}) {
	v32, _ := val.(int32)
	dateStr := time.Unix(int64(v32)*86400, 0).Format(time.DateOnly)
	pp.rows[row][col].SetString(dateStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setDecimalData(row, col int, val interface{}) {
	colTp := pp.dumpers[col].Type()
	decimal := pp.colMetas[col].decimalMeta

	if colTp == parquet.Types.Int64 || colTp == parquet.Types.Int32 {
		var v int64
		if colTp == parquet.Types.Int32 {
			v32, _ := val.(int32)
			v = int64(v32)
		} else {
			v, _ = val.(int64)
		}
		if !decimal.IsSet || decimal.Scale == 0 {
			pp.rows[row][col].SetInt64(v)
			return
		}
		minLen := decimal.Scale + 1
		if v < 0 {
			minLen++
		}
		val := fmt.Sprintf("%0*d", minLen, v)
		dotIndex := len(val) - int(decimal.Scale)
		pp.rows[row][col].SetString(val[:dotIndex]+"."+val[dotIndex:], "utf8mb4_bin")
	} else if colTp == parquet.Types.FixedLenByteArray {
		v, _ := val.(parquet.FixedLenByteArray)
		s := binaryToDecimalStr(v, int(decimal.Scale))
		pp.rows[row][col].SetString(s, "utf8mb4_bin")
	} else {
		v, _ := val.(parquet.ByteArray)
		s := binaryToDecimalStr(v, int(decimal.Scale))
		pp.rows[row][col].SetString(s, "utf8mb4_bin")
	}
}

func (pp *ParquetParser) setBoolData(row, col int, val interface{}) {
	boolVal, _ := val.(bool)
	if boolVal {
		pp.rows[row][col].SetUint64(1)
		return
	}
	pp.rows[row][col].SetUint64(0)
}

func (pp *ParquetParser) setFloat32Data(row, col int, val interface{}) {
	vf32, _ := val.(float32)
	pp.rows[row][col].SetFloat32(vf32)
}

func (pp *ParquetParser) setFloat64Data(row, col int, val interface{}) {
	vf64, _ := val.(float64)
	pp.rows[row][col].SetFloat64(vf64)
}

func (pp *ParquetParser) setFixedByteArrayData(row, col int, val interface{}) {
	vfa, _ := val.(parquet.FixedLenByteArray)
	pp.rows[row][col].SetString(string(vfa), "utf8mb4_bin")
}

func (pp *ParquetParser) setByteArrayData(row, col int, val interface{}) {
	vba, _ := val.(parquet.ByteArray)
	pp.rows[row][col].SetString(string(vba), "utf8mb4_bin")
}

func (pp *ParquetParser) setInt96Data(row, col int, val interface{}) {
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
	v96, _ := val.(parquet.Int96)
	pp.rows[row][col].SetString(v96.ToTime().Format(utcTimeLayout), "utf8mb4_bin")
}

// Init initializes the Parquet parser and allocate necessary buffers
func (pp *ParquetParser) Init() error {
	meta := pp.readers[0].MetaData()

	pp.curRowGroup, pp.totalRowGroup = -1, pp.readers[0].NumRowGroups()

	pp.totalRows = int(meta.NumRows)

	numCols := meta.Schema.NumColumns()
	pp.rows = make([][]types.Datum, defaultBatchSize)
	for i := range pp.rows {
		pp.rows[i] = make([]types.Datum, numCols)
	}

	pp.dumpers = make([]*columnDumper, numCols)
	for i := 0; i < numCols; i++ {
		pp.dumpers[i] = createcolumnDumper(meta.Schema.Column(i).PhysicalType())
	}

	return nil
}

// resetReader is used to reclaim the memory used by the column reader.
func (pp *ParquetParser) resetReader() {
	for _, d := range pp.dumpers {
		if d.reader != nil {
			d.reader.Reset()
		}
	}
}

// ReadRows read several rows internally and store them in the row buffer.
func (pp *ParquetParser) ReadRows(num int) (int, error) {
	if num > defaultBatchSize {
		return 0, errors.Errorf("Number of rows read larger than buffer size")
	}

	readNum := min(num, pp.totalRows-pp.curRows)
	if readNum == 0 {
		return 0, nil
	}

	read := 0
	for read < readNum {
		// Move to next row group
		if pp.curRowInGroup == pp.totalRowsInGroup {
			if pp.curRowGroup >= 0 {
				pp.resetReader()
			}
			pp.curRowGroup++
			for c := 0; c < len(pp.dumpers); c++ {
				rowGroupReader := pp.readers[c].RowGroup(pp.curRowGroup)
				colReader, err := rowGroupReader.Column(c)
				if err != nil {
					return 0, errors.Trace(err)
				}
				pp.dumpers[c].SetReader(colReader)
			}
			pp.curRowInGroup, pp.totalRowsInGroup = 0, int(pp.readers[0].MetaData().RowGroups[pp.curRowGroup].NumRows)
		}

		// Read in this group
		curRead := min(readNum-read, pp.totalRowsInGroup-pp.curRowInGroup)
		_, err := pp.readInGroup(curRead, read)
		if err != nil {
			return 0, errors.Trace(err)
		}
		read += curRead
		pp.curRowInGroup += curRead
	}

	pp.curRows += readNum
	pp.curIdx, pp.avail = 0, readNum
	return readNum, nil
}

// readInGroup read severals rows in current row group.
// storeOffset represents the starting position for storing the read rows.
// It's a part of the ReadRows.
func (pp *ParquetParser) readInGroup(num, storeOffset int) (int, error) {
	var (
		err   error
		total int
	)

	// Read data into buffers first
	for col, dumper := range pp.dumpers {
		meta := pp.colMetas[col]
		physicalTp := dumper.Type()

		var setFunc func(row, col int, val interface{})
		if physicalTp == parquet.Types.Boolean || physicalTp == parquet.Types.Int96 || meta.converted == schema.ConvertedTypes.None {
			switch physicalTp {
			case parquet.Types.Boolean:
				setFunc = pp.setBoolData
			case parquet.Types.Int32:
				setFunc = pp.setInt32Data
			case parquet.Types.Int64:
				setFunc = pp.setInt64Data
			case parquet.Types.Int96:
				setFunc = pp.setInt96Data
			case parquet.Types.Float:
				setFunc = pp.setFloat32Data
			case parquet.Types.Double:
				setFunc = pp.setFloat64Data
			case parquet.Types.ByteArray:
				setFunc = pp.setByteArrayData
			case parquet.Types.FixedLenByteArray:
				setFunc = pp.setFixedByteArrayData
			}
		} else {
			switch meta.converted {
			case schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
				setFunc = pp.setStringData
			case schema.ConvertedTypes.Int8, schema.ConvertedTypes.Int16, schema.ConvertedTypes.Int32:
				setFunc = pp.setInt32Data
			case schema.ConvertedTypes.Uint8, schema.ConvertedTypes.Uint16, schema.ConvertedTypes.Uint32:
				setFunc = pp.setUint32Data
			case schema.ConvertedTypes.Int64:
				setFunc = pp.setInt64Data
			case schema.ConvertedTypes.Uint64:
				setFunc = pp.setUint64Data
			case schema.ConvertedTypes.TimeMillis:
				setFunc = pp.setTimeMillisData
			case schema.ConvertedTypes.TimeMicros:
				setFunc = pp.setTimeMicrosData
			case schema.ConvertedTypes.TimestampMillis:
				setFunc = pp.setTimestampMillisData
			case schema.ConvertedTypes.TimestampMicros:
				setFunc = pp.setTimestampMicrosData
			case schema.ConvertedTypes.Date:
				setFunc = pp.setDateData
			case schema.ConvertedTypes.Decimal:
				setFunc = pp.setDecimalData
			}
		}

		for i := 0; i < num; i++ {
			val, ok := dumper.Next()
			if !ok {
				break
			}

			if val == nil {
				pp.rows[storeOffset+i][col].SetNull()
				continue
			}
			setFunc(storeOffset+i, col, val)
		}
	}

	return total, err
}

// Pos returns the currently row number of the parquet file
func (pp *ParquetParser) Pos() (pos int64, rowID int64) {
	return int64(pp.curRows - pp.avail + pp.curIdx), pp.lastRow.RowID
}

// SetPos implements the Parser interface.
// For parquet file, this interface will read and discard the first `pos` rows,
// and set the current row ID to `rowID`
func (pp *ParquetParser) SetPos(pos int64, rowID int64) error {
	curPos, _ := pp.Pos()
	if pos < curPos {
		return errors.Errorf("Parquet parset doesn't support seek back yet")
	}

	// Read and discard these rows
	pos = min(pos, int64(pp.totalRows))
	for !(int(pos) >= pp.curRows-pp.avail && int(pos) < pp.curRows) {
		numRead, err := pp.ReadRows(defaultBatchSize)
		if err != nil {
			return errors.Trace(err)
		}
		if numRead == 0 {
			break
		}
	}

	pp.curIdx = int(pos) - (pp.curRows - pp.avail)
	pp.lastRow.RowID = rowID
	return nil
}

// ScannedPos implements the Parser interface.
// For parquet it's nonsense to get the position of internal reader,
// thus it will return the number of rows read.
func (pp *ParquetParser) ScannedPos() (int64, error) {
	return int64(pp.curRows), nil
}

// Close closes the parquet file of the parser.
// It implements the Parser interface.
func (pp *ParquetParser) Close() error {
	defer func() {
		if a, ok := pp.alloc.(interface{ Close() }); ok {
			a.Close()
		}

		if pp.memLimiter != nil {
			pp.memLimiter.Release(pp.memoryUsage)
		}
	}()

	pp.resetReader()
	for _, r := range pp.readers {
		if err := r.Close(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// GetRow get the the current row.
// Return error if can't read next row.
// User should call ReadRow before calling this.
func (pp *ParquetParser) GetRow() ([]types.Datum, error) {
	if pp.curIdx >= pp.avail {
		read, err := pp.ReadRows(defaultBatchSize)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if read == 0 {
			return nil, nil
		}
	}

	row := pp.rows[pp.curIdx]
	pp.curIdx++
	return row, nil
}

// ReadRow reads a row in the parquet file by the parser.
// It implements the Parser interface.
// Return io.EOF if reaching the end of the file.
func (pp *ParquetParser) ReadRow() error {
	pp.lastRow.RowID++
	pp.lastRow.Length = 0
	row, err := pp.GetRow()
	if err != nil {
		return errors.Trace(err)
	}
	if row == nil {
		return io.EOF
	}
	pp.lastRow.Row = row
	pp.lastRow.Length = 0
	return nil
}

// LastRow gets the last row parsed by the parser.
// It implements the Parser interface.
func (pp *ParquetParser) LastRow() Row {
	pp.lastRow.Length = 0
	for _, v := range pp.lastRow.Row {
		if v.IsNull() {
			continue
		}
		if v.Kind() == types.KindString {
			// use GetBytes to avoid memory allocation
			pp.lastRow.Length += len(v.GetBytes())
		} else {
			pp.lastRow.Length += 8
		}
	}
	return pp.lastRow
}

// RecycleRow implements the Parser interface.
func (*ParquetParser) RecycleRow(_ Row) {
}

// Columns returns the _lower-case_ column names corresponding to values in
// the LastRow.
func (pp *ParquetParser) Columns() []string {
	return pp.columnNames
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
	size int64,
) (storage.ReadSeekCloser, error) {
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

	pf := &parquetFileWrapper{
		ReadSeekCloser: r,
		store:          store,
		ctx:            ctx,
		path:           path,
	}
	pf.InitBuffer(defaultBufSize)
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
) (*ParquetParser, error) {
	wrapper, ok := r.(*parquetFileWrapper)
	if !ok {
		wrapper = &parquetFileWrapper{
			ReadSeekCloser: r,
			store:          store,
			ctx:            ctx,
			path:           path,
		}
		wrapper.InitBuffer(defaultBufSize)
	}

	allocator := GetDefaultAllocator()
	prop := parquet.NewReaderProperties(allocator)
	prop.BufferedStreamEnabled = true

	reader, err := file.NewParquetReader(wrapper, file.WithReadProps(prop))
	if err != nil {
		return nil, errors.Trace(err)
	}

	fileSchema := reader.MetaData().Schema
	columnMetas := make([]convertedType, fileSchema.NumColumns())
	columnNames := make([]string, 0, fileSchema.NumColumns())

	for i := range columnMetas {
		desc := reader.MetaData().Schema.Column(i)
		columnNames = append(columnNames, strings.ToLower(desc.Name()))

		logicalType := desc.LogicalType()
		if logicalType.IsValid() {
			columnMetas[i].converted, columnMetas[i].decimalMeta = logicalType.ToConvertedType()
		} else {
			columnMetas[i].converted = desc.ConvertedType()
			pnode, _ := desc.SchemaNode().(*schema.PrimitiveNode)
			columnMetas[i].decimalMeta = pnode.DecimalMetadata()
		}
	}

	subreaders := make([]*file.Reader, 0, fileSchema.NumColumns())
	subreaders = append(subreaders, reader)
	for i := 1; i < fileSchema.NumColumns(); i++ {
		newWrapper, err := wrapper.Open("")
		if err != nil {
			return nil, errors.Trace(err)
		}
		reader, err := file.NewParquetReader(newWrapper, file.WithReadProps(prop), file.WithMetadata(reader.MetaData()))
		if err != nil {
			return nil, errors.Trace(err)
		}
		subreaders = append(subreaders, reader)
	}

	parser := &ParquetParser{
		readers:     subreaders,
		colMetas:    columnMetas,
		columnNames: columnNames,
		alloc:       allocator,
		logger:      log.FromContext(ctx),
	}
	if err := parser.Init(); err != nil {
		return nil, errors.Trace(err)
	}

	return parser, nil
}

type sampleAllocator struct {
	allocated []int
}

func (sa *sampleAllocator) Allocate(size int) []byte {
	sa.allocated = append(sa.allocated, size)
	return make([]byte, size)
}

func (*sampleAllocator) Free([]byte) {}

func (sa *sampleAllocator) Reallocate(size int, _ []byte) []byte {
	sa.allocated = append(sa.allocated, size)
	return make([]byte, size)
}

// NewParquetParserWithMeta generates a parquet parser.
func NewParquetParserWithMeta(
	ctx context.Context,
	store storage.ExternalStorage,
	r storage.ReadSeekCloser,
	path string,
	meta ParquetFileMeta,
) (*ParquetParser, error) {
	// Acquire memory limiter first
	var memoryUsage int
	if meta.UseSampleAllocator {
		memoryUsage = 0
		meta.UseStreaming = true
	} else if meta.MemoryUsageFull < meta.MemoryQuota {
		memoryUsage = meta.MemoryUsageFull
		meta.UseStreaming = false
	} else {
		memoryUsage = meta.MemoryUsage
		meta.UseStreaming = true
	}
	memoryUsage = min(memoryUsage, memLimit)
	memLimiter.Acquire(memoryUsage)
	log.FromContext(ctx).Info("Get memory usage of parquet reader",
		zap.String("file", path),
		zap.String("memory usage", fmt.Sprintf("%d MB", memoryUsage>>20)),
		zap.String("memory quota", fmt.Sprintf("%d MB", meta.MemoryUsage>>20)),
		zap.Bool("streaming mode", meta.UseStreaming),
		zap.Bool("use sample allocator", meta.UseSampleAllocator),
	)

	wrapper, ok := r.(*parquetFileWrapper)
	if !ok {
		wrapper = &parquetFileWrapper{
			ReadSeekCloser: r,
			store:          store,
			ctx:            ctx,
			path:           path,
		}
		wrapper.InitBuffer(defaultBufSize)
	}

	var allocator memory.Allocator
	if meta.UseSampleAllocator {
		allocator = &sampleAllocator{}
	} else {
		alloc := GetDefaultAllocator()
		allocator = alloc
	}

	prop := parquet.NewReaderProperties(allocator)
	prop.BufferedStreamEnabled = meta.UseStreaming

	reader, err := file.NewParquetReader(wrapper, file.WithReadProps(prop))
	if err != nil {
		return nil, errors.Trace(err)
	}

	fileSchema := reader.MetaData().Schema
	columnMetas := make([]convertedType, fileSchema.NumColumns())
	columnNames := make([]string, 0, fileSchema.NumColumns())

	for i := range columnMetas {
		desc := reader.MetaData().Schema.Column(i)
		columnNames = append(columnNames, strings.ToLower(desc.Name()))

		logicalType := desc.LogicalType()
		if logicalType.IsValid() {
			columnMetas[i].converted, columnMetas[i].decimalMeta = logicalType.ToConvertedType()
		} else {
			columnMetas[i].converted = desc.ConvertedType()
			pnode, _ := desc.SchemaNode().(*schema.PrimitiveNode)
			columnMetas[i].decimalMeta = pnode.DecimalMetadata()
		}
	}

	subreaders := make([]*file.Reader, 0, fileSchema.NumColumns())
	subreaders = append(subreaders, reader)
	for i := 1; i < fileSchema.NumColumns(); i++ {
		var newWrapper parquet.ReaderAtSeeker
		// If use streaming mode, we will open file for each column.
		if meta.UseStreaming {
			newWrapper, err = wrapper.Open("")
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			newWrapper = wrapper
		}
		reader, err := file.NewParquetReader(newWrapper, file.WithReadProps(prop), file.WithMetadata(reader.MetaData()))
		if err != nil {
			return nil, errors.Trace(err)
		}
		subreaders = append(subreaders, reader)
	}

	parser := &ParquetParser{
		readers:     subreaders,
		colMetas:    columnMetas,
		columnNames: columnNames,
		alloc:       allocator,
		logger:      log.FromContext(ctx),
		memoryUsage: memoryUsage,
		memLimiter:  memLimiter,
	}
	if err := parser.Init(); err != nil {
		return nil, errors.Trace(err)
	}

	return parser, nil
}
