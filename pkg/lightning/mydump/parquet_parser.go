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
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

const (
	// defaultBatchSize is the number of rows fetched each time in the parquet reader
	defaultBatchSize = 128

	// defaultBufSize specifies the default size of skip buffer.
	// Skip buffer is used when reading data from the cloud. If there is a gap between the current
	// read position and the last read position, these data is stored in this buffer to avoid
	// potentially reopening the underlying file when the gap size is less than the buffer size.
	defaultBufSize = 64 * 1024

	utcTimeLayout = "2006-01-02 15:04:05.999999Z"
	timeLayout    = "2006-01-02 15:04:05.999999"
)

var openedParser atomic.Int32

// columnDumper is a helper struct to read data from one column.
type columnDumper struct {
	reader         file.ColumnChunkReader
	batchSize      int64
	valueOffset    int
	valuesBuffered int

	levelOffset    int64
	levelsBuffered int64
	defLevels      []int16
	repLevels      []int16
	values         []any

	valueBuffer any
}

func createDumper(tp parquet.Type) *columnDumper {
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
		values:      make([]any, batchSize),
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
func (dump *columnDumper) Next() (any, bool) {
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

func (pf *parquetFileWrapper) Init(bufSize int) {
	pf.skipBuf = make([]byte, bufSize)
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

func (pf *parquetFileWrapper) Open(name string) (parquet.ReaderAtSeeker, error) {
	if len(name) == 0 {
		name = pf.path
	}
	reader, err := pf.store.Open(pf.ctx, name, &storage.ReaderOption{PrefetchSize: 1 << 20})
	if err != nil {
		return nil, errors.Trace(err)
	}

	newPf := &parquetFileWrapper{
		ReadSeekCloser: reader,
		store:          pf.store,
		ctx:            pf.ctx,
		path:           name,
	}
	newPf.Init(defaultBufSize)
	return newPf, nil
}

// ParquetParser parses a parquet file for import
// It implements the Parser interface.
type ParquetParser struct {
	readers     []*file.Reader
	colMetas    []convertedType
	columnNames []string

	alloc memory.Allocator

	dumpers []*columnDumper

	// rows stores the actual data after parsing.
	rows [][]types.Datum

	// curIdx and avail is the current index and total number of rows in rows buffer
	curIdx int
	avail  int

	curRowGroup   int
	totalRowGroup int

	curRowInGroup    int // number of rows read in current group
	totalRowsInGroup int // total rows in current group
	curRows          int // number of rows read in total
	totalRows        int // total rows in this file
	totalBytesRead   int // total bytes read, estimated by all the read datum.

	lastRow Row
	logger  log.Logger

	memoryUsage int
	memLimiter  *membuf.Limiter
}

func (pp *ParquetParser) setStringData(row, col int, val any) {
	vba, _ := val.(parquet.ByteArray)
	pp.rows[row][col].SetString(string(vba), "utf8mb4_bin")
}

func (pp *ParquetParser) setInt32Data(row, col int, val any) {
	v32, _ := val.(int32)
	pp.rows[row][col].SetInt64(int64(v32))
}

func (pp *ParquetParser) setUint32Data(row, col int, val any) {
	v64, _ := val.(int64)
	pp.rows[row][col].SetUint64(uint64(v64))
}

func (pp *ParquetParser) setInt64Data(row, col int, val any) {
	v64, _ := val.(int64)
	pp.rows[row][col].SetInt64(v64)
}

func (pp *ParquetParser) setUint64Data(row, col int, val any) {
	v64, _ := val.(int64)
	pp.rows[row][col].SetUint64(uint64(v64))
}

func (pp *ParquetParser) setTimeMillisData(row, col int, val any) {
	v32, _ := val.(int32)
	timeStr := formatTime(int64(v32), "MILLIS", "15:04:05.999999", "15:04:05.999999Z", true)
	pp.rows[row][col].SetString(timeStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setTimeMicrosData(row, col int, val any) {
	v64, _ := val.(int64)
	timeStr := formatTime(v64, "MICROS", "15:04:05.999999", "15:04:05.999999Z", true)
	pp.rows[row][col].SetString(timeStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setTimestampMillisData(row, col int, val any) {
	v64, _ := val.(int64)
	timeStr := formatTime(v64, "MILLIS", timeLayout, utcTimeLayout, true)
	pp.rows[row][col].SetString(timeStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setTimestampMicrosData(row, col int, val any) {
	v64, _ := val.(int64)
	timeStr := formatTime(v64, "MICROS", timeLayout, utcTimeLayout, true)
	pp.rows[row][col].SetString(timeStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setDateData(row, col int, val any) {
	v32, _ := val.(int32)
	dateStr := time.Unix(int64(v32)*86400, 0).Format(time.DateOnly)
	pp.rows[row][col].SetString(dateStr, "utf8mb4_bin")
}

func (pp *ParquetParser) setDecimalData(row, col int, val any) {
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

func (pp *ParquetParser) setBoolData(row, col int, val any) {
	boolVal, _ := val.(bool)
	if boolVal {
		pp.rows[row][col].SetUint64(1)
		return
	}
	pp.rows[row][col].SetUint64(0)
}

func (pp *ParquetParser) setFloat32Data(row, col int, val any) {
	vf32, _ := val.(float32)
	pp.rows[row][col].SetFloat32(vf32)
}

func (pp *ParquetParser) setFloat64Data(row, col int, val any) {
	vf64, _ := val.(float64)
	pp.rows[row][col].SetFloat64(vf64)
}

func (pp *ParquetParser) setFixedByteArrayData(row, col int, val any) {
	vfa, _ := val.(parquet.FixedLenByteArray)
	pp.rows[row][col].SetString(string(vfa), "utf8mb4_bin")
}

func (pp *ParquetParser) setByteArrayData(row, col int, val any) {
	vba, _ := val.(parquet.ByteArray)
	pp.rows[row][col].SetString(string(vba), "utf8mb4_bin")
}

func (pp *ParquetParser) setInt96Data(row, col int, val any) {
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
		pp.dumpers[i] = createDumper(meta.Schema.Column(i).PhysicalType())
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

	for i := 0; i < readNum; i++ {
		pp.totalBytesRead += estimateRowSize(pp.rows[i])
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

		var setFunc func(row, col int, val any)
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

		if pp.memLimiter != nil {
			pp.memLimiter.Release(pp.memoryUsage)
		}

		openedParser.Add(-1)
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
// Return error if we can't read next row.
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

func estimateRowSize(row []types.Datum) int {
	length := 0
	for _, v := range row {
		if v.IsNull() {
			continue
		}
		if v.Kind() == types.KindString {
			// use GetBytes to avoid memory allocation
			length += len(v.GetBytes())
		} else {
			length += 8
		}
	}
	return length
}

// LastRow gets the last row parsed by the parser.
// It implements the Parser interface.
func (pp *ParquetParser) LastRow() Row {
	pp.lastRow.Length = estimateRowSize(pp.lastRow.Row)
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
) (storage.ReadSeekCloser, error) {
	r, err := store.Open(ctx, path, &storage.ReaderOption{PrefetchSize: 1 << 20})
	if err != nil {
		return nil, err
	}

	pf := &parquetFileWrapper{
		ReadSeekCloser: r,
		store:          store,
		ctx:            ctx,
		path:           path,
	}
	pf.Init(defaultBufSize)
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

// sampleAllocator is used to collection memory usage in parquet reader.
type sampleAllocator struct {
	maxCompressedLength int
	maxDataPage         int
	totalDictPage       int
	otherAllocated      int
}

func (sa *sampleAllocator) Allocate(size int, tp memory.BufferType) []byte {
	allocSize := AllocSize(size)
	switch tp {
	case memory.BufferCompressed:
		sa.maxCompressedLength = max(sa.maxCompressedLength, allocSize)
	case memory.BufferDataPage:
		sa.maxDataPage = max(sa.maxDataPage, allocSize)
	case memory.BufferDictionary:
		// For each row group, we need to store all dictionary pages to decode data page.
		sa.totalDictPage += allocSize
	default:
		sa.otherAllocated += allocSize
	}
	return make([]byte, size)
}

func (*sampleAllocator) Free([]byte) {}

func (sa *sampleAllocator) Reallocate(size int, _ []byte, tp memory.BufferType) []byte {
	return sa.Allocate(size, tp)
}

func (sa *sampleAllocator) reset() {
	sa.maxCompressedLength = 0
	sa.maxDataPage = 0
	sa.totalDictPage = 0
	sa.otherAllocated = 0
}

// GetDefaultParquetMeta return a default file meta
func GetDefaultParquetMeta() ParquetFileMeta {
	return ParquetFileMeta{
		MemoryUsageStream:  0,
		MemoryUsageFull:    math.MaxInt32,
		MemoryQuota:        0,
		UseSampleAllocator: true,
		UseStreaming:       true,
	}
}

// NewParquetParser generates a parquet parser.
func NewParquetParser(
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
	} else if meta.MemoryUsageFull <= meta.MemoryQuota || meta.MemoryUsageFull == meta.MemoryUsageStream {
		memoryUsage = meta.MemoryUsageFull
		meta.UseStreaming = false
	} else {
		memoryUsage = meta.MemoryUsageStream
		meta.UseStreaming = true
	}
	memoryUsage = min(memoryUsage, readerMemoryLimit)
	readerMemoryLimiter.Acquire(memoryUsage)
	log.FromContext(ctx).Info("Get memory usage of parquet reader",
		zap.String("file", path),
		zap.String("memory usage", fmt.Sprintf("%d MB", memoryUsage>>20)),
		zap.String("memory usage full", fmt.Sprintf("%d MB", meta.MemoryUsageFull>>20)),
		zap.String("memory quota", fmt.Sprintf("%d MB", meta.MemoryQuota>>20)),
		zap.String("memory limit", fmt.Sprintf("%d MB", readerMemoryLimit>>20)),
		zap.Int32("opened parser", openedParser.Add(1)),
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
		wrapper.Init(defaultBufSize)
	}

	var allocator memory.Allocator
	if meta.UseSampleAllocator {
		allocator = &sampleAllocator{}
	} else {
		alloc := GetAllocator()
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
		memLimiter:  readerMemoryLimiter,
	}
	if err := parser.Init(); err != nil {
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
	avgRowSize float64,
	memoryUsageStream int,
	memoryUsageFull int,
	err error,
) {
	r, err := store.Open(ctx, fileMeta.Path, nil)
	if err != nil {
		return 0, 0, 0, err
	}

	wrapper := &parquetFileWrapper{
		ReadSeekCloser: r,
		store:          store,
		ctx:            ctx,
		path:           fileMeta.Path,
	}
	wrapper.Init(defaultBufSize)

	prop := parquet.NewReaderProperties(nil)
	prop.BufferedStreamEnabled = true
	reader, err := file.NewParquetReader(wrapper, file.WithReadProps(prop))
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
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
	allSampleAllocators := make([]*sampleAllocator, 0, fileSchema.NumColumns())
	for i := 0; i < fileSchema.NumColumns(); i++ {
		newWrapper, err := wrapper.Open("")
		if err != nil {
			return 0, 0, 0, errors.Trace(err)
		}

		alloc := &sampleAllocator{}
		prop := parquet.NewReaderProperties(alloc)
		prop.BufferedStreamEnabled = true
		allSampleAllocators = append(allSampleAllocators, alloc)

		reader, err := file.NewParquetReader(
			newWrapper,
			file.WithReadProps(prop),
			file.WithMetadata(reader.MetaData()),
		)

		if err != nil {
			return 0, 0, 0, errors.Trace(err)
		}
		subreaders = append(subreaders, reader)
	}

	parser := &ParquetParser{
		readers:     subreaders,
		colMetas:    columnMetas,
		columnNames: columnNames,
		logger:      log.FromContext(ctx),
	}
	if err := parser.Init(); err != nil {
		return 0, 0, 0, errors.Trace(err)
	}

	//nolint: errcheck
	defer parser.Close()

	var (
		rowSize  int64
		rowCount int64
	)

	if reader.NumRowGroups() == 0 || reader.MetaData().RowGroups[0].NumRows == 0 {
		return 0, 0, 0, nil
	}

	totalReadRows := reader.MetaData().RowGroups[0].NumRows
	for i := 0; i < int(totalReadRows); i++ {
		err = parser.ReadRow()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				break
			}
			return 0, 0, 0, err
		}
		lastRow := parser.LastRow()
		rowCount++
		rowSize += int64(lastRow.Length)
		parser.RecycleRow(lastRow)
	}

	avgRowSize = float64(rowSize) / float64(rowCount)

	memoryUsageStream = len(columnMetas) << 20
	memoryUsageFull = len(columnMetas) << 20

	for _, alloc := range allSampleAllocators {
		memoryUsageFull += alloc.maxDataPage
		memoryUsageFull += alloc.totalDictPage
		memoryUsageStream += alloc.otherAllocated
		memoryUsageStream += alloc.maxDataPage
		memoryUsageStream += alloc.totalDictPage
	}

	pageBufferFull := 0
	for _, rg := range parser.readers[0].MetaData().RowGroups {
		totalUsage := 0
		for _, c := range rg.Columns {
			totalUsage += AllocSize(int(c.MetaData.GetTotalCompressedSize()))
		}
		pageBufferFull = max(pageBufferFull, totalUsage)
	}
	memoryUsageFull += pageBufferFull

	memoryUsageStream = roundUp(memoryUsageStream, defaultArenaSize)
	memoryUsageFull = roundUp(memoryUsageFull, defaultArenaSize)
	return avgRowSize, memoryUsageStream, memoryUsageFull, nil
}
