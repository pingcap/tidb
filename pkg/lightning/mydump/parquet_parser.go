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
	"math/big"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/xitongsys/parquet-go/source"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
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

// Buffers to store data read from columns.
// Declare here to avoid frequent allocation.
type readBuffer struct {
	fixedLenArrayBuffer []parquet.FixedLenByteArray
	float32Buffer       []float32
	float64Buffer       []float64
	byteArrayBuffer     []parquet.ByteArray
	int32Buffer         []int32
	int64Buffer         []int64
	int96Buffer         []parquet.Int96
	boolBuffer          []bool
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

func formatTime(v int64, unit string, format, utcFormat string) string {
	var t time.Time
	switch unit {
	case "MICROS":
		t = time.UnixMicro(v)
	case "MILLIS":
		t = time.UnixMilli(v)
	default:
		t = time.Unix(0, v)
	}

	return t.UTC().Format(utcFormat)
}

// ParquetParser parses a parquet file for import
// It implements the Parser interface.
type ParquetParser struct {
	reader      *file.Reader
	colMetas    []convertedType
	columnNames []string

	colReaders []file.ColumnChunkReader
	colBuffers []readBuffer
	rows       [][]types.Datum
	curIdx     int
	avail      int

	curRowGroup   int
	totalRowGroup int

	curRowInGroup    int
	totalRowsInGroup int
	curRows          int
	totalRows        int

	lastRow Row
	logger  log.Logger

	readSeekCloser ReadSeekCloser
}

func (p *ParquetParser) setStringData(readNum, col, offset int) {
	buf := p.colBuffers[col].byteArrayBuffer
	for i := 0; i < readNum; i++ {
		p.rows[offset+i][col].SetString(buf[i].String(), "utf8mb4_bin")
	}
}

func (p *ParquetParser) setInt32Data(readNum, col, offset int) {
	buf := p.colBuffers[col].int32Buffer
	for i := 0; i < readNum; i++ {
		p.rows[offset+i][col].SetInt64(int64(buf[i]))
	}
}

func (p *ParquetParser) setUint32Data(readNum, col, offset int) {
	buf := p.colBuffers[col].int32Buffer
	for i := 0; i < readNum; i++ {
		p.rows[offset+i][col].SetUint64(uint64(buf[i]))
	}
}

func (p *ParquetParser) setInt64Data(readNum, col, offset int) {
	buf := p.colBuffers[col].int64Buffer
	for i := 0; i < readNum; i++ {
		p.rows[offset+i][col].SetInt64(int64(buf[i]))
	}
}

func (p *ParquetParser) setUint64Data(readNum, col, offset int) {
	buf := p.colBuffers[col].int64Buffer
	for i := 0; i < readNum; i++ {
		p.rows[offset+i][col].SetUint64(uint64(buf[i]))
	}
}

func (p *ParquetParser) setTimeMillisData(readNum, col, offset int) {
	buf := p.colBuffers[col].int32Buffer
	for i := 0; i < readNum; i++ {
		timeStr := formatTime(int64(buf[i]), "MILLIS", "15:04:05.999999", "15:04:05.999999Z")
		p.rows[offset+i][col].SetString(timeStr, "utf8mb4_bin")
	}
}

func (p *ParquetParser) setTimeMicrosData(readNum, col, offset int) {
	buf := p.colBuffers[col].int32Buffer
	for i := 0; i < readNum; i++ {
		timeStr := formatTime(int64(buf[i]), "MICROS", "15:04:05.999999", "15:04:05.999999Z")
		p.rows[offset+i][col].SetString(timeStr, "utf8mb4_bin")
	}
}

func (p *ParquetParser) setTimestampMillisData(readNum, col, offset int) {
	buf := p.colBuffers[col].int64Buffer
	for i := 0; i < readNum; i++ {
		timeStr := formatTime(buf[i], "MILLIS", timeLayout, utcTimeLayout)
		p.rows[offset+i][col].SetString(timeStr, "utf8mb4_bin")
	}
}

func (p *ParquetParser) setTimestampMicrosData(readNum, col, offset int) {
	buf := p.colBuffers[col].int64Buffer
	for i := 0; i < readNum; i++ {
		timeStr := formatTime(buf[i], "MICROS", timeLayout, utcTimeLayout)
		p.rows[offset+i][col].SetString(timeStr, "utf8mb4_bin")
	}
}

func (p *ParquetParser) setDateData(readNum, col, offset int) {
	buf := p.colBuffers[col].int32Buffer
	for i := 0; i < readNum; i++ {
		dateStr := time.Unix(int64(buf[i])*86400, 0).Format(time.DateOnly)
		p.rows[offset+i][col].SetString(dateStr, "utf8mb4_bin")
	}
}

func (p *ParquetParser) setDecimalData(readNum, col, offset int) error {
	colTp := p.colReaders[col].Type()
	decimal := p.colMetas[col].decimalMeta

	for i := 0; i < readNum; i++ {
		if colTp == parquet.Types.Int32 || colTp == parquet.Types.Int32 {
			v := p.colBuffers[col].int64Buffer[i]
			if colTp == parquet.Types.Int32 {
				v = int64(p.colBuffers[col].int32Buffer[i])
			}
			if !decimal.IsSet || decimal.Scale == 0 {
				p.rows[offset+i][col].SetInt64(v)
				continue
			}
			minLen := decimal.Scale + 1
			if v < 0 {
				minLen++
			}
			val := fmt.Sprintf("%0*d", minLen, v)
			dotIndex := len(val) - int(decimal.Scale)
			p.rows[offset+i][col].SetString(val[:dotIndex]+"."+val[dotIndex:], "utf8mb4_bin")
		} else if colTp == parquet.Types.FixedLenByteArray {
			s := binaryToDecimalStr(p.colBuffers[col].fixedLenArrayBuffer[i], int(decimal.Scale))
			p.rows[offset+i][col].SetString(s, "utf8mb4_bin")
		} else {
			s := binaryToDecimalStr(p.colBuffers[col].byteArrayBuffer[i], int(decimal.Scale))
			p.rows[offset+i][col].SetString(s, "utf8mb4_bin")
		}
	}
	return nil
}

func (p *ParquetParser) setBoolData(readNum, col, offset int) {
	buf := p.colBuffers[col].boolBuffer
	for i := 0; i < readNum; i++ {
		if buf[i] {
			p.rows[offset+i][col].SetUint64(1)
		} else {
			p.rows[offset+i][col].SetUint64(0)
		}
	}
}

func (p *ParquetParser) setInt96Data(readNum, col, offset int) {
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
	buf := p.colBuffers[col].int96Buffer
	for i := 0; i < readNum; i++ {
		p.rows[offset+i][col].SetString(buf[i].ToTime().Format(utcTimeLayout), "utf8mb4_bin")
	}
}

func (p *ParquetParser) Init() error {
	p.curRowGroup, p.totalRowGroup = -1, p.reader.NumRowGroups()

	p.totalRows = int(p.reader.MetaData().NumRows)

	numCols := p.reader.MetaData().Schema.NumColumns()
	p.colReaders = make([]file.ColumnChunkReader, numCols)
	p.colBuffers = make([]readBuffer, numCols)
	p.rows = make([][]types.Datum, batchReadRowSize)
	for i := range p.rows {
		p.rows[i] = make([]types.Datum, numCols)
	}

	return nil
}

func (p *ParquetParser) GetRow() ([]types.Datum, error) {
	if p.curIdx >= p.avail {
		read, err := p.readRows(batchReadRowSize)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if read == 0 {
			return nil, nil
		}
		p.curIdx, p.avail = 0, read
	}

	row := p.rows[p.curIdx]
	p.curIdx++
	return row, nil
}

func (p *ParquetParser) readRows(num int) (int, error) {
	readNum := min(num, p.totalRows-p.curRows)
	if readNum == 0 {
		return 0, nil
	}

	read := 0
	for read < readNum {
		// Move to next row group
		if p.curRowInGroup == p.totalRowsInGroup {
			p.curRowGroup++
			rowGroupReader := p.reader.RowGroup(p.curRowGroup)
			var err error
			for c := 0; c < len(p.colReaders); c++ {
				p.colReaders[c], err = rowGroupReader.Column(c)
				if err != nil {
					return 0, errors.Trace(err)
				}
			}
			p.curRowInGroup, p.totalRowsInGroup = 0, int(rowGroupReader.NumRows())
		}

		// Read in this group
		curRead := min(readNum-read, p.totalRowsInGroup-p.curRowInGroup)
		_, err := p.readInGroup(curRead, read)
		if err != nil {
			return 0, errors.Trace(err)
		}
		read += curRead
		p.curRowInGroup += curRead
	}

	p.curRows += readNum
	return readNum, nil
}

// Read num rows in current row group and store results
func (p *ParquetParser) readInGroup(num, dataOffset int) (int, error) {
	var (
		err   error
		total int64
	)

	// Read data into buffers first
	req := int64(num)
	for i, col := range p.colReaders {
		buf := p.colBuffers[i]
		switch col.Type() {
		case parquet.Types.FixedLenByteArray:
			total, _, err = col.(*file.FixedLenByteArrayColumnChunkReader).ReadBatch(req, buf.fixedLenArrayBuffer, nil, nil)
		case parquet.Types.Float:
			total, _, err = col.(*file.Float32ColumnChunkReader).ReadBatch(req, buf.float32Buffer, nil, nil)
		case parquet.Types.Double:
			total, _, err = col.(*file.Float64ColumnChunkReader).ReadBatch(req, buf.float64Buffer, nil, nil)
		case parquet.Types.ByteArray:
			total, _, err = col.(*file.ByteArrayColumnChunkReader).ReadBatch(req, buf.byteArrayBuffer, nil, nil)
		case parquet.Types.Int32:
			total, _, err = col.(*file.Int32ColumnChunkReader).ReadBatch(req, buf.int32Buffer, nil, nil)
		case parquet.Types.Int64:
			total, _, err = col.(*file.Int64ColumnChunkReader).ReadBatch(req, buf.int64Buffer, nil, nil)
		case parquet.Types.Int96:
			total, _, err = col.(*file.Int96ColumnChunkReader).ReadBatch(req, buf.int96Buffer, nil, nil)
		case parquet.Types.Boolean:
			total, _, err = col.(*file.BooleanColumnChunkReader).ReadBatch(req, buf.boolBuffer, nil, nil)
		}

		if err != nil {
			return 0, errors.Trace(err)
		}

		// Parse data according to converted type
		if col.Type() == parquet.Types.Boolean {
			p.setBoolData(num, i, dataOffset)
			continue
		} else if col.Type() == parquet.Types.Int96 {
			p.setInt96Data(num, i, dataOffset)
			continue
		}

		meta := p.colMetas[i]

		switch meta.converted {
		case schema.ConvertedTypes.BSON:
		case schema.ConvertedTypes.JSON:
		case schema.ConvertedTypes.UTF8:
		case schema.ConvertedTypes.Enum:
			p.setStringData(num, i, dataOffset)
		case schema.ConvertedTypes.Int8:
		case schema.ConvertedTypes.Int16:
		case schema.ConvertedTypes.Int32:
			p.setInt32Data(num, i, dataOffset)
		case schema.ConvertedTypes.Uint8:
		case schema.ConvertedTypes.Uint16:
		case schema.ConvertedTypes.Uint32:
			p.setUint32Data(num, i, dataOffset)
		case schema.ConvertedTypes.Int64:
			p.setInt64Data(num, i, dataOffset)
		case schema.ConvertedTypes.Uint64:
			p.setUint64Data(num, i, dataOffset)
		case schema.ConvertedTypes.TimeMillis:
			p.setTimeMillisData(num, i, dataOffset)
		case schema.ConvertedTypes.TimeMicros:
			p.setTimeMicrosData(num, i, dataOffset)
		case schema.ConvertedTypes.TimestampMillis:
			p.setTimestampMillisData(num, i, dataOffset)
		case schema.ConvertedTypes.TimestampMicros:
			p.setTimestampMicrosData(num, i, dataOffset)
		case schema.ConvertedTypes.Date:
			p.setDateData(num, i, dataOffset)
		case schema.ConvertedTypes.Decimal:
			p.setDecimalData(num, i, dataOffset)
		}
	}

	return int(total), err
}

// Pos returns the currently row number of the parquet file
func (p *ParquetParser) Pos() (pos int64, rowID int64) {
	return int64(p.curRows), p.lastRow.RowID
}

// SetPos sets the position in a parquet file.
// It implements the Parser interface.
func (p *ParquetParser) SetPos(pos int64, rowID int64) error {
	p.lastRow.RowID = rowID
	if pos < int64(p.curRows) {
		panic("don't support seek back yet")
	}

	read := int(pos) - p.curRows
	_, err := p.readRows(read)
	return errors.Trace(err)
}

// ScannedPos implements the Parser interface.
// For parquet it's parquet file's reader current position.
func (pp *ParquetParser) ScannedPos() (int64, error) {
	return pp.readSeekCloser.Seek(0, io.SeekCurrent)
}

// Close closes the parquet file of the parser.
// It implements the Parser interface.
func (pp *ParquetParser) Close() error {
	return pp.reader.Close()
}

// ReadRow reads a row in the parquet file by the parser.
// It implements the Parser interface.
func (p *ParquetParser) ReadRow() error {
	p.lastRow.RowID++
	p.lastRow.Length = 0
	row, err := p.GetRow()
	if err != nil {
		return errors.Trace(err)
	}
	if row == nil {
		return io.EOF
	}
	p.lastRow.Row = row
	p.lastRow.Length = 0
	return nil
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

// readParquetFileRowCount reads the parquet file row count.
// It is a special func to fetch parquet file row count fast.
// TODO(joechnerh): implement this
func readParquetFileRowCount(
	ctx context.Context,
	store storage.ExternalStorage,
	r storage.ReadSeekCloser,
	path string,
) (int64, error) {
	return 0, nil
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
	numberRows, err := readParquetFileRowCount(ctx, store, r, fileMeta.Path)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return numberRows, nil
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

	// TODO(joechenrh): use r
	reader, err := file.OpenParquetFile(path, false)
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
			columnMetas[i].decimalMeta = desc.SchemaNode().(*schema.PrimitiveNode).DecimalMetadata()
		}
	}

	parser := &ParquetParser{
		reader:         reader,
		colMetas:       columnMetas,
		columnNames:    columnNames,
		logger:         log.FromContext(ctx),
		readSeekCloser: wrapper,
	}
	parser.Init()

	return parser, nil
}
