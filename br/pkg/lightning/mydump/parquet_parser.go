package mydump

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/types"
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
)

type ParquetParser struct {
	Reader      *preader.ParquetReader
	columns     []string
	columnMetas []*parquet.SchemaElement
	rows        []interface{}
	readRows    int64
	curStart    int64
	curIndex    int
	lastRow     Row
	logger      log.Logger
}

// readerWrapper is a used for implement `source.ParquetFile`
type readerWrapper struct {
	ReadSeekCloser
	store storage.ExternalStorage
	ctx   context.Context
	// current file path
	path string
}

func (r *readerWrapper) Write(p []byte) (n int, err error) {
	return 0, errors.New("unsupported operation")
}

func (r *readerWrapper) Open(name string) (source.ParquetFile, error) {
	if len(name) == 0 {
		name = r.path
	}
	reader, err := r.store.Open(r.ctx, name)
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

func (r *readerWrapper) Create(name string) (source.ParquetFile, error) {
	return nil, errors.New("unsupported operation")
}

// bytesReaderWrapper is a wrapper of bytes.Reader used for implement `source.ParquetFile`
type bytesReaderWrapper struct {
	*bytes.Reader
	rawBytes []byte
	// current file path
	path string
}

func (r *bytesReaderWrapper) Close() error {
	return nil
}

func (r *bytesReaderWrapper) Create(name string) (source.ParquetFile, error) {
	return nil, errors.New("unsupported operation")
}

func (r *bytesReaderWrapper) Write(p []byte) (n int, err error) {
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

	r, err := store.Open(ctx, path)
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

// a special func to fetch parquet file row count fast.
func ReadParquetFileRowCount(
	ctx context.Context,
	store storage.ExternalStorage,
	r storage.ReadSeekCloser,
	path string,
) (int64, error) {
	wrapper := &readerWrapper{
		ReadSeekCloser: r,
		store:          store,
		ctx:            ctx,
		path:           path,
	}
	var err error
	res := new(preader.ParquetReader)
	res.NP = 1
	res.PFile = wrapper
	if err = res.ReadFooter(); err != nil {
		return 0, err
	}
	numRows := res.Footer.NumRows
	if err = wrapper.Close(); err != nil {
		return 0, err
	}
	return numRows, nil
}

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
	for _, c := range reader.SchemaHandler.SchemaElements {
		if c.GetNumChildren() == 0 {
			// NOTE: the SchemaElement.Name is capitalized, SchemaHandler.Infos.ExName is the raw column name
			// though in this context, there is no difference between these two fields
			columns = append(columns, strings.ToLower(c.Name))
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
		Reader:      reader,
		columns:     columns,
		columnMetas: columnMetas,
		logger:      log.L(),
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

func (pp *ParquetParser) Close() error {
	pp.Reader.ReadStop()
	return pp.Reader.PFile.Close()
}

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
	for i := 0; i < length; i++ {
		pp.lastRow.Length += getDatumLen(v.Field(i))
		if err := setDatumValue(&pp.lastRow.Row[i], v.Field(i), pp.columnMetas[i]); err != nil {
			return err
		}
	}
	return nil
}

func getDatumLen(v reflect.Value) int {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return 0
		} else {
			return getDatumLen(v.Elem())
		}
	}
	if v.Kind() == reflect.String {
		return len(v.String())
	}
	return 8
}

// convert a parquet value to Datum
//
// See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
func setDatumValue(d *types.Datum, v reflect.Value, meta *parquet.SchemaElement) error {
	switch v.Kind() {
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
		if v.IsNil() {
			d.SetNull()
		} else {
			return setDatumValue(d, v.Elem(), meta)
		}
	default:
		log.L().Error("unknown value", zap.Stringer("kind", v.Kind()),
			zap.String("type", v.Type().Name()), zap.Reflect("value", v.Interface()))
		return errors.Errorf("unknown value: %v", v)
	}
	return nil
}

func setDatumByString(d *types.Datum, v string, meta *parquet.SchemaElement) {
	if meta.LogicalType != nil && meta.LogicalType.DECIMAL != nil {
		v = binaryToDecimalStr([]byte(v), int(meta.LogicalType.DECIMAL.Scale))
	}
	d.SetString(v, "")
}

func binaryToDecimalStr(rawBytes []byte, scale int) string {
	negative := rawBytes[0] > 127
	if negative {
		for i := 0; i < len(rawBytes); i++ {
			rawBytes[i] = ^rawBytes[i]
		}
		for i := len(rawBytes) - 1; i >= 0; i-- {
			rawBytes[i] += 1
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
		d.SetString(val[:dotIndex]+"."+val[dotIndex:], "")
	case logicalType.DATE != nil:
		dateStr := time.Unix(v*86400, 0).Format("2006-01-02")
		d.SetString(dateStr, "")
	case logicalType.TIMESTAMP != nil:
		// convert all timestamp types (datetime/timestamp) to string
		timeStr := formatTime(v, logicalType.TIMESTAMP.Unit, "2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05.999999Z", logicalType.TIMESTAMP.IsAdjustedToUTC)
		d.SetString(timeStr, "")
	case logicalType.TIME != nil:
		// convert all timestamp types (datetime/timestamp) to string
		timeStr := formatTime(v, logicalType.TIME.Unit, "15:04:05.999999", "15:04:05.999999Z",
			logicalType.TIME.IsAdjustedToUTC)
		d.SetString(timeStr, "")
	default:
		d.SetInt64(v)
	}
	return nil
}

func formatTime(v int64, units *parquet.TimeUnit, format, utcFormat string, utc bool) string {
	var sec, nsec int64
	if units.MICROS != nil {
		sec = v / 1e6
		nsec = (v % 1e6) * 1e3
	} else if units.MILLIS != nil {
		sec = v / 1e3
		nsec = (v % 1e3) * 1e6
	} else {
		// nano
		sec = v / 1e9
		nsec = v % 1e9
	}
	t := time.Unix(sec, nsec).UTC()
	if utc {
		return t.Format(utcFormat)
	}
	return t.Format(format)
}

func (pp *ParquetParser) LastRow() Row {
	return pp.lastRow
}

func (pp *ParquetParser) RecycleRow(row Row) {
}

// Columns returns the _lower-case_ column names corresponding to values in
// the LastRow.
func (pp *ParquetParser) Columns() []string {
	return pp.columns
}

// SetColumns set restored column names to parser
func (pp *ParquetParser) SetColumns(cols []string) {
	// just do nothing
}

func (pp *ParquetParser) SetLogger(l log.Logger) {
	pp.logger = l
}
