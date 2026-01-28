// Copyright 2023-2023 PingCAP, Inc.

package mydump

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
	"time"

	"gitee.com/joccau/orc"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	orc_proto "github.com/scritchley/orc/proto"
	"go.uber.org/zap"
)

type fileReader struct {
	*storage.LocalFile
}

// Size returns the size of the file in bytes.
func (f fileReader) Size() int64 {
	stats, err := f.Stat()
	if err != nil {
		return 0
	}
	return stats.Size()
}

func readOrcFileRowCount(
	ctx context.Context,
	reader storage.ReadSeekCloser,
) (int64, error) {
	lf, ok := reader.(*storage.LocalFile)
	if !ok {
		panic("only support local storage!")
	}

	r, err := orc.NewReader(fileReader{
		LocalFile: lf,
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer r.Close()

	return int64(r.NumRows()), nil
}

// ReadOrcFileRowCountByFile reads the total count of orc file.
func ReadOrcFileRowCountByFile(
	ctx context.Context,
	store storage.ExternalStorage,
	fileMeta SourceFileMeta,
) (int64, error) {
	r, err := store.Open(ctx, fileMeta.Path, nil)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return readOrcFileRowCount(ctx, r)
}

// ORCParser defines the parser to read orc file.
// It implements the mydump.Parser interface.
type ORCParser struct {
	Reader *orc.Reader
	cursor *orc.Cursor

	// rowOffSet specifies the row idx in the full ORC file.
	pos int64
	// stripeOffset specifies the specified stripe.
	stripeIdx int64
	// rowOffset specifies the specified row idx in specified stripe.
	rowIdxInStripe int64

	lastRow Row
	logger  log.Logger

	fr *fileReader

	columnName []string
	columnType []*orc.TypeDescription
}

// NewORCParser creates a parser to parse the orc file.
func NewORCParser(logger log.Logger, reader storage.ReadSeekCloser, path string) (*ORCParser, error) {
	lf, ok := reader.(*storage.LocalFile)
	if !ok {
		panic("only support local storage!")
	}

	fr := &fileReader{
		LocalFile: lf,
	}
	r, err := orc.NewReader(fr)
	if err != nil {
		return nil, err
	}

	columns := r.Schema().Columns()
	schema := r.Schema()
	subSchemas := make([]*orc.TypeDescription, 0, len(columns))
	for _, column := range columns {
		subSchema, err := schema.GetField(column)
		if err != nil {
			return nil, errors.Trace(err)
		}

		subSchemas = append(subSchemas, subSchema)
	}

	cursor := r.Select(r.Schema().Columns()...)

	return &ORCParser{
		Reader:     r,
		cursor:     cursor,
		logger:     logger,
		fr:         fr,
		columnName: columns,
		columnType: subSchemas,
	}, nil
}

// NumRows get the row count of orc file.
func (p *ORCParser) NumRows() int {
	return p.Reader.NumRows()
}

// Pos returns the currently row number of the parquet file and rowID.
// implement mydump.Parser
func (p *ORCParser) Pos() (pos int64, rowID int64) {
	return p.pos, p.lastRow.RowID
}

func (p *ORCParser) calStripeIdx(pos int64) (stripeIdx int64, rowIdxInStripe int64, err error) {
	if pos > int64(p.NumRows()) {
		return 0, 0, errors.Errorf("invalid pos: %v should smaller than row count: %v ", pos, p.NumRows())
	}

	stripes, err := p.Reader.GetStripes()
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	var rows int64 = 0
	for i, stripe := range stripes {
		row := int64(stripe.GetNumberOfRows())
		if pos <= rows+row {
			return int64(i), pos - rows, nil
		}

		rows += row
	}

	return 0, 0, errors.Errorf("invalid pos: %v should smaller than row count: %v ", pos, p.NumRows())
}

// SetPos sets the row as the pos in the orc file.
func (p *ORCParser) SetPos(pos int64, rowID int64) error {
	if pos > int64(p.NumRows()) {
		p.logger.Error("invalid pos in file", zap.Int64("pos", pos), zap.Int("row-count", p.NumRows()))
		return errors.Errorf("invalid pos: %v should smaller than row count: %v ", pos, p.NumRows())
	}
	if pos < p.pos {
		panic("don't support seek back yet")
	}

	stripeIdx, rowIdxInStripe, err := p.calStripeIdx(pos)
	if err != nil {
		return errors.Trace(err)
	}

	// seek stripeIdx
	for i := 0; i <= int(stripeIdx); i++ {
		if ok := p.cursor.Stripes(); !ok {
			return errors.Errorf("invalid pos: %v", pos)
		}
	}

	// seek rowIdx in stripe
	for i := 0; i < int(rowIdxInStripe); i++ {
		if ok := p.cursor.Next(); !ok {
			return errors.Errorf("invalid pos: %v", pos)
		}
	}

	p.lastRow.RowID = rowID
	p.pos = pos
	p.stripeIdx = stripeIdx
	p.rowIdxInStripe = rowIdxInStripe
	return nil
}

// ScannedPos implements the Parser interface.
// For orc it's parquet file's reader current position.
func (p *ORCParser) ScannedPos() (int64, error) {
	return p.fr.Seek(0, io.SeekCurrent)
}

// Close close the orc parser if read orc file finished.
func (p *ORCParser) Close() error {
	if err := p.cursor.Close(); err != nil {
		return errors.Trace(err)
	}
	return p.Reader.Close()
}

// readRowInStripe reads a row in the stripe.
func (p *ORCParser) readRowInStripe() (bool, error) {
	if p.cursor.Next() {
		p.pos++
		p.rowIdxInStripe++

		r := p.cursor.Row()
		length := len(p.Columns())
		p.lastRow.Row = make([]types.Datum, length)

		for i := 0; i < length; i++ {
			if r[i] == nil {
				p.lastRow.Row[i].SetNull()
				continue
			}

			v := reflect.ValueOf(r[i])
			p.lastRow.Length += getDatumLen(v)
			if err := setORCDatumValue(&p.lastRow.Row[i], r[i], p.columnType[i], p.logger); err != nil {
				return false, errors.Trace(err)
			}
		}
		return true, nil
	}

	return false, nil
}

// ReadRow reads a row of orc file.
func (p *ORCParser) ReadRow() error {
	p.lastRow.RowID++
	p.lastRow.Length = 0

	hasNext, err := p.readRowInStripe()
	if err != nil {
		return errors.Trace(err)
	} else if !hasNext {
		if p.cursor.Stripes() {
			p.stripeIdx++
			p.rowIdxInStripe = 0
			hasNext, err = p.readRowInStripe()
			if err != nil {
				return errors.Trace(err)
			}

			if !hasNext && p.cursor.Err() == nil {
				return io.EOF
			}
		} else {
			if p.cursor.Err() == nil {
				return io.EOF
			}
		}
	}

	return errors.Trace(p.cursor.Err())
}

// LastRow gets the last row parsed by the parser.
// It implements the Parser interface.
func (p *ORCParser) LastRow() Row {
	return p.lastRow
}

// RecycleRow implements the Parser interface.
func (p *ORCParser) RecycleRow(row Row) {
}

// Columns returns the _lower-case_ column names corresponding to values in
// the LastRow.
func (p *ORCParser) Columns() []string {
	return p.Reader.Schema().Columns()
}

// SetColumns set restored column names to parser
func (p *ORCParser) SetColumns(strings []string) {
	// just do nothing
}

// SetLogger sets the logger used in the parser.
// It implements the Parser interface.
func (p *ORCParser) SetLogger(logger log.Logger) {
	p.logger = logger
}

// SetRowID sets the rowID in a orc file when we start a compressed file.
// It implements the Parser interface.
func (p *ORCParser) SetRowID(rowID int64) {
	p.lastRow.RowID = rowID
}

func setORCDatumValue(d *types.Datum, value any, t *orc.TypeDescription, logger log.Logger) error {
	return setORCDatumByReflect(d, value, t, logger)
}

func setORCDatumByReflect(d *types.Datum, value any, t *orc.TypeDescription, logger log.Logger) error {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		d.SetUint64(v.Uint())
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		d.SetInt64(v.Int())
	case reflect.String:
		d.SetString(v.String(), mysql.DefaultCollationName)
	case reflect.Float32:
		d.SetFloat32FromF64(v.Float())
	case reflect.Float64:
		d.SetFloat64(v.Float())
	default:
		return setORCDatumByOrcKind(d, value, t, logger)
	}
	return nil
}

func setORCDatumByOrcKind(d *types.Datum, value any, t *orc.TypeDescription, logger log.Logger) error {
	v := reflect.ValueOf(value)
	switch *t.Type().Kind {
	case orc_proto.Type_BOOLEAN:
		if v.Bool() {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case orc_proto.Type_BYTE: // tinyint
		d.SetInt64(v.Int())
	case orc_proto.Type_SHORT, orc_proto.Type_INT, orc_proto.Type_LONG:
		d.SetInt64(v.Int())
	case orc_proto.Type_FLOAT:
		d.SetFloat32FromF64(v.Float())
	case orc_proto.Type_DOUBLE:
		d.SetFloat64(v.Float())
	case orc_proto.Type_STRING, orc_proto.Type_CHAR, orc_proto.Type_VARCHAR:
		d.SetString(v.String(), mysql.DefaultCollationName)
	case orc_proto.Type_BINARY:
		d.SetBytes(v.Bytes())
	case orc_proto.Type_DATE:
		date, ok := value.(orc.Date)
		if !ok {
			logger.Error("unknown value", zap.Stringer("kind", v.Kind()),
				zap.String("type", v.Type().Name()), zap.Reflect("value", v.Interface()))
			return errors.Errorf("unknown value: %v", v)
		}

		coretime := types.FromGoTime(date.Time)
		t := types.NewTime(coretime, mysql.TypeDate, types.DefaultFsp)
		d.SetMysqlTime(t)
	case orc_proto.Type_TIMESTAMP:
		ts, ok := value.(time.Time)
		if !ok {
			logger.Error("unknown value", zap.Stringer("kind", v.Kind()),
				zap.String("type", v.Type().Name()), zap.Reflect("value", v.Interface()))
			return errors.Errorf("unknown value: %v", v)
		}

		coretime := types.FromGoTime(ts)
		t := types.NewTime(coretime, mysql.TypeDatetime, types.MaxFsp)
		d.SetMysqlTime(t)
	case orc_proto.Type_DECIMAL:
		decimal, ok := value.(orc.Decimal)
		if !ok {
			logger.Error("unknown value", zap.Stringer("kind", v.Kind()),
				zap.String("type", v.Type().Name()), zap.Reflect("value", v.Interface()))
			return errors.Errorf("unknown value: %v", v)
		}

		mydecimal := &types.MyDecimal{}
		mydecimal.FromFloat64(decimal.Float64())
		d.SetMysqlDecimal(mydecimal)
	case orc_proto.Type_STRUCT, orc_proto.Type_MAP, orc_proto.Type_UNION, orc_proto.Type_LIST:
		jsondata, err := json.Marshal(value)
		if err != nil {
			return errors.Annotatef(err, "failed to marshal, unknown value: %v", v)
		}

		bj := types.BinaryJSON{}
		if err = bj.UnmarshalJSON(jsondata); err != nil {
			return errors.Trace(err)
		}
		d.SetMysqlJSON(bj)
	default:
		logger.Error("unknown value", zap.Stringer("kind", v.Kind()),
			zap.String("type", v.Type().Name()), zap.Reflect("value", v.Interface()))
		return errors.Errorf("unknown value: %v", v)
	}

	return nil
}
