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
	"io"
	"os"
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/types"
	"github.com/scritchley/orc"
	"go.uber.org/zap"
)

type ORCParser struct {
	reader  *orc.Reader
	lastRow Row
	cursor  *orc.Cursor
	logger  log.Logger
}

var _ Parser = &ORCParser{}

// todo: copied from orc.go
type fileReader struct {
	*os.File
}

// Size returns the size of the file in bytes.
func (f fileReader) Size() int64 {
	stats, err := f.Stat()
	if err != nil {
		return 0
	}
	return stats.Size()
}

func NewORCParser(logger log.Logger, reader storage.ReadSeekCloser, path string) (*ORCParser, error) {
	file, ok := reader.(*os.File)
	if !ok {
		panic("not implemented")
	}
	r, err := orc.NewReader(fileReader{
		File: file,
	})
	if err != nil {
		return nil, err
	}
	cursor := r.Select(r.Schema().Columns()...)
	return &ORCParser{
		reader: r,
		cursor: cursor,
		logger: logger,
	}, nil
}

func (p *ORCParser) Pos() (pos int64, rowID int64) {
	//TODO implement me
	return 0, 0
}

func (p *ORCParser) SetPos(pos int64, rowID int64) error {
	//TODO implement me
	return nil
}

func (p *ORCParser) ScannedPos() (int64, error) {
	//TODO implement me
	return 0, nil
}

func (p *ORCParser) Close() error {
	//TODO implement me
	if err := p.cursor.Close(); err != nil {
		// todo log error
	}
	return p.reader.Close()
}

func (p *ORCParser) ReadRow() error {
	//TODO implement me
	if p.cursor.Next() {
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
			if err := setORCDatumValue(&p.lastRow.Row[i], v, p.logger); err != nil {
				return err
			}
		}
	} else if p.cursor.Stripes() {
		if p.cursor.Next() {
			r := p.cursor.Row()
			length := len(p.Columns())
			p.lastRow.Row = make([]types.Datum, length)
			for i := 0; i < length; i++ {
				v := reflect.ValueOf(r[i])
				p.lastRow.Length += getDatumLen(v)
				if err := setORCDatumValue(&p.lastRow.Row[i], v, p.logger); err != nil {
					return err
				}
			}
		}
		// todo: empty stripe???
	} else if p.cursor.Err() == nil {
		return io.EOF
	}
	return p.cursor.Err()
}

func (p *ORCParser) LastRow() Row {
	//TODO implement me
	return p.lastRow
}

func (p *ORCParser) RecycleRow(row Row) {
	//TODO implement me
}

func (p *ORCParser) Columns() []string {
	//TODO implement me
	return p.reader.Schema().Columns()
}

func (p *ORCParser) SetColumns(strings []string) {
	//TODO implement me
}

func (p *ORCParser) SetLogger(logger log.Logger) {
	//TODO implement me
}

func (p *ORCParser) SetRowID(rowID int64) {
	//TODO implement me
}

// todo: copied from parquet parser for easy testing, need to refactor
func setORCDatumValue(d *types.Datum, v reflect.Value, logger log.Logger) error {
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
		d.SetInt64(v.Int())
	case reflect.String:
		// todo: collation
		d.SetString(v.String(), "")
	case reflect.Float32, reflect.Float64:
		d.SetFloat64(v.Float())
	case reflect.Ptr:
		if v.IsNil() {
			d.SetNull()
		} else {
			return setORCDatumValue(d, v.Elem(), logger)
		}
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			d.SetBytes(v.Bytes())
		} else {
			logger.Error("unknown value", zap.Stringer("kind", v.Kind()),
				zap.String("type", v.Type().Name()), zap.Reflect("value", v.Interface()))
			return errors.Errorf("unknown value: %v", v)
		}
	default:
		logger.Error("unknown value", zap.Stringer("kind", v.Kind()),
			zap.String("type", v.Type().Name()), zap.Reflect("value", v.Interface()))
		return errors.Errorf("unknown value: %v", v)
	}
	return nil
}
