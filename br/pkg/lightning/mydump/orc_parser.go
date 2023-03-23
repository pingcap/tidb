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
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
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

func NewORCParser(logger log.Logger, path string) (*ORCParser, error) {
	r, err := orc.Open(path)
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
	if p.cursor.Stripes() {
		if p.cursor.Next() {
			v := reflect.ValueOf(p.cursor.Row())
			length := len(p.Columns())
			p.lastRow.Row = make([]types.Datum, length)
			for i := 0; i < length; i++ {
				p.lastRow.Length += getDatumLen(v.Field(i))
				if err := setORCDatumValue(&p.lastRow.Row[i], v.Field(i), p.logger); err != nil {
					return err
				}
			}
		}
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
	default:
		logger.Error("unknown value", zap.Stringer("kind", v.Kind()),
			zap.String("type", v.Type().Name()), zap.Reflect("value", v.Interface()))
		return errors.Errorf("unknown value: %v", v)
	}
	return nil
}
