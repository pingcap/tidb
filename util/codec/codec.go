// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"time"

	"bytes"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
)

const (
	nilFlag byte = iota
	bytesFlag
	intFlag
	uintFlag
	floatFlag
	decimalFlag
	durationFlag
)

// writeValues writes values and their type flags into a byte buffer. The flag
// is used for convert bytes back to values.
func writeValues(e Encoder, b *bytes.Buffer, vals ...interface{}) error {
	for _, val := range vals {
		switch v := val.(type) {
		case bool:
			e.WriteSingleByte(b, intFlag)
			if v {
				e.WriteInt(b, int64(1))
			} else {
				e.WriteInt(b, int64(0))
			}
		case int:
			e.WriteSingleByte(b, intFlag)
			e.WriteInt(b, int64(v))
		case int8:
			e.WriteSingleByte(b, intFlag)
			e.WriteInt(b, int64(v))
		case int16:
			e.WriteSingleByte(b, intFlag)
			e.WriteInt(b, int64(v))
		case int32:
			e.WriteSingleByte(b, intFlag)
			e.WriteInt(b, int64(v))
		case int64:
			e.WriteSingleByte(b, intFlag)
			e.WriteInt(b, int64(v))
		case uint:
			e.WriteSingleByte(b, uintFlag)
			e.WriteUint(b, uint64(v))
		case uint8:
			e.WriteSingleByte(b, uintFlag)
			e.WriteUint(b, uint64(v))
		case uint16:
			e.WriteSingleByte(b, uintFlag)
			e.WriteUint(b, uint64(v))
		case uint32:
			e.WriteSingleByte(b, uintFlag)
			e.WriteUint(b, uint64(v))
		case uint64:
			e.WriteSingleByte(b, uintFlag)
			e.WriteUint(b, uint64(v))
		case float32:
			e.WriteSingleByte(b, floatFlag)
			e.WriteFloat(b, float64(v))
		case float64:
			e.WriteSingleByte(b, floatFlag)
			e.WriteFloat(b, float64(v))
		case string:
			e.WriteSingleByte(b, bytesFlag)
			e.WriteBytes(b, []byte(v))
		case []byte:
			e.WriteSingleByte(b, bytesFlag)
			e.WriteBytes(b, []byte(v))
		case mysql.Time:
			e.WriteSingleByte(b, bytesFlag)
			e.WriteBytes(b, []byte(v.String()))
		case mysql.Duration:
			e.WriteSingleByte(b, durationFlag)
			e.WriteInt(b, int64(v.Duration))
		case mysql.Decimal:
			e.WriteSingleByte(b, decimalFlag)
			e.WriteDecimal(b, v)
		case mysql.Hex:
			e.WriteSingleByte(b, intFlag)
			e.WriteInt(b, int64(v.ToNumber()))
		case mysql.Bit:
			e.WriteSingleByte(b, uintFlag)
			e.WriteUint(b, uint64(v.ToNumber()))
		case mysql.Enum:
			e.WriteSingleByte(b, uintFlag)
			e.WriteUint(b, uint64(v.ToNumber()))
		case mysql.Set:
			e.WriteSingleByte(b, uintFlag)
			e.WriteUint(b, uint64(v.ToNumber()))
		case nil:
			e.WriteSingleByte(b, nilFlag)
		default:
			return errors.Errorf("unsupport encode type %T", val)
		}
	}
	return nil
}

func readValues(e Encoder, b *bytes.Buffer) ([]interface{}, error) {
	var vals []interface{}
	var v interface{}
	for b.Len() > 0 {
		flag, err := e.ReadSingleByte(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		switch flag {
		case intFlag:
			v, err = e.ReadInt(b)
		case uintFlag:
			v, err = e.ReadUint(b)
		case floatFlag:
			v, err = e.ReadFloat(b)
		case bytesFlag:
			v, err = e.ReadBytes(b)
		case decimalFlag:
			v, err = e.ReadDecimal(b)
		case durationFlag:
			var r int64
			r, err = e.ReadInt(b)
			if err == nil {
				// use max fsp, let outer to do round manually.
				v = mysql.Duration{Duration: time.Duration(r), Fsp: mysql.MaxFsp}
			}
		case nilFlag:
			v = nil
		default:
			err = errors.Errorf("invalid encoded key flag %v", flag)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	return vals, nil
}

func (e ascEncoder) Write(b *bytes.Buffer, v ...interface{}) error {
	return writeValues(e, b, v...)
}

func (e ascEncoder) Read(b *bytes.Buffer) ([]interface{}, error) {
	return readValues(e, b)
}

func (e descEncoder) Write(b *bytes.Buffer, v ...interface{}) error {
	return writeValues(e, b, v...)
}

func (e descEncoder) Read(b *bytes.Buffer) ([]interface{}, error) {
	return readValues(e, b)
}

func (e compactEncoder) Write(b *bytes.Buffer, v ...interface{}) error {
	return writeValues(e, b, v...)
}

func (e compactEncoder) Read(b *bytes.Buffer) ([]interface{}, error) {
	return readValues(e, b)
}
