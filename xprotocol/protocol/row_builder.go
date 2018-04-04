// Copyright 2017 PingCAP, Inc.
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

package protocol

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/hack"
)

func dumpIntBinary(value int64) []byte {
	p := proto.NewBuffer([]byte{})
	// err == nil for ever, make errcheck happy.
	err := p.EncodeZigzag64(uint64(value))
	terror.Log(errors.Trace(err))
	return p.Bytes()
}

func dumpUintBinary(value uint64) []byte {
	p := proto.NewBuffer([]byte{})
	// error == nil for ever, make errcheck happy.
	err := p.EncodeVarint(uint64(value))
	terror.Log(errors.Trace(err))
	return p.Bytes()
}

func dumpStringBinary(b []byte, alloc arena.Allocator) []byte {
	data := alloc.Alloc(len(b) + 1)
	data = append(data, b...)
	data = append(data, byte(0))
	return data
}

func strToXDecimal(str string) ([]byte, error) {
	if len(str) == 0 {
		return nil, nil
	}

	// First byte stores the scale (number of digits after '.')
	// then all digits in BCD
	scale := 0
	dotPos := strings.Index(str, ".")
	slices := strings.Split(str, ".")

	if len(slices) > 2 || len(slices[0]) == 0 {
		return nil, errors.New("invalid decimal")
	}

	if dotPos != -1 {
		scale = len(str) - dotPos - 1
	}

	dec := []byte{byte(scale)}
	sign := 0xc
	if strings.HasPrefix(slices[0], "-") || strings.HasPrefix(slices[0], "+") {
		if strings.HasPrefix(slices[0], "-") {
			sign = 0xd
		}
		if len(slices[0]) == 1 {
			return nil, errors.New("invalid decimal")
		}
		slices[0] = slices[0][1:]
	}

	joined := ""
	for _, v := range slices {
		if _, err := strconv.Atoi(v); err != nil {
			return nil, errors.New("invalid decimal")
		}
		joined += v
	}

	// Append two char into one byte.
	// If joined[i+1] is the last char, stop the loop.
	// If joined[i+2] is the last char, stop the loop in the next loop after append sign.
	for i := 0; i < len(joined); i += 2 {
		if i == len(joined)-1 {
			// If it is the last char of joined, append like the following.
			dec = append(dec, byte((int(joined[i])-int('0'))<<4|sign))
			sign = 0
			break
		}
		dec = append(dec, byte((int(joined[i])-int('0'))<<4|(int(joined[i+1])-int('0'))))
	}

	if sign != 0 {
		dec = append(dec, byte(sign<<4))
	}
	return dec, nil
}

func dateTimeToXDateTime(t types.Time) (data []byte) {
	year, mon, day := t.Time.Year(), t.Time.Month(), t.Time.Day()
	if t.IsZero() {
		year, mon, day = 1, int(time.January), 1
	}
	data = append(data, dumpUintBinary(uint64(year))...)
	data = append(data, dumpUintBinary(uint64(mon))...)
	data = append(data, dumpUintBinary(uint64(day))...)
	if t.Type == mysql.TypeTimestamp || t.Type == mysql.TypeDatetime {
		data = append(data, dumpUintBinary(uint64(t.Time.Hour()))...)
		data = append(data, dumpUintBinary(uint64(t.Time.Minute()))...)
		data = append(data, dumpUintBinary(uint64(t.Time.Second()))...)
		if t.Time.Microsecond() != 0 {
			data = append(data, dumpUintBinary(uint64(t.Time.Microsecond()))...)
		}
	}
	return
}

func durationToXTime(d time.Duration) (data []byte) {
	if d < 0 {
		data = append(data, 0x1)
		d = -d
	} else {
		data = append(data, 0x0)
	}
	hours := d / time.Hour
	d -= hours * time.Hour
	minutes := d / time.Minute
	d -= minutes * time.Minute
	seconds := d / time.Second
	d -= seconds * time.Second
	data = append(data, dumpUintBinary(uint64(hours))...)
	data = append(data, dumpUintBinary(uint64(minutes))...)
	data = append(data, dumpUintBinary(uint64(seconds))...)
	if d != 0 {
		data = append(data, dumpUintBinary(uint64(d/time.Microsecond))...)
	}
	return
}

// DumpDatumToBinary converts type.Datum to X Protocol binary data.
func DumpDatumToBinary(alloc arena.Allocator, val types.Datum) ([]byte, error) {
	switch val.Kind() {
	case types.KindNull:
		return nil, nil
	case types.KindInt64:
		return dumpIntBinary(val.GetInt64()), nil
	case types.KindUint64, types.KindMysqlBit:
		return dumpUintBinary(val.GetUint64()), nil
	case types.KindFloat32:
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, math.Float32bits(val.GetFloat32()))
		return data, nil
	case types.KindFloat64:
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, math.Float64bits(val.GetFloat64()))
		return data, nil
	case types.KindString, types.KindBytes:
		return dumpStringBinary(val.GetBytes(), alloc), nil
	case types.KindMysqlDecimal:
		return strToXDecimal(val.GetMysqlDecimal().String())
	case types.KindMysqlTime:
		return dateTimeToXDateTime(val.GetMysqlTime()), nil
	case types.KindMysqlDuration:
		return durationToXTime(val.GetMysqlDuration().Duration), nil
	case types.KindMysqlSet:
		return dumpStringBinary(hack.Slice(val.GetMysqlSet().String()), alloc), nil
	case types.KindMysqlEnum:
		return dumpStringBinary(hack.Slice(val.GetMysqlEnum().String()), alloc), nil
	case types.KindMysqlJSON:
		return dumpStringBinary(hack.Slice(val.GetMysqlJSON().String()), alloc), nil
	case types.KindBinaryLiteral:
		return dumpStringBinary(hack.Slice(val.GetBinaryLiteral().ToString()), alloc), nil
	default:
		return nil, errors.Errorf("unknown datum type %d", val.Kind())
	}
}
