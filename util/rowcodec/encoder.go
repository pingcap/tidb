// Copyright 2019 PingCAP, Inc.
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

package rowcodec

import (
	"math"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// Encoder is used to encode a row.
type Encoder struct {
	row
	tempColIDs []int64
	values     []types.Datum
	sc         *stmtctx.StatementContext
}

// NewEncoder creates a new Encoder with column IDs.
func NewEncoder(colIDs []int64, sc *stmtctx.StatementContext) *Encoder {
	return &Encoder{
		tempColIDs: colIDs,
		sc:         sc,
	}
}

func (encoder *Encoder) reset() {
	encoder.isLarge = false
	encoder.numNotNullCols = 0
	encoder.numNullCols = 0
	encoder.data = encoder.data[:0]
	encoder.values = encoder.values[:0]
}

// Encode encodes a row from a datums slice.
func (encoder *Encoder) Encode(values []types.Datum, buf []byte) ([]byte, error) {
	encoder.reset()
	encoder.values = append(encoder.values, values...)
	for i, colID := range encoder.tempColIDs {
		if colID > 255 {
			encoder.isLarge = true
		}
		if values[i].IsNull() {
			encoder.numNullCols++
		} else {
			encoder.numNotNullCols++
		}
	}
	return encoder.build(buf)
}

func (encoder *Encoder) build(buf []byte) ([]byte, error) {
	r := &encoder.row
	// Separate null and not-null column IDs.
	numCols := len(encoder.tempColIDs)
	nullIdx := numCols - int(r.numNullCols)
	notNullIdx := 0
	if r.isLarge {
		encoder.initColIDs32()
		encoder.initOffsets32()
	} else {
		encoder.initColIDs()
		encoder.initOffsets()
	}
	for i, colID := range encoder.tempColIDs {
		if encoder.values[i].IsNull() {
			if r.isLarge {
				r.colIDs32[nullIdx] = uint32(colID)
			} else {
				r.colIDs[nullIdx] = byte(colID)
			}
			nullIdx++
		} else {
			if r.isLarge {
				r.colIDs32[notNullIdx] = uint32(colID)
			} else {
				r.colIDs[notNullIdx] = byte(colID)
			}
			encoder.values[notNullIdx] = encoder.values[i]
			notNullIdx++
		}
	}
	if r.isLarge {
		largeNotNullSorter := (*largeNotNullSorter)(encoder)
		sort.Sort(largeNotNullSorter)
		if r.numNullCols > 0 {
			largeNullSorter := (*largeNullSorter)(encoder)
			sort.Sort(largeNullSorter)
		}
	} else {
		smallNotNullSorter := (*smallNotNullSorter)(encoder)
		sort.Sort(smallNotNullSorter)
		if r.numNullCols > 0 {
			smallNullSorter := (*smallNullSorter)(encoder)
			sort.Sort(smallNullSorter)
		}
	}
	for i := 0; i < notNullIdx; i++ {
		var err error
		r.data, err = encodeDatum(r.data, encoder.values[i], encoder.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(r.data) > math.MaxUint16 && !r.isLarge {
			// We need to convert the row to large row.
			encoder.initColIDs32()
			for j := 0; j < numCols; j++ {
				r.colIDs32[j] = uint32(r.colIDs[j])
			}
			encoder.initOffsets32()
			for j := 0; j <= i; j++ {
				r.offsets32[j] = uint32(r.offsets[j])
			}
			r.isLarge = true
		}
		if r.isLarge {
			r.offsets32[i] = uint32(len(r.data))
		} else {
			r.offsets[i] = uint16(len(r.data))
		}
	}
	buf = append(buf, CodecVer)
	flag := byte(0)
	if r.isLarge {
		flag = 1
	}
	buf = append(buf, flag)
	buf = append(buf, byte(r.numNotNullCols), byte(r.numNotNullCols>>8))
	buf = append(buf, byte(r.numNullCols), byte(r.numNullCols>>8))
	if r.isLarge {
		buf = append(buf, u32SliceToBytes(r.colIDs32)...)
		buf = append(buf, u32SliceToBytes(r.offsets32)...)
	} else {
		buf = append(buf, r.colIDs...)
		buf = append(buf, u16SliceToBytes(r.offsets)...)
	}
	buf = append(buf, r.data...)
	return buf, nil
}

func encodeDatum(buf []byte, d types.Datum, sc *stmtctx.StatementContext) ([]byte, error) {
	switch d.Kind() {
	case types.KindInt64:
		buf = encodeInt(buf, d.GetInt64())
	case types.KindUint64:
		buf = encodeUint(buf, d.GetUint64())
	case types.KindString, types.KindBytes:
		buf = append(buf, d.GetBytes()...)
	case types.KindFloat32, types.KindFloat64:
		buf = encodeUint(buf, uint64(math.Float64bits(d.GetFloat64())))
	case types.KindMysqlDecimal:
		var err error
		buf, err = codec.EncodeDecimal(buf, d.GetMysqlDecimal(), d.Length(), d.Frac())
		if terror.ErrorEqual(err, types.ErrTruncated) {
			err = sc.HandleTruncate(err)
		} else if terror.ErrorEqual(err, types.ErrOverflow) {
			err = sc.HandleOverflow(err, err)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	case types.KindMysqlTime:
		t := d.GetMysqlTime()
		// Encoding timestamp need to consider timezone.
		// If it's not in UTC, transform to UTC first.
		if t.Type == mysql.TypeTimestamp && sc.TimeZone != time.UTC {
			err := t.ConvertTimeZone(sc.TimeZone, time.UTC)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		v, err := t.ToPackedUint()
		if err != nil {
			return nil, errors.Trace(err)
		}
		buf = encodeUint(buf, v)
	case types.KindMysqlDuration:
		buf = encodeInt(buf, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlEnum:
		buf = encodeUint(buf, uint64(d.GetMysqlEnum().ToNumber()))
	case types.KindMysqlSet:
		buf = encodeUint(buf, uint64(d.GetMysqlSet().ToNumber()))
	case types.KindMysqlBit, types.KindBinaryLiteral:
		val, err := types.BinaryLiteral(d.GetBytes()).ToInt(sc)
		if err != nil {
			terror.Log(errors.Trace(err))
		}
		buf = encodeUint(buf, val)
	case types.KindMysqlJSON:
		j := d.GetMysqlJSON()
		buf = append(buf, j.TypeCode)
		buf = append(buf, j.Value...)
	default:
		return nil, errors.Errorf("unsupport encode type %d", d.Kind())
	}
	return buf, nil
}

func (encoder *Encoder) initColIDs() {
	numCols := int(encoder.numNotNullCols + encoder.numNullCols)
	if cap(encoder.colIDs) >= numCols {
		encoder.colIDs = encoder.colIDs[:numCols]
	} else {
		encoder.colIDs = make([]byte, numCols)
	}
}

func (encoder *Encoder) initColIDs32() {
	numCols := int(encoder.numNotNullCols + encoder.numNullCols)
	if cap(encoder.colIDs32) >= numCols {
		encoder.colIDs32 = encoder.colIDs32[:numCols]
	} else {
		encoder.colIDs32 = make([]uint32, numCols)
	}
}

func (encoder *Encoder) initOffsets() {
	if cap(encoder.offsets) >= int(encoder.numNotNullCols) {
		encoder.offsets = encoder.offsets[:encoder.numNotNullCols]
	} else {
		encoder.offsets = make([]uint16, encoder.numNotNullCols)
	}
}

func (encoder *Encoder) initOffsets32() {
	if cap(encoder.offsets32) >= int(encoder.numNotNullCols) {
		encoder.offsets32 = encoder.offsets32[:encoder.numNotNullCols]
	} else {
		encoder.offsets32 = make([]uint32, encoder.numNotNullCols)
	}
}

/*
	We define several sorters to avoid switch cost in sort functions.
*/

type largeNotNullSorter Encoder

func (s *largeNotNullSorter) Less(i, j int) bool {
	return s.colIDs32[i] < s.colIDs32[j]
}

func (s *largeNotNullSorter) Len() int {
	return int(s.numNotNullCols)
}

func (s *largeNotNullSorter) Swap(i, j int) {
	s.colIDs32[i], s.colIDs32[j] = s.colIDs32[j], s.colIDs32[i]
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

type smallNotNullSorter Encoder

func (s *smallNotNullSorter) Less(i, j int) bool {
	return s.colIDs[i] < s.colIDs[j]
}

func (s *smallNotNullSorter) Len() int {
	return int(s.numNotNullCols)
}

func (s *smallNotNullSorter) Swap(i, j int) {
	s.colIDs[i], s.colIDs[j] = s.colIDs[j], s.colIDs[i]
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

type smallNullSorter Encoder

func (s *smallNullSorter) Less(i, j int) bool {
	nullCols := s.colIDs[s.numNotNullCols:]
	return nullCols[i] < nullCols[j]
}

func (s *smallNullSorter) Len() int {
	return int(s.numNullCols)
}

func (s *smallNullSorter) Swap(i, j int) {
	nullCols := s.colIDs[s.numNotNullCols:]
	nullCols[i], nullCols[j] = nullCols[j], nullCols[i]
}

type largeNullSorter Encoder

func (s *largeNullSorter) Less(i, j int) bool {
	nullCols := s.colIDs32[s.numNotNullCols:]
	return nullCols[i] < nullCols[j]
}

func (s *largeNullSorter) Len() int {
	return int(s.numNullCols)
}

func (s *largeNullSorter) Swap(i, j int) {
	nullCols := s.colIDs32[s.numNotNullCols:]
	nullCols[i], nullCols[j] = nullCols[j], nullCols[i]
}
