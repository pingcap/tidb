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
	values     []*types.Datum
	// Enable indicates whether this encoder should be use.
	Enable bool
}

// Encode encodes a row from a datums slice.
func (encoder *Encoder) Encode(sc *stmtctx.StatementContext, colIDs []int64, values []types.Datum, buf []byte) ([]byte, error) {
	encoder.reset()
	encoder.appendColVals(colIDs, values)
	numCols, notNullIdx := encoder.reformatCols()
	err := encoder.encodeRowCols(sc, numCols, notNullIdx)
	if err != nil {
		return nil, err
	}
	return encoder.row.toBytes(buf[:0]), nil
}

func (encoder *Encoder) reset() {
	encoder.large = false
	encoder.numNotNullCols = 0
	encoder.numNullCols = 0
	encoder.data = encoder.data[:0]
	encoder.tempColIDs = encoder.tempColIDs[:0]
	encoder.values = encoder.values[:0]
	encoder.offsets32 = encoder.offsets32[:0]
	encoder.offsets = encoder.offsets[:0]
}

func (encoder *Encoder) appendColVals(colIDs []int64, values []types.Datum) {
	for i, colID := range colIDs {
		encoder.appendColVal(colID, &values[i])
	}
}

func (encoder *Encoder) appendColVal(colID int64, d *types.Datum) {
	if colID > 255 {
		encoder.large = true
	}
	if d.IsNull() {
		encoder.numNullCols++
	} else {
		encoder.numNotNullCols++
	}
	encoder.tempColIDs = append(encoder.tempColIDs, colID)
	encoder.values = append(encoder.values, d)
}

func (encoder *Encoder) reformatCols() (numCols, notNullIdx int) {
	r := &encoder.row
	numCols = len(encoder.tempColIDs)
	nullIdx := numCols - int(r.numNullCols)
	notNullIdx = 0
	if r.large {
		r.initColIDs32()
		r.initOffsets32()
	} else {
		r.initColIDs()
		r.initOffsets()
	}
	for i, colID := range encoder.tempColIDs {
		if encoder.values[i].IsNull() {
			if r.large {
				r.colIDs32[nullIdx] = uint32(colID)
			} else {
				r.colIDs[nullIdx] = byte(colID)
			}
			nullIdx++
		} else {
			if r.large {
				r.colIDs32[notNullIdx] = uint32(colID)
			} else {
				r.colIDs[notNullIdx] = byte(colID)
			}
			encoder.values[notNullIdx] = encoder.values[i]
			notNullIdx++
		}
	}
	if r.large {
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
	return
}

func (encoder *Encoder) encodeRowCols(sc *stmtctx.StatementContext, numCols, notNullIdx int) error {
	r := &encoder.row
	for i := 0; i < notNullIdx; i++ {
		d := encoder.values[i]
		var err error
		r.data, err = encodeValueDatum(sc, d, r.data)
		if err != nil {
			return err
		}
		// handle convert to large
		if len(r.data) > math.MaxUint16 && !r.large {
			r.initColIDs32()
			for j := 0; j < numCols; j++ {
				r.colIDs32[j] = uint32(r.colIDs[j])
			}
			r.initOffsets32()
			for j := 0; j <= i; j++ {
				r.offsets32[j] = uint32(r.offsets[j])
			}
			r.large = true
		}
		if r.large {
			r.offsets32[i] = uint32(len(r.data))
		} else {
			r.offsets[i] = uint16(len(r.data))
		}
	}
	return nil
}

// encodeValueDatum encodes one row datum entry into bytes.
// due to encode as value, this method will flatten value type like tablecodec.flatten
func encodeValueDatum(sc *stmtctx.StatementContext, d *types.Datum, buffer []byte) (nBuffer []byte, err error) {
	switch d.Kind() {
	case types.KindInt64:
		buffer = encodeInt(buffer, d.GetInt64())
	case types.KindUint64:
		buffer = encodeUint(buffer, d.GetUint64())
	case types.KindString, types.KindBytes:
		buffer = append(buffer, d.GetBytes()...)
	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		t := d.GetMysqlTime()
		if t.Type() == mysql.TypeTimestamp && sc != nil && sc.TimeZone != time.UTC {
			err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
			if err != nil {
				return
			}
		}
		var v uint64
		v, err = t.ToPackedUint()
		if err != nil {
			return
		}
		buffer = encodeUint(buffer, v)
	case types.KindMysqlDuration:
		buffer = encodeInt(buffer, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlEnum:
		buffer = encodeUint(buffer, d.GetMysqlEnum().Value)
	case types.KindMysqlSet:
		buffer = encodeUint(buffer, d.GetMysqlSet().Value)
	case types.KindBinaryLiteral, types.KindMysqlBit:
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		var val uint64
		val, err = d.GetBinaryLiteral().ToInt(sc)
		if err != nil {
			return
		}
		buffer = encodeUint(buffer, val)
	case types.KindFloat32, types.KindFloat64:
		buffer = codec.EncodeFloat(buffer, d.GetFloat64())
	case types.KindMysqlDecimal:
		buffer, err = codec.EncodeDecimal(buffer, d.GetMysqlDecimal(), d.Length(), d.Frac())
		if err != nil && sc != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				err = sc.HandleTruncate(err)
			} else if terror.ErrorEqual(err, types.ErrOverflow) {
				err = sc.HandleOverflow(err, err)
			}
		}
	case types.KindMysqlJSON:
		j := d.GetMysqlJSON()
		buffer = append(buffer, j.TypeCode)
		buffer = append(buffer, j.Value...)
	default:
		err = errors.Errorf("unsupport encode type %d", d.Kind())
	}
	nBuffer = buffer
	return
}
