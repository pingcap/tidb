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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rowcodec

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/multierr"
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
// `buf` is not truncated before encoding.
// This function may return both a valid encoded bytes and an error (actually `"pingcap/errors".ErrorGroup`). If the caller
// expects to handle these errors according to `SQL_MODE` or other configuration, please refer to `pkg/errctx`.
// the caller needs to ensure the key is not nil if checksum is required.
func (encoder *Encoder) Encode(loc *time.Location, colIDs []int64, values []types.Datum, checksum Checksum, buf []byte) ([]byte, error) {
	encoder.reset()
	encoder.appendColVals(colIDs, values)
	numCols, notNullIdx := encoder.reformatCols()
	err := encoder.encodeRowCols(loc, numCols, notNullIdx)
	if err != nil {
		return nil, err
	}
	if checksum == nil {
		checksum = NoChecksum{}
	}
	return checksum.encode(encoder, buf)
}

func (encoder *Encoder) reset() {
	encoder.flags = 0
	encoder.numNotNullCols = 0
	encoder.numNullCols = 0
	encoder.data = encoder.data[:0]
	encoder.tempColIDs = encoder.tempColIDs[:0]
	encoder.values = encoder.values[:0]
	encoder.offsets32 = encoder.offsets32[:0]
	encoder.offsets = encoder.offsets[:0]
	encoder.checksumHeader = 0
	encoder.checksum1 = 0
	encoder.checksum2 = 0
}

func (encoder *Encoder) appendColVals(colIDs []int64, values []types.Datum) {
	for i, colID := range colIDs {
		encoder.appendColVal(colID, &values[i])
	}
}

func (encoder *Encoder) appendColVal(colID int64, d *types.Datum) {
	if colID > 255 {
		encoder.flags |= rowFlagLarge
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
	if r.large() {
		r.initColIDs32()
		r.initOffsets32()
	} else {
		r.initColIDs()
		r.initOffsets()
	}
	for i, colID := range encoder.tempColIDs {
		if encoder.values[i].IsNull() {
			if r.large() {
				r.colIDs32[nullIdx] = uint32(colID)
			} else {
				r.colIDs[nullIdx] = byte(colID)
			}
			nullIdx++
		} else {
			if r.large() {
				r.colIDs32[notNullIdx] = uint32(colID)
			} else {
				r.colIDs[notNullIdx] = byte(colID)
			}
			encoder.values[notNullIdx] = encoder.values[i]
			notNullIdx++
		}
	}
	if r.large() {
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

func (encoder *Encoder) encodeRowCols(loc *time.Location, numCols, notNullIdx int) error {
	r := &encoder.row
	var errs error
	for i := range notNullIdx {
		d := encoder.values[i]
		var err error
		r.data, err = encodeValueDatum(loc, d, r.data)
		if err != nil {
			errs = multierr.Append(errs, err)
		}
		// handle convert to large
		if len(r.data) > math.MaxUint16 && !r.large() {
			r.initColIDs32()
			for j := range numCols {
				r.colIDs32[j] = uint32(r.colIDs[j])
			}
			r.initOffsets32()
			for j := 0; j <= i; j++ {
				r.offsets32[j] = uint32(r.offsets[j])
			}
			r.flags |= rowFlagLarge
		}
		if r.large() {
			r.offsets32[i] = uint32(len(r.data))
		} else {
			r.offsets[i] = uint16(len(r.data))
		}
	}
	return errs
}

// encodeValueDatum encodes one row datum entry into bytes.
// due to encode as value, this method will flatten value type like tablecodec.flatten
func encodeValueDatum(loc *time.Location, d *types.Datum, buffer []byte) (nBuffer []byte, err error) {
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
		if t.Type() == mysql.TypeTimestamp && loc != nil && loc != time.UTC {
			err = t.ConvertTimeZone(loc, time.UTC)
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
		val, err = d.GetBinaryLiteral().ToInt(types.StrictContext)
		if err != nil {
			return
		}
		buffer = encodeUint(buffer, val)
	case types.KindFloat32, types.KindFloat64:
		buffer = codec.EncodeFloat(buffer, d.GetFloat64())
	case types.KindMysqlDecimal:
		buffer, err = codec.EncodeDecimal(buffer, d.GetMysqlDecimal(), d.Length(), d.Frac())
	case types.KindMysqlJSON:
		j := d.GetMysqlJSON()
		buffer = append(buffer, j.TypeCode)
		buffer = append(buffer, j.Value...)
	case types.KindVectorFloat32:
		v := d.GetVectorFloat32()
		buffer = v.SerializeTo(buffer)
	default:
		err = errors.Errorf("unsupport encode type %d", d.Kind())
	}
	nBuffer = buffer
	return
}

// Checksum is used to calculate and append checksum data into the raw bytes
type Checksum interface {
	encode(encoder *Encoder, buf []byte) ([]byte, error)
}

// NoChecksum indicates no checksum is encoded into the returned raw bytes.
type NoChecksum struct{}

func (NoChecksum) encode(encoder *Encoder, buf []byte) ([]byte, error) {
	encoder.flags &^= rowFlagChecksum // revert checksum flag
	return encoder.toBytes(buf), nil
}

const checksumVersionRaw byte = 1

// RawChecksum indicates encode the raw bytes checksum and append it to the raw bytes.
type RawChecksum struct {
	Handle kv.Handle
}

func (c RawChecksum) encode(encoder *Encoder, buf []byte) ([]byte, error) {
	encoder.flags |= rowFlagChecksum
	encoder.checksumHeader &^= checksumFlagExtra   // revert extra checksum flag
	encoder.checksumHeader &^= checksumMaskVersion // revert checksum version
	encoder.checksumHeader |= checksumVersionRaw   // set checksum version
	valueBytes := encoder.toBytes(buf)
	valueBytes = append(valueBytes, encoder.checksumHeader)
	encoder.checksum1 = crc32.Checksum(valueBytes, crc32.IEEETable)
	encoder.checksum1 = crc32.Update(encoder.checksum1, crc32.IEEETable, c.Handle.Encoded())
	valueBytes = binary.LittleEndian.AppendUint32(valueBytes, encoder.checksum1)
	return valueBytes, nil
}
