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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// Encoder is used to encode a row.
type Encoder struct {
	row
	tempColIDs []int64
	values     []types.Datum
	tempData   []byte
}

func (encoder *Encoder) reset() {
	encoder.large = false
	encoder.numNotNullCols = 0
	encoder.numNullCols = 0
	encoder.data = encoder.data[:0]
	encoder.tempColIDs = encoder.tempColIDs[:0]
	encoder.values = encoder.values[:0]
}

func (encoder *Encoder) addColumn(colID int64, d types.Datum) {
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

// Encode encodes a row from a datums slice.
func (encoder *Encoder) Encode(colIDs []int64, values []types.Datum, buf []byte) ([]byte, error) {
	encoder.reset()
	for i, colID := range colIDs {
		encoder.addColumn(colID, values[i])
	}
	return encoder.build(buf[:0])
}

// EncodeFromOldRow encodes a row from an old-format row.
func (encoder *Encoder) EncodeFromOldRow(oldRow, buf []byte) ([]byte, error) {
	encoder.reset()
	for len(oldRow) > 1 {
		var d types.Datum
		var err error
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		colID := d.GetInt64()
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		encoder.addColumn(colID, d)
	}
	return encoder.build(buf[:0])
}

func (encoder *Encoder) build(buf []byte) ([]byte, error) {
	r := &encoder.row
	// Separate null and not-null column IDs.
	numCols := len(encoder.tempColIDs)
	nullIdx := numCols - int(r.numNullCols)
	notNullIdx := 0
	if r.large {
		encoder.initColIDs32()
		encoder.initOffsets32()
	} else {
		encoder.initColIDs()
		encoder.initOffsets()
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
	encoder.initValFlags()
	for i := 0; i < notNullIdx; i++ {
		d := encoder.values[i]
		switch d.Kind() {
		case types.KindInt64:
			r.valFlags[i] = IntFlag
			r.data = encodeInt(r.data, d.GetInt64())
		case types.KindUint64:
			r.valFlags[i] = UintFlag
			r.data = encodeUint(r.data, d.GetUint64())
		case types.KindString, types.KindBytes:
			r.valFlags[i] = BytesFlag
			r.data = append(r.data, d.GetBytes()...)
		default:
			var err error
			encoder.tempData, err = codec.EncodeValue(defaultStmtCtx, encoder.tempData[:0], d)
			if err != nil {
				return nil, errors.Trace(err)
			}
			r.valFlags[i] = encoder.tempData[0]
			r.data = append(r.data, encoder.tempData[1:]...)
		}
		if len(r.data) > math.MaxUint16 && !r.large {
			// We need to convert the row to large row.
			encoder.initColIDs32()
			for j := 0; j < numCols; j++ {
				r.colIDs32[j] = uint32(r.colIDs[j])
			}
			encoder.initOffsets32()
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
	if !r.large {
		if len(r.data) >= math.MaxUint16 {
			r.large = true
			encoder.initColIDs32()
			for i, val := range r.colIDs {
				r.colIDs32[i] = uint32(val)
			}
		} else {
			encoder.initOffsets()
			for i, val := range r.offsets32 {
				r.offsets[i] = uint16(val)
			}
		}
	}
	buf = append(buf, CodecVer)
	flag := byte(0)
	if r.large {
		flag = 1
	}
	buf = append(buf, flag)
	buf = append(buf, byte(r.numNotNullCols), byte(r.numNotNullCols>>8))
	buf = append(buf, byte(r.numNullCols), byte(r.numNullCols>>8))
	buf = append(buf, r.valFlags...)
	if r.large {
		buf = append(buf, u32SliceToBytes(r.colIDs32)...)
		buf = append(buf, u32SliceToBytes(r.offsets32)...)
	} else {
		buf = append(buf, r.colIDs...)
		buf = append(buf, u16SliceToBytes(r.offsets)...)
	}
	buf = append(buf, r.data...)
	return buf, nil
}

func (encoder *Encoder) initValFlags() {
	if cap(encoder.valFlags) >= int(encoder.numNotNullCols) {
		encoder.valFlags = encoder.valFlags[:encoder.numNotNullCols]
	} else {
		encoder.valFlags = make([]byte, encoder.numNotNullCols)
	}
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

var defaultStmtCtx = &stmtctx.StatementContext{
	TimeZone: time.Local,
}

const (
	// Length of rowkey.
	rowKeyLen = 19
	// Index of record flag 'r' in rowkey used by master tidb-server.
	// The rowkey format is t{8 bytes id}_r{8 bytes handle}
	recordPrefixIdx = 10
	// Index of record flag 'r' in rowkey whit shard byte.
	shardedRecordPrefixIdx = 1
)

func IsRowKeyWithShardByte(key []byte) bool {
	return len(key) == rowKeyLen && key[0] == 't' && key[shardedRecordPrefixIdx] == 'r'
}

func IsRowKey(key []byte) bool {
	return len(key) == rowKeyLen && key[0] == 't' && key[recordPrefixIdx] == 'r'
}

// IsNewFormat checks whether row data is in new-format.
func IsNewFormat(rowData []byte) bool {
	if len(rowData) == 0 {
		return true
	}
	return rowData[0] == CodecVer
}

// RowToOldRow converts a row to old-format row.
func RowToOldRow(rowData, buf []byte) ([]byte, error) {
	if len(rowData) == 0 || !IsNewFormat(rowData) {
		return rowData, nil
	}
	buf = buf[:0]
	var r row
	err := r.setRowData(rowData)
	if err != nil {
		return nil, err
	}
	if !r.large {
		for i, colID := range r.colIDs {
			buf = append(buf, VarintFlag)
			buf = codec.EncodeVarint(buf, int64(colID))
			if i < int(r.numNotNullCols) {
				val := r.getData(i)
				switch r.valFlags[i] {
				case BytesFlag:
					buf = append(buf, CompactBytesFlag)
					buf = codec.EncodeCompactBytes(buf, val)
				case IntFlag:
					buf = append(buf, VarintFlag)
					buf = codec.EncodeVarint(buf, decodeInt(val))
				case UintFlag:
					buf = append(buf, VaruintFlag)
					buf = codec.EncodeUvarint(buf, decodeUint(val))
				default:
					buf = append(buf, r.valFlags[i])
					buf = append(buf, val...)
				}
			} else {
				buf = append(buf, NilFlag)
			}
		}
	} else {
		for i, colID := range r.colIDs32 {
			buf = append(buf, VarintFlag)
			buf = codec.EncodeVarint(buf, int64(colID))
			if i < int(r.numNotNullCols) {
				val := r.getData(i)
				switch r.valFlags[i] {
				case BytesFlag:
					buf = append(buf, CompactBytesFlag)
					buf = codec.EncodeCompactBytes(buf, val)
				case IntFlag:
					buf = append(buf, VarintFlag)
					buf = codec.EncodeVarint(buf, decodeInt(val))
				case UintFlag:
					buf = append(buf, VaruintFlag)
					buf = codec.EncodeUvarint(buf, decodeUint(val))
				default:
					buf = append(buf, r.valFlags[i])
					buf = append(buf, val...)
				}
			} else {
				buf = append(buf, NilFlag)
			}
		}
	}
	if len(buf) == 0 {
		buf = append(buf, NilFlag)
	}
	return buf, nil
}
