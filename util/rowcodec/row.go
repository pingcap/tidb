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
	"encoding/binary"
)

// row is the struct type used to access the a row.
type row struct {
	// small:  colID []byte, offsets []uint16, optimized for most cases.
	// large:  colID []uint32, offsets []uint32.
	large          bool
	numNotNullCols uint16
	numNullCols    uint16
	colIDs         []byte

	offsets []uint16
	data    []byte

	// for large row
	colIDs32  []uint32
	offsets32 []uint32
}

func (r *row) getData(i int) []byte {
	var start, end uint32
	if r.large {
		if i > 0 {
			start = r.offsets32[i-1]
		}
		end = r.offsets32[i]
	} else {
		if i > 0 {
			start = uint32(r.offsets[i-1])
		}
		end = uint32(r.offsets[i])
	}
	return r.data[start:end]
}

func (r *row) fromBytes(rowData []byte) error {
	if rowData[0] != CodecVer {
		return errInvalidCodecVer
	}
	r.large = rowData[1]&1 > 0
	r.numNotNullCols = binary.LittleEndian.Uint16(rowData[2:])
	r.numNullCols = binary.LittleEndian.Uint16(rowData[4:])
	cursor := 6
	if r.large {
		colIDsLen := int(r.numNotNullCols+r.numNullCols) * 4
		r.colIDs32 = bytesToU32Slice(rowData[cursor : cursor+colIDsLen])
		cursor += colIDsLen
		offsetsLen := int(r.numNotNullCols) * 4
		r.offsets32 = bytesToU32Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
	} else {
		colIDsLen := int(r.numNotNullCols + r.numNullCols)
		r.colIDs = rowData[cursor : cursor+colIDsLen]
		cursor += colIDsLen
		offsetsLen := int(r.numNotNullCols) * 2
		r.offsets = bytes2U16Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
	}
	r.data = rowData[cursor:]
	return nil
}

func (r *row) toBytes(buf []byte) []byte {
	buf = append(buf, CodecVer)
	flag := byte(0)
	if r.large {
		flag = 1
	}
	buf = append(buf, flag)
	buf = append(buf, byte(r.numNotNullCols), byte(r.numNotNullCols>>8))
	buf = append(buf, byte(r.numNullCols), byte(r.numNullCols>>8))
	if r.large {
		buf = append(buf, u32SliceToBytes(r.colIDs32)...)
		buf = append(buf, u32SliceToBytes(r.offsets32)...)
	} else {
		buf = append(buf, r.colIDs...)
		buf = append(buf, u16SliceToBytes(r.offsets)...)
	}
	buf = append(buf, r.data...)
	return buf
}

func (r *row) findColID(colID int64) (idx int, isNil, notFound bool) {
	// Search the column in not-null columns array.
	i, j := 0, int(r.numNotNullCols)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if r.large {
			v = int64(r.colIDs32[h])
		} else {
			v = int64(r.colIDs[h])
		}
		if v < colID {
			i = h + 1
		} else if v > colID {
			j = h
		} else {
			idx = h
			return
		}
	}

	// Search the column in null columns array.
	i, j = int(r.numNotNullCols), int(r.numNotNullCols+r.numNullCols)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if r.large {
			v = int64(r.colIDs32[h])
		} else {
			v = int64(r.colIDs[h])
		}
		if v < colID {
			i = h + 1
		} else if v > colID {
			j = h
		} else {
			isNil = true
			return
		}
	}
	notFound = true
	return
}

// ColumnIsNull returns if the column value is null. Mainly used for count column aggregation.
// this method will used in unistore.
func (r *row) ColumnIsNull(rowData []byte, colID int64, defaultVal []byte) (bool, error) {
	err := r.fromBytes(rowData)
	if err != nil {
		return false, err
	}
	_, isNil, notFound := r.findColID(colID)
	if notFound {
		return defaultVal == nil, nil
	}
	return isNil, nil
}

func (r *row) initColIDs() {
	numCols := int(r.numNotNullCols + r.numNullCols)
	if cap(r.colIDs) >= numCols {
		r.colIDs = r.colIDs[:numCols]
	} else {
		r.colIDs = make([]byte, numCols)
	}
}

func (r *row) initColIDs32() {
	numCols := int(r.numNotNullCols + r.numNullCols)
	if cap(r.colIDs32) >= numCols {
		r.colIDs32 = r.colIDs32[:numCols]
	} else {
		r.colIDs32 = make([]uint32, numCols)
	}
}

func (r *row) initOffsets() {
	if cap(r.offsets) >= int(r.numNotNullCols) {
		r.offsets = r.offsets[:r.numNotNullCols]
	} else {
		r.offsets = make([]uint16, r.numNotNullCols)
	}
}

func (r *row) initOffsets32() {
	if cap(r.offsets32) >= int(r.numNotNullCols) {
		r.offsets32 = r.offsets32[:r.numNotNullCols]
	} else {
		r.offsets32 = make([]uint32, r.numNotNullCols)
	}
}
