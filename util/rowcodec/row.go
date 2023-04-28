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
)

const (
	rowFlagLarge byte = 1 << iota
	rowFlagChecksum
)

const (
	checksumMaskVersion byte = 0b0111
	checksumFlagExtra   byte = 0b1000
)

// row is the struct type used to access a row and the row format is shown as the following.
//
// Row Format
//
//	0               1               2               3
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|      VER      |     FLAGS     |        NOT_NULL_COL_CNT       |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|          NULL_COL_CNT         |     ...NOT_NULL_COL_IDS...    |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|       ...NULL_COL_IDS...      |   ...NOT_NULL_COL_OFFSETS...  |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                    ...NOT_NULL_COL_DATA...                    |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|         ...CHECKSUM...        |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
//	- FLAGS
//	  - 0x01: large (when max(col_ids) > 255 or len(col_data) > max_u16)
//	    - size of col_id     = large ? 4 : 1
//	    - size of col_offset = large ? 4 : 2
//	  - 0x02: has checksum
//
// Checksum
//
//	0               1               2               3               4               5               6               7               8
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|       |E| VER |                            CHECKSUM                           |                    EXTRA_CHECKSUM(OPTIONAL)                   |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	     HEADER
//
//	- HEADER
//	  - VER: version
//	  - E:   has extra checksum
//	- CHECKSUM
//	  - little-endian CRC32(IEEE) when hdr.ver = 0 (default)
type row struct {
	flags          byte
	checksumHeader byte
	numNotNullCols uint16
	numNullCols    uint16

	// for small row: colID []byte, offsets []uint16, optimized for most cases.
	colIDs  []byte
	offsets []uint16

	// for large row: colID []uint32, offsets []uint32.
	colIDs32  []uint32
	offsets32 []uint32

	data      []byte
	checksum1 uint32
	checksum2 uint32
}

func (r *row) large() bool { return r.flags&rowFlagLarge > 0 }

func (r *row) hasChecksum() bool { return r.flags&rowFlagChecksum > 0 }

func (r *row) hasExtraChecksum() bool { return r.checksumHeader&checksumFlagExtra > 0 }

func (r *row) setChecksums(checksums ...uint32) {
	if len(checksums) > 0 {
		r.flags |= rowFlagChecksum
		r.checksum1 = checksums[0]
		if len(checksums) > 1 {
			r.checksumHeader |= checksumFlagExtra
			r.checksum2 = checksums[1]
		}
	}
}

func (r *row) getData(i int) []byte {
	var start, end uint32
	if r.large() {
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
	r.flags = rowData[1]
	r.numNotNullCols = binary.LittleEndian.Uint16(rowData[2:])
	r.numNullCols = binary.LittleEndian.Uint16(rowData[4:])
	cursor := 6
	lastOffset := 0
	if r.large() {
		colIDsLen := int(r.numNotNullCols+r.numNullCols) * 4
		r.colIDs32 = bytesToU32Slice(rowData[cursor : cursor+colIDsLen])
		cursor += colIDsLen
		offsetsLen := int(r.numNotNullCols) * 4
		r.offsets32 = bytesToU32Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
		if n := len(r.offsets32); n > 0 {
			lastOffset = int(r.offsets32[n-1])
		}
	} else {
		colIDsLen := int(r.numNotNullCols + r.numNullCols)
		r.colIDs = rowData[cursor : cursor+colIDsLen]
		cursor += colIDsLen
		offsetsLen := int(r.numNotNullCols) * 2
		r.offsets = bytes2U16Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
		if n := len(r.offsets); n > 0 {
			lastOffset = int(r.offsets[n-1])
		}
	}
	r.data = rowData[cursor : cursor+lastOffset]
	cursor += lastOffset

	if r.hasChecksum() {
		r.checksumHeader = rowData[cursor]
		if r.ChecksumVersion() != 0 {
			return errInvalidChecksumVer
		}
		cursor++
		r.checksum1 = binary.LittleEndian.Uint32(rowData[cursor:])
		cursor += 4
		if r.hasExtraChecksum() {
			r.checksum2 = binary.LittleEndian.Uint32(rowData[cursor:])
		} else {
			r.checksum2 = 0
		}
	} else {
		r.checksumHeader = 0
		r.checksum1 = 0
		r.checksum2 = 0
	}
	return nil
}

func (r *row) toBytes(buf []byte) []byte {
	buf = append(buf, CodecVer)
	buf = append(buf, r.flags)
	buf = append(buf, byte(r.numNotNullCols), byte(r.numNotNullCols>>8))
	buf = append(buf, byte(r.numNullCols), byte(r.numNullCols>>8))
	if r.large() {
		buf = append(buf, u32SliceToBytes(r.colIDs32)...)
		buf = append(buf, u32SliceToBytes(r.offsets32)...)
	} else {
		buf = append(buf, r.colIDs...)
		buf = append(buf, u16SliceToBytes(r.offsets)...)
	}
	buf = append(buf, r.data...)
	if r.hasChecksum() {
		buf = append(buf, r.checksumHeader)
		buf = binary.LittleEndian.AppendUint32(buf, r.checksum1)
		if r.hasExtraChecksum() {
			buf = binary.LittleEndian.AppendUint32(buf, r.checksum2)
		}
	}
	return buf
}

func (r *row) findColID(colID int64) (idx int, isNil, notFound bool) {
	// Search the column in not-null columns array.
	i, j := 0, int(r.numNotNullCols)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if r.large() {
			v = int64(r.colIDs32[h])
		} else {
			v = int64(r.colIDs[h])
		}
		if v < colID {
			i = h + 1
		} else if v == colID {
			idx = h
			return
		} else {
			j = h
		}
	}

	// Search the column in null columns array.
	i, j = int(r.numNotNullCols), int(r.numNotNullCols+r.numNullCols)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if r.large() {
			v = int64(r.colIDs32[h])
		} else {
			v = int64(r.colIDs[h])
		}
		if v < colID {
			i = h + 1
		} else if v == colID {
			isNil = true
			return
		} else {
			j = h
		}
	}
	notFound = true
	return
}

// ChecksumVersion returns the version of checksum. Note that it's valid only if checksum has been encoded in the row
// value (callers can check it by `GetChecksum`).
func (r *row) ChecksumVersion() int { return int(r.checksumHeader & checksumMaskVersion) }

// GetChecksum returns the checksum of row data (not null columns).
func (r *row) GetChecksum() (uint32, bool) {
	if !r.hasChecksum() {
		return 0, false
	}
	return r.checksum1, true
}

// GetExtraChecksum returns the extra checksum which shall be calculated in the last stable schema version (whose
// elements are all public).
func (r *row) GetExtraChecksum() (uint32, bool) {
	if !r.hasExtraChecksum() {
		return 0, false
	}
	return r.checksum2, true
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
