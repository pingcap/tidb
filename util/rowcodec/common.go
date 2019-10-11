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
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
)

// CodecVer is the constant number that represent the new row format.
const CodecVer = 128

var ErrInvalidCodecVer = errors.New("invalid codec version")

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag          byte = 0
	BytesFlag        byte = 1
	CompactBytesFlag byte = 2
	IntFlag          byte = 3
	UintFlag         byte = 4
	VarintFlag       byte = 8
	VaruintFlag      byte = 9
)

// row is the struct type used to access the a row.
type row struct {
	// small:  colID []byte, offsets []uint16, optimized for most cases.
	// large:  colID []uint32, offsets []uint32.
	large          bool
	numNotNullCols uint16
	numNullCols    uint16
	colIDs         []byte

	// valFlags is used for converting new row format to old row format.
	// It can be removed once TiDB implemented the new row format.
	valFlags []byte
	offsets  []uint16
	data     []byte

	// for large row
	colIDs32  []uint32
	offsets32 []uint32
}

// String implements the strings.Stringer interface.
func (r row) String() string {
	var colValStrs []string
	for i := 0; i < int(r.numNotNullCols); i++ {
		var colID, offStart, offEnd int64
		if r.large {
			colID = int64(r.colIDs32[i])
			if i != 0 {
				offStart = int64(r.offsets32[i-1])
			}
			offEnd = int64(r.offsets32[i])
		} else {
			colID = int64(r.colIDs[i])
			if i != 0 {
				offStart = int64(r.offsets[i-1])
			}
			offEnd = int64(r.offsets[i])
		}
		colValData := r.data[offStart:offEnd]
		valFlag := r.valFlags[i]
		var colValStr string
		if valFlag == BytesFlag {
			colValStr = fmt.Sprintf("(%d:'%s')", colID, colValData)
		} else {
			colValStr = fmt.Sprintf("(%d:%d)", colID, colValData)
		}
		colValStrs = append(colValStrs, colValStr)
	}
	return strings.Join(colValStrs, ",")
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

func (r *row) setRowData(rowData []byte) error {
	if rowData[0] != CodecVer {
		return ErrInvalidCodecVer
	}
	r.large = rowData[1]&1 > 0
	r.numNotNullCols = binary.LittleEndian.Uint16(rowData[2:])
	r.numNullCols = binary.LittleEndian.Uint16(rowData[4:])
	cursor := 6
	r.valFlags = rowData[cursor : cursor+int(r.numNotNullCols)]
	cursor += int(r.numNotNullCols)
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

func bytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

func bytes2U16Slice(b []byte) []uint16 {
	if len(b) == 0 {
		return nil
	}
	var u16s []uint16
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u16s))
	hdr.Len = len(b) / 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u16s
}

func u16SliceToBytes(u16s []uint16) []byte {
	if len(u16s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u16s) * 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u16s[0]))
	return b
}

func u32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func encodeInt(buf []byte, iVal int64) []byte {
	var tmp [8]byte
	if int64(int8(iVal)) == iVal {
		buf = append(buf, byte(iVal))
	} else if int64(int16(iVal)) == iVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(iVal))
		buf = append(buf, tmp[:2]...)
	} else if int64(int32(iVal)) == iVal {
		binary.LittleEndian.PutUint32(tmp[:], uint32(iVal))
		buf = append(buf, tmp[:4]...)
	} else {
		binary.LittleEndian.PutUint64(tmp[:], uint64(iVal))
		buf = append(buf, tmp[:8]...)
	}
	return buf
}

func decodeInt(val []byte) int64 {
	switch len(val) {
	case 1:
		return int64(int8(val[0]))
	case 2:
		return int64(int16(binary.LittleEndian.Uint16(val)))
	case 4:
		return int64(int32(binary.LittleEndian.Uint32(val)))
	default:
		return int64(binary.LittleEndian.Uint64(val))
	}
}

func encodeUint(buf []byte, uVal uint64) []byte {
	var tmp [8]byte
	if uint64(uint8(uVal)) == uVal {
		buf = append(buf, byte(uVal))
	} else if uint64(uint16(uVal)) == uVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(uVal))
		buf = append(buf, tmp[:2]...)
	} else if uint64(uint32(uVal)) == uVal {
		binary.LittleEndian.PutUint32(tmp[:], uint32(uVal))
		buf = append(buf, tmp[:4]...)
	} else {
		binary.LittleEndian.PutUint64(tmp[:], uint64(uVal))
		buf = append(buf, tmp[:8]...)
	}
	return buf
}

func decodeUint(val []byte) uint64 {
	switch len(val) {
	case 1:
		return uint64(val[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(val))
	case 4:
		return uint64(binary.LittleEndian.Uint32(val))
	default:
		return binary.LittleEndian.Uint64(val)
	}
}
