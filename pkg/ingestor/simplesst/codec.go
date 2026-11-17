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

package simplesst

import (
	"encoding/binary"
)

// RangeProperty stores properties of a range:
// - Key: the start key of the range.
// - Offset: the start offset of the range in the file.
// - Size: the size of the range.
// - Keys: the number of keys in the range.
type RangeProperty struct {
	FirstKey       []byte
	LastKey        []byte
	Offset         uint64
	Size           uint64
	Keys           uint64
	CompressedSize uint64 // v1: bytes occupied by this segment's zstd frame in the data file. 0 for v0 props.
}

// totalSize is the encoded raw size, including KV length headers.
func (r *RangeProperty) totalSize() uint64 { return r.Size + r.Keys*2*LengthBytes }

// decodeMultiProps is only used for test.
func decodeMultiProps(data []byte) []*RangeProperty {
	var ret []*RangeProperty
	for len(data) > 0 {
		propLen := int(binary.BigEndian.Uint32(data))
		propBytes := data[4 : 4+propLen]
		rp := decodeProp(propBytes)
		ret = append(ret, rp)
		data = data[4+propLen:]
	}
	return ret
}

func decodeProp(data []byte) *RangeProperty {
	rp := &RangeProperty{}
	n := 0
	keyLen := int(binary.BigEndian.Uint32(data[n : n+4]))
	n += 4
	rp.FirstKey = data[n : n+keyLen]
	n += keyLen
	keyLen = int(binary.BigEndian.Uint32(data[n : n+4]))
	n += 4
	rp.LastKey = data[n : n+keyLen]
	n += keyLen
	rp.Size = binary.BigEndian.Uint64(data[n : n+8])
	n += 8
	rp.Keys = binary.BigEndian.Uint64(data[n : n+8])
	n += 8
	rp.Offset = binary.BigEndian.Uint64(data[n : n+8])
	return rp
}

// keyLen * 2 + p.size + p.keys + p.offset
const propertyLengthExceptKeys = 4*2 + 8 + 8 + 8

func encodeMultiProps(buf []byte, props []*RangeProperty) []byte {
	var propLen [4]byte
	for _, p := range props {
		binary.BigEndian.PutUint32(propLen[:],
			uint32(propertyLengthExceptKeys+len(p.FirstKey)+len(p.LastKey)))
		buf = append(buf, propLen[:4]...)
		buf = encodeProp(buf, p)
	}
	return buf
}

func encodeProp(buf []byte, r *RangeProperty) []byte {
	var b [8]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(r.FirstKey)))
	buf = append(buf, b[:4]...)
	buf = append(buf, r.FirstKey...)
	binary.BigEndian.PutUint32(b[:], uint32(len(r.LastKey)))
	buf = append(buf, b[:4]...)
	buf = append(buf, r.LastKey...)
	binary.BigEndian.PutUint64(b[:], r.Size)
	buf = append(buf, b[:]...)
	binary.BigEndian.PutUint64(b[:], r.Keys)
	buf = append(buf, b[:]...)
	binary.BigEndian.PutUint64(b[:], r.Offset)
	buf = append(buf, b[:]...)
	return buf
}

// V1 stat property layout adds a trailing CompressedSize uint64:
//
//	[firstKeyLen u32][FirstKey][lastKeyLen u32][LastKey]
//	[Size u64][Keys u64][Offset u64][CompressedSize u64]
//
// In v1 stat files, prop.Offset means PHYSICAL byte Offset of the segment's
// zstd frame in the data file (including the 6-byte file header).
const propertyLengthExceptKeysV1 = propertyLengthExceptKeys + 8

func encodePropV1(buf []byte, r *RangeProperty) []byte {
	buf = encodeProp(buf, r)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], r.CompressedSize)
	buf = append(buf, b[:]...)
	return buf
}

func decodePropV1(data []byte) *RangeProperty {
	rp := decodeProp(data[:len(data)-8])
	rp.CompressedSize = binary.BigEndian.Uint64(data[len(data)-8:])
	return rp
}

// encodeMultiPropsV1 is the v1 counterpart to encodeMultiProps. It writes a
// length-prefixed sequence of v1 props (which carry the trailing
// CompressedSize field). Used by v1 writers when finalizing a stat file.
func encodeMultiPropsV1(buf []byte, props []*RangeProperty) []byte {
	var propLen [4]byte
	for _, p := range props {
		body := encodePropV1(nil, p)
		binary.BigEndian.PutUint32(propLen[:], uint32(len(body)))
		buf = append(buf, propLen[:4]...)
		buf = append(buf, body...)
	}
	return buf
}
