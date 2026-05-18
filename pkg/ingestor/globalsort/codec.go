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

package globalsort

import (
	"encoding/binary"
)

// RangeProperty stores properties of a range:
// - Key: the start key of the range.
// - Offset: the start offset of the range in the file.
// - Size: the size of the range.
// - Keys: the number of keys in the range.
type RangeProperty struct {
	FirstKey []byte
	LastKey  []byte
	Offset   uint64
	Size     uint64
	Keys     uint64
}

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
