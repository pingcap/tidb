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

package external

import (
	"encoding/binary"
)

type rangeProperty struct {
	key    []byte
	offset uint64
	size   uint64
	keys   uint64
}

// decodeMultiProps is only used for test.
func decodeMultiProps(data []byte) []*rangeProperty {
	var ret []*rangeProperty
	for len(data) > 0 {
		propLen := int(binary.BigEndian.Uint32(data))
		propBytes := data[4 : 4+propLen]
		rp := decodeProp(propBytes)
		ret = append(ret, rp)
		data = data[4+propLen:]
	}
	return ret
}

func decodeProp(data []byte) *rangeProperty {
	rp := &rangeProperty{}
	keyLen := binary.BigEndian.Uint32(data[0:4])
	rp.key = data[4 : 4+keyLen]
	rp.size = binary.BigEndian.Uint64(data[4+keyLen : 12+keyLen])
	rp.keys = binary.BigEndian.Uint64(data[12+keyLen : 20+keyLen])
	rp.offset = binary.BigEndian.Uint64(data[20+keyLen : 28+keyLen])
	return rp
}

// keyLen + p.size + p.keys + p.offset
const propertyLengthExceptKey = 4 + 8 + 8 + 8

func encodeMultiProps(buf []byte, props []*rangeProperty) []byte {
	var propLen [4]byte
	for _, p := range props {
		binary.BigEndian.PutUint32(propLen[:],
			uint32(propertyLengthExceptKey+len(p.key)))
		buf = append(buf, propLen[:4]...)
		buf = encodeProp(buf, p)
	}
	return buf
}

func encodeProp(buf []byte, r *rangeProperty) []byte {
	var b [8]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(r.key)))
	buf = append(buf, b[:4]...)
	buf = append(buf, r.key...)
	binary.BigEndian.PutUint64(b[:], r.size)
	buf = append(buf, b[:]...)
	binary.BigEndian.PutUint64(b[:], r.keys)
	buf = append(buf, b[:]...)
	binary.BigEndian.PutUint64(b[:], r.offset)
	buf = append(buf, b[:]...)
	return buf
}
