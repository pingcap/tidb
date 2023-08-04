// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

// rangeProperty describes some statistic of a range of a file.
type rangeProperty struct {
	key      []byte // the first key in the range
	offset   uint64 // the end offset of the range
	writerID int
	dataSeq  int
	size     uint64 // total KV size in the range, not considering the file format layout overhead
	keys     uint64 // total KV count in the range
}

// rangePropertiesCollector collects range properties for each range. The zero
// value of rangePropertiesCollector is not ready to use, should call reset()
// first.
type rangePropertiesCollector struct {
	props               []*rangeProperty
	currProp            *rangeProperty
	propSizeIdxDistance uint64
	propKeysIdxDistance uint64
}

func (rc *rangePropertiesCollector) reset() {
	rc.props = rc.props[:0]
	rc.currProp = &rangeProperty{}
}

// keyLen + p.size + p.keys + p.offset + p.WriterID + p.DataSeq
const propertyLengthExceptKey = 4 + 8 + 8 + 8 + 4 + 4

// encode encodes rc.props to a byte slice.
func (rc *rangePropertiesCollector) encode() []byte {
	b := make([]byte, 0, 1024)
	idx := 0
	for _, p := range rc.props {
		// Size.
		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(propertyLengthExceptKey+len(p.key)))
		idx += 4

		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(len(p.key)))
		idx += 4
		b = append(b, p.key...)
		idx += len(p.key)

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.size)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.keys)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.offset)
		idx += 8

		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(p.writerID))
		idx += 4

		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(p.dataSeq))
		idx += 4
	}
	return b
}
