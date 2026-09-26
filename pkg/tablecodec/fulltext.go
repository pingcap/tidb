// Copyright 2026 PingCAP, Inc.
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

package tablecodec

import (
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
)

// A FULLTEXT index built in TiKV stores one entry per distinct term per row.
// The key is an ordinary non-unique index key over the term, so the handle is
// in the key. The value carries the term's positions in the document:
//
//	value[0]     TailLen: 0, or 1 when the untouched flag is appended
//	value[1]     tikvFullTextValueKindPositions
//	value[2:]    uvarint(count), then count uvarint deltas between successive
//	             positions; zero padding to at least 10 bytes; optional flag
//
// This is the extensible index-value layout with no recognised segments,
// which every existing decoder treats as ignorable trailing bytes: the handle
// still comes from the key, the value is never mistaken for a unique-index
// value or an old-layout value (it is always longer than 9 bytes), and the
// untouched flag is recognised exactly as it is for other index values. The
// kind byte stays outside the values the extensible layout reserves for its
// own segments (CommonHandleFlag, PartitionIDFlag, IndexVersionFlag and
// RestoreDataFlag), so nothing parses the payload as one of them.

const (
	tikvFullTextValueKindPositions byte = 0x01
	// tikvFullTextValueMinLen keeps the value out of the old-layout length
	// range, so that decoders select the extensible layout.
	tikvFullTextValueMinLen = MaxOldEncodeValueLen + 1
)

// EncodeTiKVFullTextPositions appends the encoded form of a term's positions,
// which must be ascending, to buf. It is the part of the value that depends on
// the document.
func EncodeTiKVFullTextPositions(buf []byte, positions []int) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(positions)))
	prev := 0
	for _, pos := range positions {
		buf = binary.AppendUvarint(buf, uint64(pos-prev))
		prev = pos
	}
	return buf
}

// EncodeTiKVFullTextIndexValue builds the value of a FULLTEXT index entry from
// encoded positions.
func EncodeTiKVFullTextIndexValue(buf []byte, encodedPositions []byte, untouched bool) []byte {
	buf = buf[:0]
	tailLen := byte(0)
	if untouched {
		tailLen = 1
	}
	buf = append(buf, tailLen, tikvFullTextValueKindPositions)
	buf = append(buf, encodedPositions...)
	for len(buf) < tikvFullTextValueMinLen {
		buf = append(buf, 0)
	}
	if untouched {
		buf = append(buf, kv.UnCommitIndexKVFlag)
	}
	return buf
}

// DecodeTiKVFullTextIndexValue returns the positions stored in the value of a
// FULLTEXT index entry.
func DecodeTiKVFullTextIndexValue(value []byte) ([]int, error) {
	if len(value) < tikvFullTextValueMinLen {
		return nil, errors.Errorf("fulltext index value too short: %d bytes", len(value))
	}
	tailLen := int(value[0])
	if tailLen > 1 || value[1] != tikvFullTextValueKindPositions {
		return nil, errors.Errorf("not a fulltext index value: tail %d, kind %d", tailLen, value[1])
	}
	body := value[2 : len(value)-tailLen]
	count, n := binary.Uvarint(body)
	if n <= 0 {
		return nil, errors.New("fulltext index value has a malformed position count")
	}
	body = body[n:]
	positions := make([]int, 0, count)
	pos := 0
	for range count {
		delta, n := binary.Uvarint(body)
		if n <= 0 {
			return nil, errors.New("fulltext index value has a malformed position")
		}
		body = body[n:]
		pos += int(delta)
		positions = append(positions, pos)
	}
	return positions, nil
}
