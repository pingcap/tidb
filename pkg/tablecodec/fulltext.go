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
// in the key. The value carries the length of the document and the term's
// positions in it:
//
//	value[0]     TailLen: 0, or 1 when the untouched flag is appended
//	value[1]     tikvFullTextValueKindDocLengthPositions
//	value[2:]    uvarint(document length), uvarint(count), then count uvarint
//	             deltas between successive positions; zero padding to at least
//	             10 bytes; optional flag
//
// The document length is the number of tokens the document analyzes to, the
// same in every entry of a row. Together with the term frequency, the count,
// it is what a relevance score such as BM25 needs from each posting, so a
// score can be computed from the postings a search reads without reading or
// analyzing the document. A row's entries are all rewritten whenever its
// document changes, so the length cannot go stale.
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
	// tikvFullTextValueKindDocLengthPositions marks a value carrying the
	// document length and the positions. Kind 0x01, positions without the
	// length, was a pre-release layout and is refused.
	tikvFullTextValueKindDocLengthPositions byte = 0x02
	// tikvFullTextValueMinLen keeps the value out of the old-layout length
	// range, so that decoders select the extensible layout.
	tikvFullTextValueMinLen = MaxOldEncodeValueLen + 1
)

// EncodeTiKVFullTextPayload appends the part of a FULLTEXT index value that
// depends on the document to buf: the document's length in tokens, and the
// term's positions in it, which must be ascending.
func EncodeTiKVFullTextPayload(buf []byte, docLength int, positions []int) []byte {
	buf = binary.AppendUvarint(buf, uint64(docLength))
	buf = binary.AppendUvarint(buf, uint64(len(positions)))
	prev := 0
	for _, pos := range positions {
		buf = binary.AppendUvarint(buf, uint64(pos-prev))
		prev = pos
	}
	return buf
}

// EncodeTiKVFullTextIndexValue builds the value of a FULLTEXT index entry from
// an encoded payload.
func EncodeTiKVFullTextIndexValue(buf []byte, payload []byte, untouched bool) []byte {
	buf = buf[:0]
	tailLen := byte(0)
	if untouched {
		tailLen = 1
	}
	buf = append(buf, tailLen, tikvFullTextValueKindDocLengthPositions)
	buf = append(buf, payload...)
	for len(buf) < tikvFullTextValueMinLen {
		buf = append(buf, 0)
	}
	if untouched {
		buf = append(buf, kv.UnCommitIndexKVFlag)
	}
	return buf
}

// tikvFullTextPayload returns the payload of the value of a FULLTEXT index
// entry, starting with the document length.
func tikvFullTextPayload(value []byte) ([]byte, error) {
	if len(value) < tikvFullTextValueMinLen {
		return nil, errors.Errorf("fulltext index value too short: %d bytes", len(value))
	}
	tailLen := int(value[0])
	if tailLen > 1 || value[1] != tikvFullTextValueKindDocLengthPositions {
		return nil, errors.Errorf("not a fulltext index value: tail %d, kind %d", tailLen, value[1])
	}
	return value[2 : len(value)-tailLen], nil
}

// DecodeTiKVFullTextDocLength returns the length in tokens of the document
// the value of a FULLTEXT index entry was written for, without decoding the
// positions.
func DecodeTiKVFullTextDocLength(value []byte) (int, error) {
	body, err := tikvFullTextPayload(value)
	if err != nil {
		return 0, err
	}
	docLength, n := binary.Uvarint(body)
	if n <= 0 {
		return 0, errors.New("fulltext index value has a malformed document length")
	}
	return int(docLength), nil
}

// DecodeTiKVFullTextIndexValue returns the positions stored in the value of a
// FULLTEXT index entry.
func DecodeTiKVFullTextIndexValue(value []byte) ([]int, error) {
	body, err := tikvFullTextPayload(value)
	if err != nil {
		return nil, err
	}
	_, n := binary.Uvarint(body)
	if n <= 0 {
		return nil, errors.New("fulltext index value has a malformed document length")
	}
	body = body[n:]
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
