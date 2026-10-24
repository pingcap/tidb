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

package sqlfile

import (
	"bytes"
	"encoding/hex"

	"github.com/pingcap/tidb/pkg/dumpformat"
)

// nullToken is the SQL NULL literal written for NULL fields.
var nullToken = []byte("NULL")

// AppendValue appends one field's SQL encoding (a numeric literal, a quoted and
// escaped string, an x'..' hex literal, or NULL) to dst and returns the extended
// slice. val is the caller-produced raw value bytes; isNull selects NULL. It is
// the per-value framing used by Writer, exported for callers that format a
// single value rather than a whole INSERT row (e.g. primary-key chunk bounds).
func AppendValue(dst, val []byte, isNull bool, kind dumpformat.FieldKind, escapeBackslash bool) []byte {
	if isNull {
		return append(dst, nullToken...)
	}
	if kind == dumpformat.KindNumber {
		return append(dst, val...)
	}
	dst = appendOpenQuote(dst, kind)
	dst, _ = appendQuotedBody(dst, val, len(val), kind, escapeBackslash)
	return append(dst, '\'')
}

// appendOpenQuote appends the opening of a quoted value: x' for binary values
// and ' for strings. Both are closed by a single quote.
func appendOpenQuote(dst []byte, kind dumpformat.FieldKind) []byte {
	if kind == dumpformat.KindBytes {
		dst = append(dst, 'x')
	}
	return append(dst, '\'')
}

// appendQuotedBody appends the part of a quoted value between its quotes (hex
// digits for binary values, the escaped text for strings) for the first
// min(len(val), limit) bytes of val, and returns how many bytes it encoded.
// Every input byte is encoded on its own, so any split yields the same bytes.
func appendQuotedBody(dst, val []byte, limit int, kind dumpformat.FieldKind, escapeBackslash bool) ([]byte, int) {
	n := min(len(val), limit)
	if kind == dumpformat.KindBytes {
		return hex.AppendEncode(dst, val[:n]), n
	}
	return appendEscaped(dst, val[:n], escapeBackslash), n
}

// appendEscaped writes s to dst, escaping per escapeBackslash.
func appendEscaped(dst, s []byte, escapeBackslash bool) []byte {
	if escapeBackslash {
		return appendEscapedBackslash(dst, s)
	}
	// Double each single quote (' -> ''), writing straight into dst to avoid the
	// intermediate copy that bytes.ReplaceAll would make.
	for {
		i := bytes.IndexByte(s, '\'')
		if i < 0 {
			return append(dst, s...)
		}
		dst = append(dst, s[:i]...)
		dst = append(dst, '\'', '\'')
		s = s[i+1:]
	}
}

// appendEscapedBackslash writes s to dst with backslash escaping.
func appendEscapedBackslash(dst, s []byte) []byte {
	last := 0
	for i := range s {
		var escape byte
		switch s[i] {
		case 0: // Must be escaped for 'mysql'
			escape = '0'
		case '\n': // Must be escaped for logs
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"': // Better safe than sorry
			escape = '"'
		case '\032': // This gives problems on Win32
			escape = 'Z'
		}
		if escape != 0 {
			dst = append(dst, s[last:i]...)
			dst = append(dst, '\\', escape)
			last = i + 1
		}
	}
	return append(dst, s[last:]...)
}
