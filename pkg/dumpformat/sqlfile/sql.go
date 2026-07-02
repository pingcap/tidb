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
	"fmt"
)

// nullToken is the SQL NULL literal, matching dumpling's nullValue.
var nullToken = []byte("NULL")

// AppendValue appends one field's SQL encoding (a numeric literal, a quoted and
// escaped string, an x'..' hex literal, or NULL) to dst and returns the extended
// slice. val is the caller-produced raw value bytes; isNull selects NULL. It is
// the per-value framing used by SQLWriter, exported for callers that format a
// single value rather than a whole INSERT row (e.g. primary-key chunk bounds).
func AppendValue(dst, val []byte, isNull bool, kind FieldKind, escapeBackslash bool) []byte {
	if isNull {
		return append(dst, nullToken...)
	}
	switch kind {
	case KindNumber:
		return append(dst, val...)
	case KindBytes:
		return fmt.Appendf(dst, "x'%x'", val)
	default: // KindString
		dst = append(dst, '\'')
		dst = appendEscaped(dst, val, escapeBackslash)
		return append(dst, '\'')
	}
}

// appendEscaped mirrors dumpling's escapeSQL.
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

// appendEscapedBackslash mirrors dumpling's escapeBackslashSQL.
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
