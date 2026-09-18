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

package csvfile

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"

	"github.com/pingcap/tidb/pkg/dumpformat"
)

// appendField appends one field's CSV encoding to dst and returns the extended
// slice. val is the caller-produced raw value bytes; isNull selects NullValue.
func appendField(dst, val []byte, isNull bool, kind dumpformat.FieldKind, cfg *Config) []byte {
	if isNull {
		return append(dst, cfg.NullValue...)
	}
	if kind == dumpformat.KindNumber {
		return append(dst, val...)
	}
	dst = append(dst, cfg.FieldsEnclosedBy...)
	dst, _ = appendFieldBody(dst, val, len(val), kind, cfg)
	return append(dst, cfg.FieldsEnclosedBy...)
}

// appendFieldBody appends the part of an enclosed field between its enclosures
// for a prefix of val of about limit bytes, and returns how many bytes of val it
// encoded. The prefix never ends where encoding the rest separately would change
// the output: base64 pieces other than the last are a multiple of 3 bytes, and
// a piece never ends inside an enclosure that is doubled. Encoding val piece by
// piece therefore yields the same bytes as encoding it at once. limit must be at
// least 3 so that every piece makes progress.
func appendFieldBody(dst, val []byte, limit int, kind dumpformat.FieldKind, cfg *Config) ([]byte, int) {
	n := min(len(val), limit)
	if kind == dumpformat.KindBytes {
		switch cfg.BinaryFormat {
		case BinaryFormatHEX:
			return hex.AppendEncode(dst, val[:n]), n
		case BinaryFormatBase64:
			// Only the end of the whole value may be padded.
			if n < len(val) {
				n -= n % 3
			}
			return base64.StdEncoding.AppendEncode(dst, val[:n]), n
		}
	}
	return appendEscaped(dst, val, n, cfg)
}

// appendEscaped writes a prefix of s of about limit bytes to dst, escaping per
// cfg, and returns how many bytes of s it consumed.
func appendEscaped(dst, s []byte, limit int, cfg *Config) ([]byte, int) {
	switch {
	case len(cfg.FieldsEscapedBy) > 0:
		return appendEscapedBackslash(dst, s[:limit], cfg), limit
	case len(cfg.FieldsEnclosedBy) > 0:
		return appendDoubledEnclosure(dst, s, limit, []byte(cfg.FieldsEnclosedBy))
	default:
		return append(dst, s[:limit]...), limit
	}
}

// appendDoubledEnclosure doubles each occurrence of d (e.g. " -> "") in a prefix
// of s of about limit bytes, writing straight into dst to avoid the
// intermediate copy that bytes.ReplaceAll would make, and returns how many bytes
// of s it consumed. It matches occurrences left to right like bytes.ReplaceAll
// over the whole of s: a piece ends at limit only if no occurrence starts
// before limit, or else right after the last occurrence it doubled, so a
// multi-byte d is never split between pieces.
func appendDoubledEnclosure(dst, s []byte, limit int, d []byte) ([]byte, int) {
	// An occurrence that starts before limit ends before this.
	end := min(len(s), limit+len(d)-1)
	i := 0
	for i < limit {
		j := bytes.Index(s[i:end], d)
		if j < 0 || i+j >= limit {
			return append(dst, s[i:limit]...), limit
		}
		dst = append(dst, s[i:i+j]...)
		dst = append(dst, d...)
		dst = append(dst, d...)
		i += j + len(d)
	}
	return dst, i
}

// appendEscapedBackslash writes s to dst, escaping with cfg.FieldsEscapedBy
// (guaranteed non-empty by the caller). The escape byte and the enclosure (or,
// unquoted, the field separator) are escaped alongside NUL/CR/LF.
func appendEscapedBackslash(dst, s []byte, cfg *Config) []byte {
	esc := cfg.FieldsEscapedBy[0]
	var specCmt byte
	if len(cfg.FieldsEnclosedBy) > 0 {
		specCmt = cfg.FieldsEnclosedBy[0]
	} else if len(cfg.FieldsTerminatedBy) > 0 {
		specCmt = cfg.FieldsTerminatedBy[0]
	}
	last := 0
	for i := range s {
		var escape byte
		switch s[i] {
		case 0:
			escape = '0'
		case '\r':
			escape = 'r'
		case '\n':
			escape = 'n'
		default:
			if s[i] == esc || s[i] == specCmt {
				escape = s[i]
			}
		}
		if escape != 0 {
			dst = append(dst, s[last:i]...)
			dst = append(dst, esc, escape)
			last = i + 1
		}
	}
	return append(dst, s[last:]...)
}
