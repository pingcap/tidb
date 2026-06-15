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
	"fmt"
)

// appendField appends one field's CSV encoding to dst and returns the extended
// slice. val is the caller-produced raw value bytes; isNull selects NullValue.
func appendField(dst, val []byte, isNull bool, kind FieldKind, cfg *Config) []byte {
	if isNull {
		return append(dst, cfg.NullValue...)
	}
	switch kind {
	case KindNumber:
		return append(dst, val...)
	case KindBytes:
		dst = append(dst, cfg.Delimiter...)
		switch cfg.BinaryFormat {
		case BinaryFormatHEX:
			dst = fmt.Appendf(dst, "%x", val)
		case BinaryFormatBase64:
			dst = base64.StdEncoding.AppendEncode(dst, val)
		default:
			dst = appendEscaped(dst, val, cfg)
		}
		return append(dst, cfg.Delimiter...)
	default: // KindString
		dst = append(dst, cfg.Delimiter...)
		dst = appendEscaped(dst, val, cfg)
		return append(dst, cfg.Delimiter...)
	}
}

// appendEscaped mirrors dumpling's escapeCSV.
func appendEscaped(dst, s []byte, cfg *Config) []byte {
	switch {
	case cfg.EscapeBackslash:
		return appendEscapedBackslash(dst, s, cfg)
	case len(cfg.Delimiter) > 0:
		// Double the delimiter, e.g. " -> "".
		doubled := make([]byte, 0, 2*len(cfg.Delimiter))
		doubled = append(doubled, cfg.Delimiter...)
		doubled = append(doubled, cfg.Delimiter...)
		return append(dst, bytes.ReplaceAll(s, cfg.Delimiter, doubled)...)
	default:
		return append(dst, s...)
	}
}

// appendEscapedBackslash mirrors dumpling's escapeBackslashCSV.
func appendEscapedBackslash(dst, s []byte, cfg *Config) []byte {
	// With a delimiter, comment the delimiter byte; otherwise the separator byte.
	var specCmt byte
	if len(cfg.Delimiter) > 0 {
		specCmt = cfg.Delimiter[0]
	} else if len(cfg.Separator) > 0 {
		specCmt = cfg.Separator[0]
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
		case '\\':
			escape = '\\'
		case specCmt:
			escape = specCmt
		}
		if escape != 0 {
			dst = append(dst, s[last:i]...)
			dst = append(dst, '\\', escape)
			last = i + 1
		}
	}
	return append(dst, s[last:]...)
}
