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
		dst = append(dst, cfg.FieldsEnclosedBy...)
		switch cfg.BinaryFormat {
		case BinaryFormatHEX:
			dst = fmt.Appendf(dst, "%x", val)
		case BinaryFormatBase64:
			dst = base64.StdEncoding.AppendEncode(dst, val)
		default:
			dst = appendEscaped(dst, val, cfg)
		}
		return append(dst, cfg.FieldsEnclosedBy...)
	default:
		dst = append(dst, cfg.FieldsEnclosedBy...)
		dst = appendEscaped(dst, val, cfg)
		return append(dst, cfg.FieldsEnclosedBy...)
	}
}

// appendEscaped writes s to dst, escaping per cfg.
func appendEscaped(dst, s []byte, cfg *Config) []byte {
	switch {
	case len(cfg.FieldsEscapedBy) > 0:
		return appendEscapedBackslash(dst, s, cfg)
	case len(cfg.FieldsEnclosedBy) > 0:
		// Double each enclosure occurrence (e.g. " -> ""), writing straight into
		// dst to avoid the intermediate copy that bytes.ReplaceAll would make.
		d := []byte(cfg.FieldsEnclosedBy)
		for {
			j := bytes.Index(s, d)
			if j < 0 {
				return append(dst, s...)
			}
			dst = append(dst, s[:j]...)
			dst = append(dst, d...)
			dst = append(dst, d...)
			s = s[j+len(d):]
		}
	default:
		return append(dst, s...)
	}
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
