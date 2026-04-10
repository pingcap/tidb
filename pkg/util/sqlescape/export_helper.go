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

package sqlescape

import (
	"io"
	"strings"
)

// ExportedReserveBuffer is exported version of reserveBuffer
func ExportedReserveBuffer(buf []byte, appendSize int) []byte {
	return reserveBuffer(buf, appendSize)
}

// ExportedEscapeBytesBackslash is exported version of escapeBytesBackslash
func ExportedEscapeBytesBackslash(buf []byte, v []byte) []byte {
	return escapeBytesBackslash(buf, v)
}

// ExportedEscapeStringBackslash is exported version of escapeStringBackslash
func ExportedEscapeStringBackslash(buf []byte, v string) []byte {
	return escapeStringBackslash(buf, v)
}

// ExportedEscapeSQL is exported version of escapeSQL
func ExportedEscapeSQL(sql string, args ...any) ([]byte, error) {
	return escapeSQL(sql, args...)
}

// ExportedEscape is exported version of Escape (if exists)
func ExportedEscape(sql string, args ...any) ([]byte, error) {
	return escapeSQL(sql, args...)
}

// ExportedFormatSQL is exported version of FormatSQL
func ExportedFormatSQL(w io.Writer, sql string, args ...any) error {
	return FormatSQL(w, sql, args...)
}

// ExportedMustEscapeSQL is exported version of MustEscapeSQL
func ExportedMustEscapeSQL(sql string, args ...any) string {
	return MustEscapeSQL(sql, args...)
}

// ExportedMustFormatSQL is exported version of MustFormatSQL
func ExportedMustFormatSQL(w *strings.Builder, sql string, args ...any) {
	MustFormatSQL(w, sql, args...)
}
