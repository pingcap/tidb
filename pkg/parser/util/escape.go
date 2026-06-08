// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

// UnescapeChar returns the unescaped byte(s) for a MySQL backslash escape
// sequence. Given the byte after the backslash, it returns the replacement
// byte(s). For most escapes this is a single byte; for \% and \_ both the
// backslash and the character are preserved.
//
// See https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
//
//	\" \' \\ \n \0 \b \Z \r \t ==> escape to one char
//	\% \_ ==> preserve both chars
//	other ==> remove backslash
func UnescapeChar(b byte) []byte {
	switch b {
	case 'n':
		return []byte{'\n'}
	case '0':
		return []byte{0}
	case 'b':
		return []byte{8}
	case 'Z':
		return []byte{26}
	case 'r':
		return []byte{'\r'}
	case 't':
		return []byte{'\t'}
	case '%', '_':
		return []byte{'\\', b}
	default:
		return []byte{b}
	}
}
