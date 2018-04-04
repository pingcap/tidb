// Copyright 2017 PingCAP, Inc.
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

import (
	"unicode"
)

// QuoteIdentifier quotes identifier with "`".
func QuoteIdentifier(str string) string {
	return "`" + str + "`"
}

// QuoteIdentifierIfNeeded quotes identifier if needed.
func QuoteIdentifierIfNeeded(str string) string {
	needQuote := false
	if len(str) > 0 && (unicode.IsLetter(rune(str[0])) || rune(str[0]) == '_') {
		for _, r := range str[1:] {
			if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' {
				needQuote = true
				break
			}
		}
	} else {
		needQuote = true
	}

	if needQuote {
		return QuoteIdentifier(str)
	}
	return str
}

// QuoteString quotes string with "'".
func QuoteString(str string) string {
	return "'" + str + "'"
}
