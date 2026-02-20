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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// Type returns type of BinaryJSON as string.
func (bj BinaryJSON) Type() string {
	switch bj.TypeCode {
	case JSONTypeCodeObject:
		return "OBJECT"
	case JSONTypeCodeArray:
		return "ARRAY"
	case JSONTypeCodeLiteral:
		switch bj.Value[0] {
		case JSONLiteralNil:
			return "NULL"
		default:
			return "BOOLEAN"
		}
	case JSONTypeCodeInt64:
		return "INTEGER"
	case JSONTypeCodeUint64:
		return "UNSIGNED INTEGER"
	case JSONTypeCodeFloat64:
		return "DOUBLE"
	case JSONTypeCodeString:
		return "STRING"
	case JSONTypeCodeOpaque:
		typ := bj.GetOpaqueFieldType()
		switch typ {
		case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
			return "BLOB"
		case mysql.TypeBit:
			return "BIT"
		default:
			return "OPAQUE"
		}
	case JSONTypeCodeDate:
		return "DATE"
	case JSONTypeCodeDatetime:
		return "DATETIME"
	case JSONTypeCodeTimestamp:
		return "DATETIME"
	case JSONTypeCodeDuration:
		return "TIME"
	default:
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, bj.TypeCode)
		panic(msg)
	}
}

// Unquote is for JSON_UNQUOTE.
func (bj BinaryJSON) Unquote() (string, error) {
	switch bj.TypeCode {
	case JSONTypeCodeString:
		str := string(hack.String(bj.GetString()))
		return UnquoteString(str)
	default:
		return bj.String(), nil
	}
}

// UnquoteString remove quotes in a string,
// including the quotes at the head and tail of string.
func UnquoteString(str string) (string, error) {
	strLen := len(str)
	if strLen < 2 {
		return str, nil
	}
	head, tail := str[0], str[strLen-1]
	if head == '"' && tail == '"' {
		// Remove prefix and suffix '"' before unquoting
		return unquoteJSONString(str[1 : strLen-1])
	}
	// if value is not double quoted, do nothing
	return str, nil
}

// unquoteJSONString recognizes the escape sequences shown in:
// https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#json-unquote-character-escape-sequences
func unquoteJSONString(s string) (string, error) {
	ret := new(bytes.Buffer)
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			if i == len(s) {
				return "", errors.New("Missing a closing quotation mark in string")
			}
			switch s[i] {
			case '"':
				ret.WriteByte('"')
			case 'b':
				ret.WriteByte('\b')
			case 'f':
				ret.WriteByte('\f')
			case 'n':
				ret.WriteByte('\n')
			case 'r':
				ret.WriteByte('\r')
			case 't':
				ret.WriteByte('\t')
			case '\\':
				ret.WriteByte('\\')
			case 'u':
				if i+4 > len(s) {
					return "", errors.Errorf("Invalid unicode: %s", s[i+1:])
				}
				char, size, inSurrogateRange, err := decodeOneEscapedUnicode(hack.Slice(s[i+1 : i+5]))
				if err != nil {
					// For `surrogate pair`, it uses two `\\uxxx` to encode a character.
					if inSurrogateRange && len(s) >= i+10 && s[i+5] == '\\' && s[i+6] == 'u' {
						char, size, _, err = decodeOneEscapedUnicode(append([]byte(s[i+1:i+5]), []byte(s[i+7:i+11])...))
						if err != nil {
							return "", errors.Trace(err)
						}
						ret.Write(char[0:size])
						i += 10
						continue
					}
					return "", errors.Trace(err)
				}
				ret.Write(char[0:size])
				i += 4
			default:
				// For all other escape sequences, backslash is ignored.
				ret.WriteByte(s[i])
			}
		} else {
			ret.WriteByte(s[i])
		}
	}
	return ret.String(), nil
}

// decodeOneEscapedUnicode decodes one unicode into utf8 bytes specified in RFC 3629.
// According RFC 3629, the max length of utf8 characters is 4 bytes.
// And MySQL use 4 bytes to represent the unicode which must be in [0, 65536).
func decodeOneEscapedUnicode(s []byte) (char [4]byte, size int, inSurrogateRange bool, err error) {
	if len(s) > 8 {
		return char, 0, false, errors.Errorf("Invalid `s` for decodeEscapedUnicode: %s", s)
	}
	size, err = hex.Decode(char[0:4], s)
	if err != nil {
		return char, 0, false, errors.Trace(err)
	}
	if size != 2 && size != 4 {
		// The unicode must can be represented in 2 bytes or 4 bytes.
		return char, size, false, errors.Errorf("Invalid unicode length: %d", size)
	}

	r1 := rune(binary.BigEndian.Uint16(char[0:2]))
	if size == 4 {
		r1 = utf16.DecodeRune(r1, rune(binary.BigEndian.Uint16(char[2:4])))
	}
	size = utf8.RuneLen(r1)
	if size < 0 {
		if r1 >= 0xD800 && r1 <= 0xDFFF {
			inSurrogateRange = true
		}
		return char, size, inSurrogateRange, errors.Errorf("Invalid unicode: %s", s)
	}
	utf8.EncodeRune(char[0:size], r1)
	return
}

// quoteJSONString escapes interior quote and other characters for json functions.
//
// The implementation of `JSON_QUOTE` doesn't use this function. The `JSON_QUOTE` used `goJSON` to encode the string
// directly. Therefore, this function is not compatible with `JSON_QUOTE` function for the convience of internal usage.
//
// This function will add extra quotes to the string if it contains special characters which needs to escape, or it's not
// a valid ECMAScript identifier.
func quoteJSONString(s string) string {
	var escapeByteMap = map[byte]string{
		'\\': "\\\\",
		'"':  "\\\"",
		'\b': "\\b",
		'\f': "\\f",
		'\n': "\\n",
		'\r': "\\r",
		'\t': "\\t",
	}

	ret := new(bytes.Buffer)
	ret.WriteByte('"')

	start := 0
	hasEscaped := false

	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			escaped, ok := escapeByteMap[b]
			if ok {
				if start < i {
					ret.WriteString(s[start:i])
				}
				hasEscaped = true
				ret.WriteString(escaped)
				i++
				start = i
			} else {
				i++
			}
		} else {
			c, size := utf8.DecodeRuneInString(s[i:])
			if c == utf8.RuneError && size == 1 { // refer to codes of `binary.jsonMarshalStringTo`
				if start < i {
					ret.WriteString(s[start:i])
				}
				hasEscaped = true
				ret.WriteString(`\ufffd`)
				i += size
				start = i
				continue
			}
			i += size
		}
	}

	if start < len(s) {
		ret.WriteString(s[start:])
	}

	if hasEscaped || !isEcmascriptIdentifier(s) {
		ret.WriteByte('"')
		return ret.String()
	}
	return ret.String()[1:]
}


