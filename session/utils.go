// Copyright 2021 PingCAP, Inc.
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

package session

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
)

func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// EscapeBytesBackslash will escape []byte into the buffer, with backslash.
func EscapeBytesBackslash(buf []byte, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// EscapeStringBackslash will escape string into the buffer, with backslash.
func EscapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// EscapeSQL will escape input arguments into the sql string, doing necessary processing.
// But it does not prevent you from doing EscapeSQL("select '%?", ";SQL injection!;") => "select '';SQL injection!;'".
// It is still your responsibility to write safe SQL.
func EscapeSQL(sql string, args ...interface{}) (string, error) {
	holders := strings.Count(sql, "%?")
	if holders == 0 {
		return sql, nil
	} else if holders > len(args) {
		return "", errors.Errorf("missing arguments, have %d specifiers, and %d arguments", holders, len(args))
	}
	buf := make([]byte, 0, len(sql))
	argPos := 0
	for i := 0; i < len(sql); i += 2 {
		q := strings.Index(sql[i:], "%?")
		if q == -1 {
			buf = append(buf, sql[i:]...)
			break
		}
		buf = append(buf, sql[i:i+q]...)
		i += q
		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int8:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint8:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint16:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint32:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 32)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				buf = append(buf, '\'')
				buf = v.AppendFormat(buf, "2006-01-02 15:04:05")
				buf = append(buf, '\'')
			}
		case json.RawMessage:
			buf = append(buf, '\'')
			buf = EscapeBytesBackslash(buf, v)
			buf = append(buf, '\'')
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				buf = EscapeBytesBackslash(buf, v)
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			buf = EscapeStringBackslash(buf, v)
			buf = append(buf, '\'')
		case []string:
			buf = append(buf, '(')
			for i, k := range v {
				if i > 0 {
					buf = append(buf, ',')
				}
				buf = append(buf, '\'')
				buf = EscapeStringBackslash(buf, k)
				buf = append(buf, '\'')
			}
			buf = append(buf, ')')
		default:
			return "", errors.Errorf("unsupported %d-th argument: %v", argPos, arg)
		}
	}
	return string(buf), nil
}
