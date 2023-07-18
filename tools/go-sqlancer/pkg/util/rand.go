// Copyright 2022 PingCAP, Inc.
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

package util

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/tidb/parser/ast"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Rd same to rand.Intn
func Rd(n int) int {
	return rand.Intn(n)
}

// RdInt63 is to rand int64
func RdInt63(n int64) int64 {
	return rand.Int63n(n)
}

// RdRange rand int in range
func RdRange(n, m int64) int64 {
	if n == m {
		return n
	}
	if m < n {
		n, m = m, n
	}
	return n + rand.Int63n(m-n)
}

// RdInt64 rand int64
func RdInt64() int64 {
	if Rd(2) == 1 {
		return rand.Int63()
	}
	return -rand.Int63() - 1
}

// RdFloat64 rand float64
func RdFloat64() float64 {
	return rand.Float64()
}

// RdDate rand date
func RdDate() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 1, 0, time.UTC).Unix()
	max := time.Date(2100, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// RdTimestamp return same format as RdDate except rand range
// TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07'
func RdTimestamp() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// RdString rand string with given length. TODO: support rand multi-byte utf8
func RdString(length int) string {
	res := ""
	for i := 0; i < length; i++ {
		charCode := RdRange(33, 127)
		// char '\' and '"' should be escaped
		if charCode == 92 || charCode == 34 {
			charCode++
			// res = fmt.Sprintf("%s%s", res, "\\")
		}
		res = fmt.Sprintf("%s%s", res, string(rune(charCode)))
	}
	return res
}

// RdStringChar rand string with given length, letter chars only
func RdStringChar(length int) string {
	res := ""
	for i := 0; i < length; i++ {
		charCode := RdRange(97, 123)
		res = fmt.Sprintf("%s%s", res, string(rune(charCode)))
	}
	return res
}

// RdType rand data type
func RdType() string {
	switch Rd(6) {
	case 0:
		return "varchar"
	case 1:
		return "text"
	case 2:
		return "timestamp"
	case 3:
		return "datetime"
	}
	return "int"
}

// RdDataLen rand data with given type
func RdDataLen(t string) int64 {
	switch t {
	case "int":
		return RdRange(8, 20)
	case "varchar":
		return RdRange(255, 2047)
	case "float":
		return RdRange(16, 64)
	case "timestamp":
		return -1
	case "datetime":
		return -1
	case "text":
		return -1
	}
	return 10
}

// RdColumnOptions for rand column option with given type
func RdColumnOptions(t string) (options []ast.ColumnOptionType) {
	if Rd(3) == 0 {
		options = append(options, ast.ColumnOptionNotNull)
	} else if Rd(2) == 0 {
		options = append(options, ast.ColumnOptionNull)
	}
	switch t {
	case "varchar", "timestamp", "datetime", "int":
		if Rd(2) == 0 {
			options = append(options, ast.ColumnOptionDefaultValue)
		}
	}
	return
}

// RdCharset rand charset
func RdCharset() string {
	return "utf8"
}

// RdBool is to random boolean value
func RdBool() bool {
	return Rd(2) == 0
}
