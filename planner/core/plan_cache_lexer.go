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

package core

import (
	"strconv"

	"github.com/pingcap/tidb/types"
)

func isNumber(c byte) bool {
	if c >= '0' && c <= '9' {
		return true
	}
	return false
}

func isString(c byte) bool {
	if c == '\'' || c == '"' {
		return true
	}
	return false
}

func isChar(c byte) bool {
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return true
	}
	return false
}

func isSchema(c byte) bool {
	// Incomplete
	if isNumber(c) || isChar(c) || c == '_' {
		return true
	}
	return false
}

// FastLexer ...
func FastLexer(sql string) (string, []types.Datum, bool) {
	sqlText := make([]byte, 0, len(sql))
	var constantParam []byte
	isStringStatus := false
	isNumberStatus := false
	isSchemaNameStatus := false
	var params []types.Datum

	for _, c := range []byte(sql) {
		if isNumberStatus {
			if isNumber(c) {
				constantParam = append(constantParam, c)
				continue
			} else {
				numConstantParam, err := strconv.ParseInt(string(constantParam), 10, 64)
				if err != nil {
					return "", nil, false
				}
				var numConst types.Datum
				numConst.SetInt64(numConstantParam)
				params = append(params, numConst)
				constantParam = constantParam[:0]
				sqlText = append(sqlText, '?')
				isNumberStatus = false
			}
		} else if isStringStatus {
			constantParam = append(constantParam, c)
			if isString(c) {
				stringConstParam := string(constantParam)
				var stringConst types.Datum
				// TODO: set the collation
				stringConst.SetString(stringConstParam, "utf8mb4_bin")
				constantParam = constantParam[:0]
				params = append(params, stringConst)
				sqlText = append(sqlText, '?')
				isStringStatus = false
			}
			continue
		} else if isSchemaNameStatus {
			if !isSchema(c) {
				isSchemaNameStatus = false
			}
		}
		if (isNumber(c) || c == '-') && !isSchemaNameStatus {
			isNumberStatus = true
			constantParam = append(constantParam, c)
		} else if isString(c) {
			isStringStatus = true
			constantParam = append(constantParam, c)
		} else {
			if isChar(c) {
				isSchemaNameStatus = true
			}
			sqlText = append(sqlText, c)
		}
	}
	if isStringStatus {
		return "", nil, false
	} else if isNumberStatus {
		numConstantParam, err := strconv.ParseInt(string(constantParam), 10, 64)
		if err != nil {
			return "", nil, false
		}
		var numConst types.Datum
		numConst.SetInt64(numConstantParam)
		params = append(params, numConst)
		constantParam = constantParam[:0]
		sqlText = append(sqlText, '?')
	}

	return string(sqlText), params, true
}
