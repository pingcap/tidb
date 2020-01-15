// Copyright 2019 PingCAP, Inc.
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

package mysql

import (
	"strings"

	pmysql "github.com/pingcap/parser/mysql"
)

// GetSQLMode gets the sql mode for string literal. SQL_mode is a list of different modes separated by commas.
// The input string must be formatted by 'FormatSQLModeStr'
func GetSQLMode(s string) (pmysql.SQLMode, error) {
	strs := strings.Split(s, ",")
	var sqlMode pmysql.SQLMode
	for i, length := 0, len(strs); i < length; i++ {
		mode, ok := pmysql.Str2SQLMode[strs[i]]
		if !ok && strs[i] != "" {
			return sqlMode, newInvalidModeErr(strs[i])
		}
		sqlMode = sqlMode | mode
	}
	return sqlMode, nil
}

func newInvalidModeErr(s string) error {
	return NewErr(ErrWrongValueForVar, "sql_mode", s)
}
