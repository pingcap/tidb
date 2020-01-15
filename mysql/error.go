// Copyright 2020 PingCAP, Inc.
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
	"fmt"

	pmysql "github.com/pingcap/parser/mysql"
)

// NewErr generates a SQL error, with an error code and default format specifier defined in MySQLErrName.
func NewErr(errCode uint16, args ...interface{}) *pmysql.SQLError {
	e := &pmysql.SQLError{Code: errCode}

	if s, ok := MySQLState[errCode]; ok {
		e.State = s
	} else {
		e.State = DefaultMySQLState
	}

	if format, ok := MySQLErrName[errCode]; ok {
		e.Message = fmt.Sprintf(format, args...)
	} else {
		e.Message = fmt.Sprint(args...)
	}

	return e
}
