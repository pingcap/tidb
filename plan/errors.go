// Copyright 2018 PingCAP, Inc.
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

package plan

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

var (
	// ErrWrongUsage is returned when SQL operators are not properly used.
	ErrWrongUsage = terror.ClassParser.New(codeWrongUsage, mysql.MySQLErrName[mysql.ErrWrongUsage])
	errInternal   = terror.ClassParser.New(codeInternal, mysql.MySQLErrName[mysql.ErrInternal])
)

const (
	codeWrongUsage = terror.ErrCode(mysql.ErrWrongUsage)
	codeInternal   = terror.ErrCode(mysql.ErrInternal)
)

func init() {
	typesMySQLErrCodes := map[terror.ErrCode]uint16{
		codeWrongUsage: mysql.ErrWrongUsage,
		codeInternal:   mysql.ErrInternal,
	}
	terror.ErrClassToMySQLCodes[terror.ClassParser] = typesMySQLErrCodes
}
