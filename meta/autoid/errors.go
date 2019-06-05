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

package autoid

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
)

// Error instances.
var (
	ErrAutoincReadFailed = terror.ClassAutoid.New(mysql.ErrAutoincReadFailed, mysql.MySQLErrName[mysql.ErrAutoincReadFailed])
)

func init() {
	// Map error codes to mysql error codes.
	tableMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrAutoincReadFailed: mysql.ErrAutoincReadFailed,
	}
	terror.ErrClassToMySQLCodes[terror.ClassAutoid] = tableMySQLErrCodes
}
