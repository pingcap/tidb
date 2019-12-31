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

package privileges

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
)

var (
	errInvalidPrivilegeType = terror.ClassPrivilege.New(mysql.ErrInvalidPrivilegeType, mysql.MySQLErrName[mysql.ErrInvalidPrivilegeType])
	errNonexistingGrant     = terror.ClassPrivilege.New(mysql.ErrNonexistingGrant, mysql.MySQLErrName[mysql.ErrNonexistingGrant])
	errLoadPrivilege        = terror.ClassPrivilege.New(mysql.ErrLoadPrivilege, mysql.MySQLErrName[mysql.ErrLoadPrivilege])
)

func init() {
	privilegeMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrNonexistingGrant:     mysql.ErrNonexistingGrant,
		mysql.ErrLoadPrivilege:        mysql.ErrLoadPrivilege,
		mysql.ErrInvalidPrivilegeType: mysql.ErrInvalidPrivilegeType,
	}
	terror.ErrClassToMySQLCodes[terror.ClassPrivilege] = privilegeMySQLErrCodes
}
