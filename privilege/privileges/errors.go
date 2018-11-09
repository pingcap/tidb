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

// privilege error codes.
const (
	codeInvalidPrivilegeType  terror.ErrCode = 1
	codeInvalidUserNameFormat                = 2
	codeErrNonexistingGrant                  = mysql.ErrNonexistingGrant
)

var (
	errInvalidPrivilegeType  = terror.ClassPrivilege.New(codeInvalidPrivilegeType, "unknown privilege type")
	errInvalidUserNameFormat = terror.ClassPrivilege.New(codeInvalidUserNameFormat, "wrong username format")
	errNonexistingGrant      = terror.ClassPrivilege.New(codeErrNonexistingGrant, mysql.MySQLErrName[mysql.ErrNonexistingGrant])
)

func init() {
	privilegeMySQLErrCodes := map[terror.ErrCode]uint16{
		codeErrNonexistingGrant: mysql.ErrNonexistingGrant,
	}
	terror.ErrClassToMySQLCodes[terror.ClassPrivilege] = privilegeMySQLErrCodes
}
