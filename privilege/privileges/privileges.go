// Copyright 2015 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/privilege"
)

type PrivilegeCheck struct {
	username string
	host     string
	privs    map[int]*Privilege
}

func (p *PrivilegeCheck) SetUser(user string) {
	strs := strings.Split(user, "@")
	p.username, p.host = strs[0], strs[1]
}

func (p *PrivilegeCheck) CheckPrivilege(ctx context.Context, db *model.DBInfo, privilege mysql.PrivilegeType) (bool, error) {
	if privs == nil {
		// Lazy load
		err := loadPrivileges()
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	// Check global scope privileges
	// Check db scope privileges
}
func (p *PrivilegeCheck) CheckPrivilege(ctx context.Context, db *model.DBInfo, tbl *model.TableInfo, privilege mysql.PrivilegeType) (bool, error) {
	if privs == nil {
		// Lazy load
		err := loadPrivileges()
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	// Check global scope privileges
	// Check db scope privileges
	// Check table scope privileges
}

func (p *PrivilegeCheck) loadPrivileges() error {
	// Load privileges from mysql.User/DB/Table_privs/Column_privs table
	// select * from mysql.User where User="" and Host="" or User="" and Host="%"
	// select * from mysql.DB where User="" and Host=""
	// select * from mysql.tables_priv where User="" and Host=""
	return nil
}
