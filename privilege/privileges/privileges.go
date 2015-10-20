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
	"fmt"
	"strings"

	"github.com/pingcap/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/util/sqlexec"
)

type PrivilegeInfo struct {
	mysql.PrivilegeType
	Wildcard bool
}

type PrivilegeCheck struct {
	username string
	host     string
	privs    map[int]map[mysql.PrivilegeType]bool
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

func (p *PrivilegeCheck) loadGlobalPrivileges() error {
	// TODO: or Host="%"
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND Host="%s";`, mysql.SystemDB, mysql.UserTable, p.username, p.host)
	rs, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer rs.Close()
	ps := make(map[mysql.PrivilegeType]bool)
	for {
		row, err := rs.Next()
		if err != nil {
			return errors.Trace(err)
		}
		fs := rs.GetFields()
		for i := 3; i < len(fs); i++ {
			f := fs[i]
			// check each priv field
		}
	}
	p.privs[coldef.GrantLevelGlobal] = ps
	return nil
}

func (p *PrivilegeCheck) loadDBScopePrivileges() error {
	return nil
}

func (p *PrivilegeCheck) loadTableScopePrivileges() error {
	return nil
}

func (p *PrivilegeCheck) loadPrivileges() error {
	p.privs = make(map[int]map[mysql.PrivilegeType]bool)
	// Load privileges from mysql.User/DB/Table_privs/Column_privs table
	err := p.loadGlobalPrivileges()
	if err != nil {
		return errors.Trace(err)
	}
	err = p.loadDBScopePrivileges()
	if err != nil {
		return errors.Trace(err)
	}
	err = p.loadTableScopePrivileges()
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: consider column scope privilege latter.
	return nil
}
