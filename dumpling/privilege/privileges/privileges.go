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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/sqlexec"
)

var _ privilege.Checker = (*UserPrivileges)(nil)

type privileges struct {
	privs map[mysql.PrivilegeType]bool
}

func (ps *privileges) contain(p mysql.PrivilegeType) bool {
	if ps.privs == nil {
		return false
	}
	_, ok := ps.privs[p]
	return ok
}

func (ps *privileges) add(p mysql.PrivilegeType) {
	if ps.privs == nil {
		ps.privs = make(map[mysql.PrivilegeType]bool)
	}
	ps.privs[p] = true
}

type userPrivileges struct {
	// Global privileges
	GlobalPrivs *privileges
	// DBName-privileges
	DBPrivs map[string]*privileges
	// DBName-TableName-privileges
	TablePrivs map[string]map[string]*privileges
}

// UserPrivileges implements privilege.Checker interface.
// This is used to check privilege for the current user.
type UserPrivileges struct {
	username string
	host     string
	privs    *userPrivileges
}

// Check implements Checker.Check interface.
func (p *UserPrivileges) Check(ctx context.Context, db *model.DBInfo, tbl *model.TableInfo, privilege mysql.PrivilegeType) (bool, error) {
	if p.privs == nil {
		// Lazy load
		err := p.loadPrivileges(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	// Check global scope privileges.
	ok := p.privs.GlobalPrivs.contain(privilege)
	if ok {
		return true, nil
	}
	// Check db scope privileges.
	dbp, ok := p.privs.DBPrivs[db.Name.O]
	if ok {
		ok = dbp.contain(privilege)
		if ok {
			return true, nil
		}
	}
	if tbl == nil {
		return false, nil
	}
	// Check table scope privileges.
	dbTbl, ok := p.privs.TablePrivs[db.Name.O]
	if !ok {
		return false, nil
	}
	tblp, ok := dbTbl[tbl.Name.O]
	if !ok {
		return false, nil
	}
	return tblp.contain(privilege), nil
}

func (p *UserPrivileges) loadPrivileges(ctx context.Context) error {
	user := variable.GetSessionVars(ctx).User
	strs := strings.Split(user, "@")
	p.username, p.host = strs[0], strs[1]
	p.privs = &userPrivileges{}
	// Load privileges from mysql.User/DB/Table_privs/Column_privs table
	err := p.loadGlobalPrivileges(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = p.loadDBScopePrivileges(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = p.loadTableScopePrivileges(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: consider column scope privilege latter.
	return nil
}

// mysql.User/mysql.DB table privilege columns start from index 3.
// See: booststrap.go CreateUserTable/CreateDBPrivTable
const userTablePrivColumnStartIndex = 3
const dbTablePrivColumnStartIndex = 3

func (p *UserPrivileges) loadGlobalPrivileges(ctx context.Context) error {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND (Host="%s" OR Host="%%");`, mysql.SystemDB, mysql.UserTable, p.username, p.host)
	rs, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer rs.Close()
	ps := &privileges{}
	fs, err := rs.Fields()
	if err != nil {
		return errors.Trace(err)
	}
	for {
		row, err := rs.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		for i := userTablePrivColumnStartIndex; i < len(fs); i++ {
			d := row.Data[i]
			ed, ok := d.(mysql.Enum)
			if !ok {
				return errors.Errorf("Privilege should be mysql.Enum: %v(%T)", d, d)
			}
			if ed.String() != "Y" {
				continue
			}
			f := fs[i]
			p, ok := mysql.Col2PrivType[f.Name]
			if !ok {
				return errors.Errorf("Unknown Privilege Type!")
			}
			ps.add(p)
		}
	}
	p.privs.GlobalPrivs = ps
	return nil
}

func (p *UserPrivileges) loadDBScopePrivileges(ctx context.Context) error {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND (Host="%s" OR Host="%%");`, mysql.SystemDB, mysql.DBTable, p.username, p.host)
	rs, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer rs.Close()
	ps := make(map[string]*privileges)
	fs, err := rs.Fields()
	if err != nil {
		return errors.Trace(err)
	}
	for {
		row, err := rs.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		// DB
		db, ok := row.Data[1].(string)
		if !ok {
			errors.Errorf("This should be never happened!")
		}
		ps[db] = &privileges{}
		for i := dbTablePrivColumnStartIndex; i < len(fs); i++ {
			d := row.Data[i]
			ed, ok := d.(mysql.Enum)
			if !ok {
				return errors.Errorf("Privilege should be mysql.Enum: %v(%T)", d, d)
			}
			if ed.String() != "Y" {
				continue
			}
			f := fs[i]
			p, ok := mysql.Col2PrivType[f.Name]
			if !ok {
				return errors.Errorf("Unknown Privilege Type!")
			}
			ps[db].add(p)
		}
	}
	p.privs.DBPrivs = ps
	return nil
}

func (p *UserPrivileges) loadTableScopePrivileges(ctx context.Context) error {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND (Host="%s" OR Host="%%");`, mysql.SystemDB, mysql.TablePrivTable, p.username, p.host)
	rs, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer rs.Close()
	ps := make(map[string]map[string]*privileges)
	for {
		row, err := rs.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		// DB
		db, ok := row.Data[1].(string)
		if !ok {
			return errors.Errorf("This should be never happened!")
		}
		// Table_name
		tbl, ok := row.Data[3].(string)
		if !ok {
			return errors.Errorf("This should be never happened!")
		}
		_, ok = ps[db]
		if !ok {
			ps[db] = make(map[string]*privileges)
		}
		ps[db][tbl] = &privileges{}
		// Table_priv
		tblPrivs, ok := row.Data[6].(mysql.Set)
		if !ok {
			errors.Errorf("This should be never happened!")
		}
		pvs := strings.Split(tblPrivs.Name, ",")
		for _, d := range pvs {
			p, ok := mysql.SetStr2Priv[d]
			if !ok {
				return errors.Errorf("Unknown Privilege Type!")
			}
			ps[db][tbl].add(p)
		}
	}
	p.privs.TablePrivs = ps
	return nil
}
