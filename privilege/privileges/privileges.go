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

var _ privilege.Checker = (*PrivilegeCheck)(nil)

type userPrivilege struct {
	GlobalPrivs map[mysql.PrivilegeType]bool
	DBPrivs     map[string]map[mysql.PrivilegeType]bool
	TablePrivs  map[string]map[string]map[mysql.PrivilegeType]bool
}

// PrivilegeCheck implements privilege.Checker interface.
// This is used to check privilege for the current user.
type PrivilegeCheck struct {
	username string
	host     string
	privs    *userPrivilege
}

// CheckDBPrivilege implements PrivilegeChecker.CheckDBPrivilege interface.
func (p *PrivilegeCheck) CheckDBPrivilege(ctx context.Context, db *model.DBInfo, privilege mysql.PrivilegeType) (bool, error) {
	if p.privs == nil {
		// Lazy load
		err := p.loadPrivileges(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	// Check global scope privileges.
	_, ok := p.privs.GlobalPrivs[privilege]
	if ok {
		return true, nil
	}
	// Check db scope privileges.
	dbp, ok := p.privs.DBPrivs[db.Name.O]
	if !ok {
		return false, nil
	}
	_, ok = dbp[privilege]
	return ok, nil
}

// CheckTablePrivilege implements PrivilegeChecker.CheckTablePrivilege interface.
func (p *PrivilegeCheck) CheckTablePrivilege(ctx context.Context, db *model.DBInfo, tbl *model.TableInfo, privilege mysql.PrivilegeType) (bool, error) {
	if p.privs == nil {
		// Lazy load
		err := p.loadPrivileges(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	// Check global scope privileges.
	_, ok := p.privs.GlobalPrivs[privilege]
	if ok {
		return true, nil
	}
	// Check db scope privileges.
	dbp, ok := p.privs.DBPrivs[db.Name.O]
	if ok {
		_, ok = dbp[privilege]
		if ok {
			return true, nil
		}
	}
	// Check table scope privileges.
	tblp, ok := p.privs.TablePrivs[db.Name.O]
	if !ok {
		return false, nil
	}
	_, ok = tblp[tbl.Name.O]
	return ok, nil
}

func (p *PrivilegeCheck) loadGlobalPrivileges(ctx context.Context) error {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND (Host="%s" OR Host="%%");`, mysql.SystemDB, mysql.UserTable, p.username, p.host)
	rs, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer rs.Close()
	ps := make(map[mysql.PrivilegeType]bool)
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
		for i := 3; i < len(fs); i++ {
			d := row.Data[i]
			ed, ok := d.(mysql.Enum)
			if !ok {
				return fmt.Errorf("Privilege should be mysql.Enum: %v(%T)", d, d)
			}
			if ed.String() != "Y" {
				continue
			}
			f := fs[i]
			// check each priv field
			p, ok := mysql.Col2PrivType[f.Name]
			if !ok {
				panic("This should be never happened!")
			}
			ps[p] = true
		}
	}
	p.privs.GlobalPrivs = ps
	return nil
}

func (p *PrivilegeCheck) loadDBScopePrivileges(ctx context.Context) error {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND (Host="%s" OR Host="%%");`, mysql.SystemDB, mysql.DBTable, p.username, p.host)
	rs, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer rs.Close()
	ps := make(map[string]map[mysql.PrivilegeType]bool)
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
		db, ok := row.Data[1].(string)
		if !ok {
			panic("This should be never happened!")
		}
		ps[db] = make(map[mysql.PrivilegeType]bool)
		for i := 3; i < len(fs); i++ {
			d := row.Data[i]
			ed, ok := d.(mysql.Enum)
			if !ok {
				return fmt.Errorf("Privilege should be mysql.Enum: %v(%T)", d, d)
			}
			if ed.String() != "Y" {
				continue
			}
			f := fs[i]
			// check each priv field
			p, ok := mysql.Col2PrivType[f.Name]
			if !ok {
				panic("This should be never happened!")
			}
			ps[db][p] = true
		}
	}
	p.privs.DBPrivs = ps
	return nil
}

func (p *PrivilegeCheck) loadTableScopePrivileges(ctx context.Context) error {
	sql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User="%s" AND (Host="%s" OR Host="%%");`, mysql.SystemDB, mysql.TablePrivTable, p.username, p.host)
	rs, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer rs.Close()
	ps := make(map[string]map[string]map[mysql.PrivilegeType]bool)
	for {
		row, err := rs.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		db, ok := row.Data[1].(string)
		if !ok {
			panic("This should be never happened!")
		}
		tbl, ok := row.Data[3].(string)
		if !ok {
			panic("This should be never happened!")
		}
		_, ok = ps[db]
		if !ok {
			ps[db] = make(map[string]map[mysql.PrivilegeType]bool)
		}
		ps[db][tbl] = make(map[mysql.PrivilegeType]bool)
		tblPrivs, ok := row.Data[6].(mysql.Set)
		if !ok {
			panic("This should be never happened!")
		}
		pvs := strings.Split(tblPrivs.Name, ",")
		for _, d := range pvs {
			p, ok := mysql.SetStr2Priv[d]
			if !ok {
				panic("This should be never happened!")
			}
			ps[db][tbl][p] = true
		}
	}
	p.privs.TablePrivs = ps
	return nil
}

func (p *PrivilegeCheck) loadPrivileges(ctx context.Context) error {
	user := variable.GetSessionVars(ctx).User
	strs := strings.Split(user, "@")
	p.username, p.host = strs[0], strs[1]
	p.privs = &userPrivilege{}
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
