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

package check

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
)

var (
	dumpPrivileges = map[mysql.PrivilegeType]struct{}{
		mysql.ReloadPriv: {},
		mysql.SelectPriv: {},
	}
	replicationPrivileges = map[mysql.PrivilegeType]struct{}{
		mysql.ReplicationClientPriv: {},
		mysql.ReplicationSlavePriv:  {},
	}

	// some privileges are only effective on global level. in other words, GRANT ALL ON test.* is not enough for them
	// https://dev.mysql.com/doc/refman/5.7/en/grant.html#grant-global-privileges
	privNeedGlobal = map[mysql.PrivilegeType]struct{}{
		mysql.ReloadPriv:            {},
		mysql.ReplicationClientPriv: {},
		mysql.ReplicationSlavePriv:  {},
	}
)

/*****************************************************/

// SourceDumpPrivilegeChecker checks dump privileges of source DB.
type SourceDumpPrivilegeChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewSourceDumpPrivilegeChecker returns a Checker.
func NewSourceDumpPrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &SourceDumpPrivilegeChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
// We only check RELOAD, SELECT privileges.
func (pc *SourceDumpPrivilegeChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check dump privileges of source DB",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	grants, err := dbutil.ShowGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}

	verifyPrivileges(result, grants, dumpPrivileges)
	return result
}

// Name implements the Checker interface.
func (pc *SourceDumpPrivilegeChecker) Name() string {
	return "source db dump privilege checker"
}

/*****************************************************/

// SourceReplicatePrivilegeChecker checks replication privileges of source DB.
type SourceReplicatePrivilegeChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewSourceReplicationPrivilegeChecker returns a Checker.
func NewSourceReplicationPrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &SourceReplicatePrivilegeChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
// We only check REPLICATION SLAVE, REPLICATION CLIENT privileges.
func (pc *SourceReplicatePrivilegeChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check replication privileges of source DB",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	grants, err := dbutil.ShowGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}

	verifyPrivileges(result, grants, replicationPrivileges)
	return result
}

// Name implements the Checker interface.
func (pc *SourceReplicatePrivilegeChecker) Name() string {
	return "source db replication privilege checker"
}

// TODO: if we add more privilege in future, we might add special checks (globally granted?) for that new privilege
func verifyPrivileges(result *Result, grants []string, expectedGrants map[mysql.PrivilegeType]struct{}) {
	result.State = StateFailure
	if len(grants) == 0 {
		result.Errors = append(result.Errors, NewError("there is no such grant defined for current user on host '%%'"))
		return
	}

	var (
		user       string
		lackGrants = make(map[mysql.PrivilegeType]struct{}, len(expectedGrants))
	)
	for k := range expectedGrants {
		lackGrants[k] = struct{}{}
	}

	for i, grant := range grants {

		// get username and hostname
		node, err := parser.New().ParseOneStmt(grant, "", "")
		if err != nil {
			result.Errors = append(result.Errors, NewError(errors.Annotatef(err, "grant %s, grant after replace %s", grants[i], grant).Error()))
			return
		}
		grantStmt, ok := node.(*ast.GrantStmt)
		if !ok {
			switch node.(type) {
			case *ast.GrantProxyStmt, *ast.GrantRoleStmt:
				continue
			default:
				result.Errors = append(result.Errors, NewError("%s is not grant statement", grants[i]))
				return
			}
		}

		if len(grantStmt.Users) == 0 {
			result.Errors = append(result.Errors, NewError("grant has no user %s", grantStmt.Text()))
			return
		} else if user == "" {
			// show grants will only output grants for requested user
			user = grantStmt.Users[0].User.Username
		}

		for _, privElem := range grantStmt.Privs {
			if privElem.Priv == mysql.AllPriv {
				if grantStmt.Level.Level == ast.GrantLevelGlobal {
					result.State = StateSuccess
					return
				} else {
					// REPLICATION CLIENT, REPLICATION SLAVE, RELOAD should be global privileges,
					// thus a non-global GRANT ALL is not enough
					for expectedGrant := range lackGrants {
						if _, ok := privNeedGlobal[expectedGrant]; !ok {
							delete(lackGrants, expectedGrant)
						}
					}
				}
			} else {
				// check every privilege and remove it from expectedGrants
				if _, ok := lackGrants[privElem.Priv]; ok {
					if _, ok := privNeedGlobal[privElem.Priv]; ok {
						if grantStmt.Level.Level == ast.GrantLevelGlobal {
							delete(lackGrants, privElem.Priv)
						}
					} else {
						// currently, only SELECT privilege goes here. we didn't require SELECT to be granted globally,
						// dumpling could report error if an allow-list table is lack of privilege.
						// we only check that SELECT is granted on all columns, otherwise we can't SHOW CREATE TABLE
						if len(privElem.Cols) == 0 {
							delete(lackGrants, privElem.Priv)
						}
					}
				}
			}
		}
	}

	if len(lackGrants) != 0 {
		lackGrantsStr := make([]string, 0, len(lackGrants))
		for g := range lackGrants {
			lackGrantsStr = append(lackGrantsStr, mysql.Priv2Str[g])
		}
		privileges := strings.Join(lackGrantsStr, ",")
		result.Errors = append(result.Errors, NewError("lack of %s privilege", privileges))
		result.Instruction = fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%s';", privileges, user, "%")
		return
	}

	result.State = StateSuccess
	return
}
