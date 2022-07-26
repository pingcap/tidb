// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
)

// ShowVersion queries variable 'version' and returns its value.
func ShowVersion(ctx context.Context, db QueryExecutor) (value string, err error) {
	return ShowMySQLVariable(ctx, db, "version")
}

// ShowLogBin queries variable 'log_bin' and returns its value.
func ShowLogBin(ctx context.Context, db QueryExecutor) (value string, err error) {
	return ShowMySQLVariable(ctx, db, "log_bin")
}

// ShowBinlogFormat queries variable 'binlog_format' and returns its value.
func ShowBinlogFormat(ctx context.Context, db QueryExecutor) (value string, err error) {
	return ShowMySQLVariable(ctx, db, "binlog_format")
}

// ShowBinlogRowImage queries variable 'binlog_row_image' and returns its values.
func ShowBinlogRowImage(ctx context.Context, db QueryExecutor) (value string, err error) {
	return ShowMySQLVariable(ctx, db, "binlog_row_image")
}

// ShowServerID queries variable 'server_id' and returns its value.
func ShowServerID(ctx context.Context, db QueryExecutor) (serverID uint64, err error) {
	value, err := ShowMySQLVariable(ctx, db, "server_id")
	if err != nil {
		return 0, errors.Trace(err)
	}

	serverID, err = strconv.ParseUint(value, 10, 64)
	return serverID, errors.Annotatef(err, "parse server_id %s failed", value)
}

// ShowMySQLVariable queries MySQL variable and returns its value.
func ShowMySQLVariable(ctx context.Context, db QueryExecutor, variable string) (value string, err error) {
	query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s';", variable)
	err = db.QueryRowContext(ctx, query).Scan(&variable, &value)
	if err != nil {
		return "", errors.Trace(err)
	}
	return value, nil
}

// ShowGrants queries privileges for a mysql user.
// For mysql 8.0, if user has granted roles, ShowGrants also extract privilege from roles.
func ShowGrants(ctx context.Context, db QueryExecutor, user, host string) ([]string, error) {
	if host == "" {
		host = "%"
	}

	var query string
	if user == "" {
		// for current user.
		query = "SHOW GRANTS"
	} else {
		query = fmt.Sprintf("SHOW GRANTS FOR '%s'@'%s'", user, host)
	}

	readGrantsFunc := func() ([]string, error) {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer rows.Close()

		grants := make([]string, 0, 8)
		for rows.Next() {
			var grant string
			err = rows.Scan(&grant)
			if err != nil {
				return nil, errors.Trace(err)
			}

			// TiDB parser does not support parse `IDENTIFIED BY PASSWORD <secret>`,
			// but it may appear in some cases, ref: https://dev.mysql.com/doc/refman/5.6/en/show-grants.html.
			// We do not need the password in grant statement, so we can replace it.
			grant = strings.Replace(grant, "IDENTIFIED BY PASSWORD <secret>", "IDENTIFIED BY PASSWORD 'secret'", 1)

			// support parse `IDENTIFIED BY PASSWORD WITH {GRANT OPTION | resource_option} ...`
			grant = strings.Replace(grant, "IDENTIFIED BY PASSWORD WITH", "IDENTIFIED BY PASSWORD 'secret' WITH", 1)

			// support parse `IDENTIFIED BY PASSWORD`
			if strings.HasSuffix(grant, "IDENTIFIED BY PASSWORD") {
				grant = grant + " 'secret'"
			}

			grants = append(grants, grant)
		}
		if err := rows.Err(); err != nil {
			return nil, errors.Trace(err)
		}
		return grants, nil
	}

	grants, err := readGrantsFunc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// for mysql 8.0, we should collect granted roles
	var roles []*auth.RoleIdentity
	p := parser.New()
	for _, grant := range grants {
		node, err := p.ParseOneStmt(grant, "", "")
		if err != nil {
			return nil, err
		}
		if grantRoleStmt, ok := node.(*ast.GrantRoleStmt); ok {
			roles = append(roles, grantRoleStmt.Roles...)
		}
	}

	if len(roles) == 0 {
		return grants, nil
	}

	var s strings.Builder
	s.WriteString(query)
	s.WriteString(" USING ")
	for i, role := range roles {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(role.String())
	}
	query = s.String()

	return readGrantsFunc()
}

// CheckPrivilege uses the output of ShowGrants, to check `lackPriv` has been granted. Elements in `lackPriv` is deleted
// during CheckPrivilege, and after it returns, `lackPriv` is the privileges that are not found in SHOW GRANTS.
// level of keys of `lackPriv`: privilege => schema => table.
func CheckPrivilege(grants []string, lackPriv map[mysql.PrivilegeType]map[string]map[string]struct{}) error {
	if len(grants) == 0 {
		return errors.New("there is no such grant defined for current user on host '%'")
	}

	p := parser.New()
GRANTLOOP:
	for _, grant := range grants {
		if len(lackPriv) == 0 {
			break
		}
		node, err := p.ParseOneStmt(grant, "", "")
		if err != nil {
			return errors.Trace(err)
		}
		grantStmt, ok := node.(*ast.GrantStmt)
		if !ok {
			switch node.(type) {
			case *ast.GrantProxyStmt, *ast.GrantRoleStmt:
				continue
			default:
				return errors.Errorf("%s is not grant statement", grant)
			}
		}

		if len(grantStmt.Users) == 0 {
			return errors.Errorf("grant has no user %s", grant)
		}

		dbName := grantStmt.Level.DBName
		tableName := grantStmt.Level.TableName
		switch grantStmt.Level.Level {
		case ast.GrantLevelGlobal:
			for _, privElem := range grantStmt.Privs {
				// all privileges available at a given privilege level (except GRANT OPTION)
				// from https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_all
				if privElem.Priv == mysql.AllPriv {
					for k := range lackPriv {
						if k == mysql.GrantPriv {
							continue
						}
						delete(lackPriv, k)
					}
					continue GRANTLOOP
				}
				// mysql> show master status;
				// ERROR 1227 (42000): Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation
				if privElem.Priv == mysql.SuperPriv {
					delete(lackPriv, mysql.ReplicationClientPriv)
				}
				delete(lackPriv, privElem.Priv)
			}
		case ast.GrantLevelDB:
			for _, privElem := range grantStmt.Privs {
				// all privileges available at a given privilege level (except GRANT OPTION)
				// from https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_all
				if privElem.Priv == mysql.AllPriv {
					for priv := range lackPriv {
						if priv == mysql.GrantPriv {
							continue
						}
						if _, ok := lackPriv[priv][dbName]; !ok {
							continue
						}
						delete(lackPriv[priv], dbName)
						if len(lackPriv[priv]) == 0 {
							delete(lackPriv, priv)
						}
					}
					continue
				}
				if _, ok := lackPriv[privElem.Priv]; !ok {
					continue
				}
				if _, ok := lackPriv[privElem.Priv][dbName]; !ok {
					continue
				}
				// dumpling could report error if an allow-list table is lack of privilege.
				// we only check that SELECT is granted on all columns, otherwise we can't SHOW CREATE TABLE
				if privElem.Priv == mysql.SelectPriv && len(privElem.Cols) != 0 {
					continue
				}
				delete(lackPriv[privElem.Priv], dbName)
				if len(lackPriv[privElem.Priv]) == 0 {
					delete(lackPriv, privElem.Priv)
				}
			}
		case ast.GrantLevelTable:
			for _, privElem := range grantStmt.Privs {
				// all privileges available at a given privilege level (except GRANT OPTION)
				// from https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_all
				if privElem.Priv == mysql.AllPriv {
					for priv := range lackPriv {
						if priv == mysql.GrantPriv {
							continue
						}
						if _, ok := lackPriv[priv][dbName]; !ok {
							continue
						}
						if _, ok := lackPriv[priv][dbName][tableName]; !ok {
							continue
						}
						delete(lackPriv[priv][dbName], tableName)
						if len(lackPriv[priv][dbName]) == 0 {
							delete(lackPriv[priv], dbName)
						}
						if len(lackPriv[priv]) == 0 {
							delete(lackPriv, priv)
						}
					}
					continue
				}
				if _, ok := lackPriv[privElem.Priv]; !ok {
					continue
				}
				if _, ok := lackPriv[privElem.Priv][dbName]; !ok {
					continue
				}
				if _, ok := lackPriv[privElem.Priv][dbName][tableName]; !ok {
					continue
				}
				// dumpling could report error if an allow-list table is lack of privilege.
				// we only check that SELECT is granted on all columns, otherwise we can't SHOW CREATE TABLE
				if privElem.Priv == mysql.SelectPriv && len(privElem.Cols) != 0 {
					continue
				}
				delete(lackPriv[privElem.Priv][dbName], tableName)
				if len(lackPriv[privElem.Priv][dbName]) == 0 {
					delete(lackPriv[privElem.Priv], dbName)
				}
				if len(lackPriv[privElem.Priv]) == 0 {
					delete(lackPriv, privElem.Priv)
				}
			}
		}
	}
	return nil
}
