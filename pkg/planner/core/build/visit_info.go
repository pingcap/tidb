// Copyright 2025 PingCAP, Inc.
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

package build

import (
	"context"
	"reflect"
	"slices"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/lock"
	tablelock "github.com/pingcap/tidb/pkg/lock/context"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// VisitInfo is record the visit info when traverse down through ast.
type VisitInfo struct {
	privilege     mysql.PrivilegeType
	db            string
	table         string
	column        string
	err           error
	alterWritable bool
	// if multiple privileges is provided, user should
	// have at least one privilege to pass the check.
	dynamicPrivs     []string
	dynamicWithGrant bool
}

// GetPriv return privilege.
func (v *VisitInfo) GetPriv() mysql.PrivilegeType {
	return v.privilege
}

// GetDB return db string.
func (v *VisitInfo) GetDB() string {
	return v.db
}

// GetTable return table string.
func (v *VisitInfo) GetTable() string {
	return v.table
}

// GetColumn return column string.
func (v *VisitInfo) GetColumn() string {
	return v.column
}

// SetAlterWritable set the visit info's alterWritable
func (v *VisitInfo) SetAlterWritable(b bool) {
	v.alterWritable = b
}

// Equals just compare two visitInfo is same or not.
func (v *VisitInfo) Equals(other *VisitInfo) bool {
	return v.privilege == other.privilege &&
		v.db == other.db &&
		v.table == other.table &&
		v.column == other.column &&
		v.err == other.err &&
		v.alterWritable == other.alterWritable &&
		reflect.DeepEqual(v.dynamicPrivs, other.dynamicPrivs) &&
		v.dynamicWithGrant == other.dynamicWithGrant
}

// AppendDynamicVisitInfo record and append another ExtendedPriv visitInfo into current slice.
func AppendDynamicVisitInfo(vi []VisitInfo, privs []string, withGrant bool, err error) []VisitInfo {
	return append(vi, VisitInfo{
		privilege:        mysql.ExtendedPriv,
		dynamicPrivs:     privs,
		dynamicWithGrant: withGrant,
		err:              err,
	})
}

// AppendVisitInfo record and append another visitInfo into current slice.
func AppendVisitInfo(vi []VisitInfo, priv mysql.PrivilegeType, db, tbl, col string, err error) []VisitInfo {
	return append(vi, VisitInfo{
		privilege: priv,
		db:        db,
		table:     tbl,
		column:    col,
		err:       err,
	})
}

// CheckPrivilege checks the privilege for a user.
func CheckPrivilege(activeRoles []*auth.RoleIdentity, pm privilege.Manager, vs []VisitInfo) error {
	for _, v := range vs {
		if v.privilege == mysql.ExtendedPriv {
			hasPriv := false
			for _, priv := range v.dynamicPrivs {
				hasPriv = hasPriv || pm.RequestDynamicVerification(activeRoles, priv, v.dynamicWithGrant)
				if hasPriv {
					break
				}
			}
			if !hasPriv {
				if v.err == nil {
					return plannererrors.ErrPrivilegeCheckFail.GenWithStackByArgs(v.dynamicPrivs)
				}
				return v.err
			}
		} else if !pm.RequestVerification(activeRoles, v.db, v.table, v.column, v.privilege) {
			if v.err == nil {
				return plannererrors.ErrPrivilegeCheckFail.GenWithStackByArgs(v.privilege.String())
			}
			return v.err
		}
	}
	return nil
}

// VisitInfo4PrivCheck generates privilege check infos because privilege check of local temporary tables is different
// with normal tables. `CREATE` statement needs `CREATE TEMPORARY TABLE` privilege from the database, and subsequent
// statements do not need any privileges.
func VisitInfo4PrivCheck(ctx context.Context, is infoschema.InfoSchema, node ast.Node, vs []VisitInfo) (privVisitInfo []VisitInfo) {
	if node == nil {
		return vs
	}

	switch stmt := node.(type) {
	case *ast.CreateTableStmt:
		privVisitInfo = make([]VisitInfo, 0, len(vs))
		for _, v := range vs {
			if v.privilege == mysql.CreatePriv {
				if stmt.TemporaryKeyword == ast.TemporaryLocal {
					// `CREATE TEMPORARY TABLE` privilege is required from the database, not the table.
					newVisitInfo := v
					newVisitInfo.privilege = mysql.CreateTMPTablePriv
					newVisitInfo.table = ""
					privVisitInfo = append(privVisitInfo, newVisitInfo)
				} else {
					// If both the normal table and temporary table already exist, we need to check the privilege.
					privVisitInfo = append(privVisitInfo, v)
				}
			} else {
				// `CREATE TABLE LIKE tmp` or `CREATE TABLE FROM SELECT tmp` in the future.
				if needCheckTmpTablePriv(ctx, is, v) {
					privVisitInfo = append(privVisitInfo, v)
				}
			}
		}
	case *ast.DropTableStmt:
		// Dropping a local temporary table doesn't need any privileges.
		if stmt.IsView {
			privVisitInfo = vs
		} else {
			privVisitInfo = make([]VisitInfo, 0, len(vs))
			if stmt.TemporaryKeyword != ast.TemporaryLocal {
				for _, v := range vs {
					if needCheckTmpTablePriv(ctx, is, v) {
						privVisitInfo = append(privVisitInfo, v)
					}
				}
			}
		}
	case *ast.GrantStmt, *ast.DropSequenceStmt, *ast.DropPlacementPolicyStmt:
		// Some statements ignore local temporary tables, so they should check the privileges on normal tables.
		privVisitInfo = vs
	default:
		privVisitInfo = make([]VisitInfo, 0, len(vs))
		for _, v := range vs {
			if needCheckTmpTablePriv(ctx, is, v) {
				privVisitInfo = append(privVisitInfo, v)
			}
		}
	}
	return
}

func needCheckTmpTablePriv(ctx context.Context, is infoschema.InfoSchema, v VisitInfo) bool {
	if v.db != "" && v.table != "" {
		// Other statements on local temporary tables except `CREATE` do not check any privileges.
		tb, err := is.TableByName(ctx, ast.NewCIStr(v.db), ast.NewCIStr(v.table))
		// If the table doesn't exist, we do not report errors to avoid leaking the existence of the table.
		if err == nil && tb.Meta().TempTableType == model.TempTableLocal {
			return false
		}
	}
	return true
}

// GetDBTableInfo gets the accessed dbs and tables info.
func GetDBTableInfo(visitInfo []VisitInfo) []stmtctx.TableEntry {
	var tables []stmtctx.TableEntry
	existsFunc := func(tbls []stmtctx.TableEntry, tbl *stmtctx.TableEntry) bool {
		return slices.Contains(tbls, *tbl)
	}
	for _, v := range visitInfo {
		if v.db == "" && v.table == "" {
			// when v.db == "" and v.table == "", it means this visitInfo is for dynamic privilege,
			// so it is not related to any database or table.
			continue
		}

		tbl := &stmtctx.TableEntry{DB: v.db, Table: v.table}
		if !existsFunc(tables, tbl) {
			tables = append(tables, *tbl)
		}
	}
	return tables
}

// CheckTableLock checks the table lock.
func CheckTableLock(ctx tablelock.TableLockReadContext, is infoschema.InfoSchema, vs []VisitInfo) error {
	if !config.TableLockEnabled() {
		return nil
	}

	checker := lock.NewChecker(ctx, is)
	for i := range vs {
		err := checker.CheckTableLock(vs[i].db, vs[i].table, vs[i].privilege, vs[i].alterWritable)
		// if table with lock-write table dropped, we can access other table, such as `rename` operation
		if err == lock.ErrLockedTableDropped {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// CheckFastPlanPrivilege check the fast plan's privs.
func CheckFastPlanPrivilege(ctx base.PlanContext, dbName, tableName string, checkTypes ...mysql.PrivilegeType) error {
	pm := privilege.GetPrivilegeManager(ctx)
	visitInfos := make([]VisitInfo, 0, len(checkTypes))
	for _, checkType := range checkTypes {
		if pm != nil && !pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, tableName, "", checkType) {
			return plannererrors.ErrPrivilegeCheckFail.GenWithStackByArgs(checkType.String())
		}
		// This visitInfo is only for table lock check, so we do not need column field,
		// just fill it empty string.
		visitInfos = append(visitInfos, VisitInfo{
			privilege: checkType,
			db:        dbName,
			table:     tableName,
			column:    "",
			err:       nil,
		})
	}

	infoSchema := ctx.GetInfoSchema().(infoschema.InfoSchema)
	return CheckTableLock(ctx, infoSchema, visitInfos)
}
