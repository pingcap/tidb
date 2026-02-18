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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func (b *PlanBuilder) buildDDL(ctx context.Context, node ast.DDLNode) (base.Plan, error) {
	var authErr error
	switch v := node.(type) {
	case *ast.AlterDatabaseStmt:
		if v.AlterDefaultDatabase {
			v.Name = ast.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
		}
		if v.Name.O == "" {
			return nil, plannererrors.ErrNoDB
		}
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name.O)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.Name.L, "", "", authErr)
	case *ast.AlterTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		dbName := getLowerDB(v.Table.Schema, b.ctx.GetSessionVars())
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, dbName,
			v.Table.Name.L, "", authErr)
		for _, spec := range v.Specs {
			if spec.Tp == ast.AlterTableRenameTable || spec.Tp == ast.AlterTableExchangePartition {
				if b.ctx.GetSessionVars().User != nil {
					authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, dbName,
					v.Table.Name.L, "", authErr)

				if b.ctx.GetSessionVars().User != nil {
					authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, spec.NewTable.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, dbName,
					spec.NewTable.Name.L, "", authErr)

				if b.ctx.GetSessionVars().User != nil {
					authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, spec.NewTable.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, dbName,
					spec.NewTable.Name.L, "", authErr)
			} else if spec.Tp == ast.AlterTableDropPartition || spec.Tp == ast.AlterTableTruncatePartition {
				if b.ctx.GetSessionVars().User != nil {
					authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
					v.Table.Name.L, "", authErr)
			} else if spec.Tp == ast.AlterTableWriteable {
				b.visitInfo[0].alterWritable = true
			} else if spec.Tp == ast.AlterTableAddStatistics {
				var selectErr, insertErr error
				user := b.ctx.GetSessionVars().User
				if user != nil {
					selectErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("ADD STATS_EXTENDED", user.AuthUsername,
						user.AuthHostname, v.Table.Name.L)
					insertErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("ADD STATS_EXTENDED", user.AuthUsername,
						user.AuthHostname, "stats_extended")
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, v.Table.Schema.L,
					v.Table.Name.L, "", selectErr)
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, mysql.SystemDB,
					"stats_extended", "", insertErr)
			} else if spec.Tp == ast.AlterTableDropStatistics {
				user := b.ctx.GetSessionVars().User
				if user != nil {
					authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP STATS_EXTENDED", user.AuthUsername,
						user.AuthHostname, "stats_extended")
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.UpdatePriv, mysql.SystemDB,
					"stats_extended", "", authErr)
			} else if spec.Tp == ast.AlterTableAddConstraint {
				if b.ctx.GetSessionVars().User != nil && spec.Constraint != nil &&
					spec.Constraint.Tp == ast.ConstraintForeignKey && spec.Constraint.Refer != nil {
					authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("REFERENCES", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, spec.Constraint.Refer.Table.Name.L)
					b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ReferencesPriv, spec.Constraint.Refer.Table.Schema.L,
						spec.Constraint.Refer.Table.Name.L, "", authErr)
				}
			}
		}
	case *ast.AlterSequenceStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.Name.Schema.L,
			v.Name.Name.L, "", authErr)
	case *ast.CreateDatabaseStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Name.L,
			"", "", authErr)
	case *ast.CreateIndexStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("INDEX", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.IndexPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.CreateTableStmt:
		if v.TemporaryKeyword != ast.TemporaryNone {
			for _, cons := range v.Constraints {
				if cons.Tp == ast.ConstraintForeignKey {
					return nil, infoschema.ErrCannotAddForeign
				}
			}
		}
		if b.ctx.GetSessionVars().User != nil {
			// This is tricky here: we always need the visitInfo because it's not only used in privilege checks, and we
			// must pass the table name. However, the privilege check is towards the database. We'll deal with it later.
			if v.TemporaryKeyword == ast.TemporaryLocal {
				authErr = plannererrors.ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, v.Table.Schema.L)
			} else {
				authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
			}
			for _, cons := range v.Constraints {
				if cons.Tp == ast.ConstraintForeignKey && cons.Refer != nil {
					authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("REFERENCES", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, cons.Refer.Table.Name.L)
					b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ReferencesPriv, cons.Refer.Table.Schema.L,
						cons.Refer.Table.Name.L, "", authErr)
				}
			}
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
		if v.ReferTable != nil {
			if b.ctx.GetSessionVars().User != nil {
				authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, v.ReferTable.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, v.ReferTable.Schema.L,
				v.ReferTable.Name.L, "", authErr)
		}
	case *ast.CreateViewStmt:
		err := checkForUserVariables(v.Select)
		if err != nil {
			return nil, err
		}
		b.isCreateView = true
		b.capFlag |= canExpandAST | renameView
		b.renamingViewName = v.ViewName.Schema.L + "." + v.ViewName.Name.L
		defer func() {
			b.capFlag &= ^canExpandAST
			b.capFlag &= ^renameView
			b.isCreateView = false
		}()

		if stmt := findStmtAsViewSchema(v); stmt != nil {
			stmt.AsViewSchema = true
		}

		nodeW := resolve.NewNodeWWithCtx(v.Select, b.resolveCtx)
		plan, err := b.Build(ctx, nodeW)
		if err != nil {
			return nil, err
		}
		schema := plan.Schema()
		names := plan.OutputNames()
		if v.Cols == nil {
			adjustOverlongViewColname(plan.(base.LogicalPlan))
			v.Cols = make([]ast.CIStr, len(schema.Columns))
			for i, name := range names {
				v.Cols[i] = name.ColName
			}
		}
		if len(v.Cols) != schema.Len() {
			return nil, dbterror.ErrViewWrongList
		}
		var authCreateErr, authDropErr error
		if user := b.ctx.GetSessionVars().User; user != nil {
			authCreateErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("CREATE VIEW", user.AuthUsername,
				user.AuthHostname, v.ViewName.Name.L)
			if v.OrReplace {
				authDropErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP", user.AuthUsername,
					user.AuthHostname, v.ViewName.Name.L)
			}
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateViewPriv, v.ViewName.Schema.L,
			v.ViewName.Name.L, "", authCreateErr)
		if v.OrReplace {
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.ViewName.Schema.L,
				v.ViewName.Name.L, "", authDropErr)
		}
		if v.Definer.CurrentUser && b.ctx.GetSessionVars().User != nil {
			v.Definer = b.ctx.GetSessionVars().User
		}
		if b.ctx.GetSessionVars().User != nil && v.Definer.String() != b.ctx.GetSessionVars().User.String() {
			err = plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "",
				"", "", err)
		}
	case *ast.CreateSequenceStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Name.Schema.L,
			v.Name.Name.L, "", authErr)
	case *ast.DropDatabaseStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Name.L,
			"", "", authErr)
	case *ast.DropIndexStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("INDEX", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.IndexPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.DropTableStmt:
		for _, tableVal := range v.Tables {
			if b.ctx.GetSessionVars().User != nil {
				authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, tableVal.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, tableVal.Schema.L,
				tableVal.Name.L, "", authErr)
		}
	case *ast.DropSequenceStmt:
		for _, sequence := range v.Sequences {
			if b.ctx.GetSessionVars().User != nil {
				authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, sequence.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, sequence.Schema.L,
				sequence.Name.L, "", authErr)
		}
	case *ast.TruncateTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.RenameTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.TableToTables[0].OldTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.TableToTables[0].OldTable.Schema.L,
			v.TableToTables[0].OldTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.TableToTables[0].OldTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.TableToTables[0].OldTable.Schema.L,
			v.TableToTables[0].OldTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.TableToTables[0].NewTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.TableToTables[0].NewTable.Schema.L,
			v.TableToTables[0].NewTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.TableToTables[0].NewTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, v.TableToTables[0].NewTable.Schema.L,
			v.TableToTables[0].NewTable.Name.L, "", authErr)
	case *ast.RecoverTableStmt:
		if v.Table == nil {
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
		} else {
			if b.ctx.GetSessionVars().User != nil {
				authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Table.Schema.L, v.Table.Name.L, "", authErr)
			if b.ctx.GetSessionVars().User != nil {
				authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L, v.Table.Name.L, "", authErr)
		}
	case *ast.FlashBackTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Table.Schema.L, v.Table.Name.L, "", authErr)
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L, v.Table.Name.L, "", authErr)
	case *ast.FlashBackDatabaseStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = plannererrors.ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.DBName.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.DBName.L, "", "", authErr)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.DBName.L, "", "", authErr)
	case *ast.FlashBackToTimestampStmt:
		// Flashback cluster can only be executed by user with `super` privilege.
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.LockTablesStmt:
		user := b.ctx.GetSessionVars().User
		for _, lock := range v.TableLocks {
			var lockAuthErr, selectAuthErr error
			if user != nil {
				lockAuthErr = plannererrors.ErrDBaccessDenied.FastGenByArgs(user.AuthUsername, user.AuthHostname, lock.Table.Schema.L)
				selectAuthErr = plannererrors.ErrTableaccessDenied.FastGenByArgs("SELECT", user.AuthUsername, user.AuthHostname, lock.Table.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.LockTablesPriv, lock.Table.Schema.L, lock.Table.Name.L, "", lockAuthErr)
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, lock.Table.Schema.L, lock.Table.Name.L, "", selectAuthErr)
		}
	case *ast.CleanupTableLockStmt:
		// This command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.RepairTableStmt:
		// Repair table command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.DropPlacementPolicyStmt, *ast.CreatePlacementPolicyStmt, *ast.AlterPlacementPolicyStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or PLACEMENT_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"PLACEMENT_ADMIN"}, false, err)
	case *ast.CreateResourceGroupStmt, *ast.DropResourceGroupStmt, *ast.AlterResourceGroupStmt:
		err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESOURCE_GROUP_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"RESOURCE_GROUP_ADMIN"}, false, err)
	case *ast.OptimizeTableStmt:
		return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStack("OPTIMIZE TABLE is not supported")
	}
	p := &DDL{Statement: node}
	return p, nil
}
