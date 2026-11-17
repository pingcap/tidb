// Copyright 2026 PingCAP, Inc.
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

package task

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type restoredMaskingColumn struct {
	source, target pmodel.CIStr
	invalid        bool
}

func (v *restoredMaskingColumn) Enter(node ast.Node) (ast.Node, bool) {
	if col, ok := node.(*ast.ColumnNameExpr); ok {
		if col.Name.Name.L != v.source.L {
			v.invalid = true
		}
		col.Name.Schema = pmodel.CIStr{}
		col.Name.Table = pmodel.CIStr{}
		col.Name.Name = v.target
	}
	return node, false
}

func (*restoredMaskingColumn) Leave(node ast.Node) (ast.Node, bool) { return node, true }

func rebindRestoredMaskingExpression(expr, sourceColumn, targetColumn string) (string, error) {
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(expr)
	stmt, err := parser.New().ParseOneStmt(query.String(), "", "")
	if err != nil {
		return "", errors.Trace(err)
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok || selectStmt.Fields == nil || len(selectStmt.Fields.Fields) != 1 || selectStmt.From != nil {
		return "", errors.New("invalid restored masking policy expression")
	}
	visitor := &restoredMaskingColumn{source: pmodel.NewCIStr(sourceColumn), target: pmodel.NewCIStr(targetColumn)}
	node, ok := selectStmt.Fields.Fields[0].Expr.Accept(visitor)
	if !ok || visitor.invalid {
		return "", errors.New("restored masking policy expression refers to another column")
	}
	var sb strings.Builder
	if err := node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", errors.Trace(err)
	}
	return sb.String(), nil
}

func isSourceMaskingPolicyTable(db *stream.DBReplace, tbl *stream.TableReplace) bool {
	dbName := db.SourceName
	if dbName == "" {
		dbName = db.Name
	}
	tableName := tbl.SourceName
	if tableName == "" {
		tableName = tbl.Name
	}
	return (strings.EqualFold(dbName, mysql.SystemDB) || strings.EqualFold(dbName, utils.TemporaryDBName(mysql.SystemDB).O)) &&
		strings.EqualFold(tableName, "tidb_masking_policy")
}

// prepareMaskingPolicyStaging preserves the source policy IDs in a private
// table, whose physical ID is saved in the regular PiTR ID map. This also lets
// retries and subsequent log-only restores apply source-key deletes correctly.
func prepareMaskingPolicyStaging(ctx context.Context, dom *domain.Domain, se glue.Session, cfg *LogRestoreConfig, canCreate, idMapPersisted bool) error {
	if !cfg.WithSysTable {
		return nil
	}
	hasPolicyStage := false
	for _, db := range cfg.tableMappingManager.DBReplaceMap {
		for _, tbl := range db.TableMap {
			if !isSourceMaskingPolicyTable(db, tbl) {
				continue
			}
			hasPolicyStage = true
			if target, ok := dom.InfoSchema().TableByID(ctx, tbl.TableID); ok {
				targetDB, exists := dom.InfoSchema().SchemaByID(target.Meta().DBID)
				if exists && targetDB.Name.L == utils.TemporaryDBName(mysql.SystemDB).L && target.Meta().Name.L == "tidb_masking_policy" {
					if !idMapPersisted {
						continue // A retry may still need the snapshot's original table name.
					}
					// Keep the original MVCC versions: SQL copying would write the
					// baseline at a timestamp newer than the source log mutations.
					if err := snapclient.PreserveMaskingPolicyStagingTable(ctx, dom, se, target.Meta()); err != nil {
						return err
					}
					continue
				}
				if !exists || targetDB.Name.L != strings.ToLower(snapclient.MaskingPolicyStagingDB) ||
					!strings.HasPrefix(target.Meta().Name.L, "tidb_pitr_masking_policy_") {
					return errors.New("PiTR masking policy ID map does not reference a retained staging table")
				}
				continue
			}
			if !canCreate {
				return errors.New("PiTR masking policy staging table is missing; restore from the full backup again")
			}
			// The source may have introduced masking after the baseline snapshot.
			// In that case no snapshot rows exist and an empty stage is correct.
			if err := se.Execute(ctx, "CREATE DATABASE IF NOT EXISTS "+utils.EncloseName(snapclient.MaskingPolicyStagingDB)); err != nil {
				return errors.Trace(err)
			}
			name := snapclient.MaskingPolicyStagingTableName(tbl.TableID)
			qualified := utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, name)
			if err := se.Execute(ctx, "CREATE TABLE IF NOT EXISTS "+qualified+" LIKE mysql.tidb_masking_policy"); err != nil {
				return errors.Annotate(err, "cannot create PiTR masking policy staging table")
			}
			target, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(snapclient.MaskingPolicyStagingDB), pmodel.NewCIStr(name))
			if err != nil {
				return errors.Trace(err)
			}
			tbl.TableID = target.Meta().ID
		}
	}
	if hasPolicyStage && idMapPersisted {
		return errors.Trace(se.Execute(ctx, "DROP DATABASE IF EXISTS "+utils.EncloseName(utils.TemporaryDBName(mysql.SystemDB).O)))
	}
	return nil
}

// System tables normally have no log rewrite rules. Only the source policy
// table is ingested, and only into its private staging table, never the live
// mysql.tidb_masking_policy table.
func addMaskingPolicyRewriteRules(schemas *stream.SchemasReplace, rules map[int64]*restoreutils.RewriteRules) {
	for _, db := range schemas.DbReplaceMap {
		for sourceID, tbl := range db.TableMap {
			if isSourceMaskingPolicyTable(db, tbl) {
				rules[sourceID] = restoreutils.GetRewriteRuleOfTable(sourceID, tbl.TableID, nil, false)
			}
		}
	}
}

// restoreMaskingPoliciesAfterLogReplay binds the target-time policy rows to the
// final restored table/column IDs. Do this before making partial-restore tables
// writable. Staging tables remain available for checkpoints and log-only PiTR.
func restoreMaskingPoliciesAfterLogReplay(ctx context.Context, dom *domain.Domain, se glue.Session, schemas *stream.SchemasReplace) error {
	if err := dom.Reload(); err != nil {
		return errors.Trace(err)
	}
	is := dom.InfoSchema()
	targetTables := make(map[int64]*model.TableInfo)
	sourceTargets := make(map[int64]*model.TableInfo)
	staging := make(map[int64]*model.TableInfo)
	for _, db := range schemas.DbReplaceMap {
		for sourceID, tbl := range db.TableMap {
			if isSourceMaskingPolicyTable(db, tbl) {
				stage, ok := is.TableByID(ctx, tbl.TableID)
				if !ok {
					return errors.New("PiTR masking policy staging table disappeared")
				}
				staging[tbl.TableID] = stage.Meta()
				continue
			}
			if db.FilteredOut || tbl.FilteredOut || utils.IsSysOrTempSysDB(tbl.EffectiveDBName(db)) {
				continue
			}
			if target, ok := is.TableByID(ctx, tbl.TableID); ok {
				sourceTargets[sourceID] = target.Meta()
				targetTables[tbl.TableID] = target.Meta()
			}
		}
	}
	if len(staging) == 0 {
		return nil
	}
	// Keep restored user tables protected until every policy is installed.
	// Only internal masking-policy DDL may operate on Restore-mode tables.
	vars := se.GetSessionCtx().GetSessionVars()
	previousRestore := vars.InMaskingPolicyRestore
	vars.InMaskingPolicyRestore = true
	defer func() { vars.InMaskingPolicyRestore = previousRestore }()
	var statements []string
	for _, stage := range staging {
		db, ok := is.SchemaByID(stage.DBID)
		if !ok {
			return errors.New("PiTR masking policy staging database disappeared")
		}
		query := fmt.Sprintf("SELECT policy_name, table_id, column_id, expression, status, restrict_on, column_name FROM %s ORDER BY policy_id",
			utils.EncloseDBAndTable(db.Name.O, stage.Name.O))
		rows, _, err := se.GetSessionCtx().GetRestrictedSQLExecutor().ExecRestrictedSQL(
			kv.WithInternalSourceType(ctx, kv.InternalTxnBR), nil, query)
		if err != nil {
			return errors.Trace(err)
		}
		for _, row := range rows {
			target, ok := sourceTargets[row.GetInt64(1)]
			if !ok {
				continue // An unselected or dropped source table has no target policy.
			}
			col := model.FindColumnInfoByID(target.Columns, row.GetInt64(2))
			if col == nil {
				return errors.Errorf("restored masking policy %s refers to missing column ID %d", row.GetString(0), row.GetInt64(2))
			}
			db, ok := is.SchemaByID(target.DBID)
			if !ok {
				return errors.New("restored masking policy target database disappeared")
			}
			expr, err := rebindRestoredMaskingExpression(row.GetString(3), row.GetString(6), col.Name.O)
			if err != nil {
				return errors.Trace(err)
			}
			statements = append(statements, snapclient.BuildCreateMaskingPolicySQL(row.GetString(0), db.Name.O, target.Name.O,
				col.Name.O, expr, row.GetString(4), row.GetString(5)))
		}
	}
	// Replace the entire policy set of selected tables, so drops and retries
	// cannot leave a stale baseline policy behind. Preserve unrelated targets.
	policies, err := is.AllMaskingPolicies(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for _, policy := range policies {
		if tbl, ok := targetTables[policy.TableID]; ok {
			db, _ := is.SchemaByID(tbl.DBID)
			sql := fmt.Sprintf("ALTER TABLE %s DROP MASKING POLICY %s", utils.EncloseDBAndTable(db.Name.O, tbl.Name.O), utils.EncloseName(policy.Name.O))
			if err := se.Execute(ctx, sql); err != nil {
				return errors.Trace(err)
			}
		}
	}
	sort.Strings(statements)
	for _, sql := range statements {
		if err := se.Execute(ctx, sql); err != nil {
			return errors.Annotate(err, "cannot rebuild masking policy after PiTR log replay")
		}
	}
	return errors.Trace(dom.Reload())
}
