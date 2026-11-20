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
	"github.com/pingcap/tidb/br/pkg/metautil"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func isMaskingPolicyDDL(action model.ActionType) bool {
	return action == model.ActionCreateMaskingPolicy || action == model.ActionAlterMaskingPolicy || action == model.ActionDropMaskingPolicy
}

// hasMaskingPolicySource reports whether this interval carries policy metadata.
func hasMaskingPolicySource(mapping *stream.TableMappingManager) bool {
	if mapping != nil {
		for _, db := range mapping.DBReplaceMap {
			for _, tbl := range db.TableMap {
				if isSourceMaskingPolicyTable(db, tbl) {
					return true
				}
			}
		}
	}
	return false
}

func readMaskingPolicyFenceOwners(ctx context.Context, dom *domain.Domain, se glue.Session) (map[int64]uint64, error) {
	_, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(snapclient.MaskingPolicyStagingDB), pmodel.NewCIStr(snapclient.MaskingPolicyFenceTableName))
	if infoschema.ErrTableNotExists.Equal(err) {
		return map[int64]uint64{}, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	rows, _, err := se.GetSessionCtx().GetRestrictedSQLExecutor().ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR), nil, "SELECT table_id, restore_id FROM %n.%n",
		snapclient.MaskingPolicyStagingDB, snapclient.MaskingPolicyFenceTableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	owners := make(map[int64]uint64, len(rows))
	for _, row := range rows {
		owners[row.GetInt64(0)] = row.GetUint64(1)
	}
	return owners, nil
}

// protectMaskingPolicyTables records ownership before making any data visible.
// Missing IDs are preallocated targets: their tables will be created in Restore
// mode. An existing Restore-mode table without a record belongs to someone else.
func protectMaskingPolicyTables(ctx context.Context, dom *domain.Domain, se glue.Session, tableIDs []int64, restoreID uint64, isRegistered func(context.Context, uint64) (bool, error)) error {
	if len(tableIDs) == 0 {
		return nil
	}
	if restoreID == 0 {
		return errors.New("masking policy restore protection requires a restore task ID")
	}
	owners, err := readMaskingPolicyFenceOwners(ctx, dom, se)
	if err != nil {
		return err
	}
	var takeovers []fenceTakeover
	for _, tableID := range tableIDs {
		tbl, exists, err := loadTableByID(ctx, dom.InfoSchema(), tableID)
		if err != nil {
			return err
		}
		if exists && tbl.Meta().Mode != model.TableModeNormal && tbl.Meta().Mode != model.TableModeRestore {
			return errors.Errorf("cannot protect table %s in %s mode for masking policy restore", tbl.Meta().Name.O, tbl.Meta().Mode.String())
		}
		owner, recorded := owners[tableID]
		if exists && tbl.Meta().Mode == model.TableModeRestore && !recorded {
			return errors.Errorf("cannot attribute Restore-mode table %s to masking policy restore", tbl.Meta().Name.O)
		}
		if recorded && owner != restoreID && (!exists || tbl.Meta().Mode == model.TableModeRestore) {
			if isRegistered == nil {
				return errors.Errorf("masking policy protection of table %d belongs to restore task %d", tableID, owner)
			}
			registered, err := isRegistered(ctx, owner)
			if err != nil {
				return errors.Trace(err)
			}
			if registered {
				return errors.Errorf("masking policy protection of table %d belongs to registered restore task %d", tableID, owner)
			}
			takeovers = append(takeovers, fenceTakeover{tableID: tableID, owner: owner, table: fmt.Sprint(tableID)})
		}
	}
	if err := takeOverFences(ctx, se, takeovers, restoreID); err != nil {
		return err
	}
	fenceRecord := utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, snapclient.MaskingPolicyFenceTableName)
	if err := se.Execute(ctx, "CREATE TABLE IF NOT EXISTS "+fenceRecord+" (table_id BIGINT PRIMARY KEY, restore_id BIGINT UNSIGNED NOT NULL)"); err != nil {
		return errors.Trace(err)
	}
	for _, tableID := range tableIDs {
		if err := se.Execute(ctx, fmt.Sprintf("REPLACE INTO %s VALUES (%d, %d)", fenceRecord, tableID, restoreID)); err != nil {
			return errors.Trace(err)
		}
		tbl, exists, err := loadTableByID(ctx, dom.InfoSchema(), tableID)
		if err != nil {
			return err
		}
		if exists && tbl.Meta().Mode == model.TableModeNormal {
			if err := se.AlterTableMode(ctx, tbl.Meta().DBID, tableID, model.TableModeRestore); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return errors.Trace(dom.Reload())
}

func reopenMaskingPolicyTables(ctx context.Context, dom *domain.Domain, se glue.Session, tableIDs []int64, restoreID uint64) error {
	owners, err := readMaskingPolicyFenceOwners(ctx, dom, se)
	if err != nil {
		return err
	}
	for _, tableID := range tableIDs {
		if owners[tableID] != restoreID || restoreID == 0 {
			return errors.Errorf("masking policy restore does not own the protection of table %d", tableID)
		}
		tbl, exists, err := loadTableByID(ctx, dom.InfoSchema(), tableID)
		if err != nil {
			return err
		}
		if exists && tbl.Meta().Mode == model.TableModeRestore {
			if err := se.AlterTableMode(ctx, tbl.Meta().DBID, tableID, model.TableModeNormal); err != nil {
				return errors.Trace(err)
			}
		}
		if err := se.Execute(ctx, fmt.Sprintf("DELETE FROM %s WHERE table_id = %d AND restore_id = %d",
			utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, snapclient.MaskingPolicyFenceTableName), tableID, restoreID)); err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(dom.Reload())
}

// fenceTakeover is a fence whose recorded owner is no longer registered.
type fenceTakeover struct {
	tableID int64
	owner   uint64
	table   string
}

// takeOverFences makes restoreID the owner of the given fences in one
// transaction. Each record must still name the owner that was found
// unregistered; otherwise nothing is changed.
func takeOverFences(ctx context.Context, se glue.Session, takeovers []fenceTakeover, restoreID uint64) (err error) {
	if len(takeovers) == 0 {
		return nil
	}
	if err := se.Execute(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err != nil {
			if rollbackErr := se.Execute(ctx, "ROLLBACK"); rollbackErr != nil {
				log.Warn("cannot roll back the takeover of masking policy fences", zap.Error(rollbackErr))
			}
		}
	}()
	exec := se.GetSessionCtx().GetRestrictedSQLExecutor()
	internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	for _, t := range takeovers {
		rows, _, err := exec.ExecRestrictedSQL(internalCtx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
			"SELECT restore_id FROM %n.%n WHERE table_id = %? FOR UPDATE",
			snapclient.MaskingPolicyStagingDB, snapclient.MaskingPolicyFenceTableName, t.tableID)
		if err != nil {
			return errors.Annotate(err, "cannot take over a fence left by an unregistered restore task")
		}
		if len(rows) != 1 || rows[0].GetUint64(0) != t.owner {
			return errors.Errorf("the masking policy fence of table %s changed owner while being taken over from restore task %d", t.table, t.owner)
		}
	}
	fenceRecord := utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, snapclient.MaskingPolicyFenceTableName)
	for _, t := range takeovers {
		if err := se.Execute(ctx, fmt.Sprintf("UPDATE %s SET restore_id = %d WHERE table_id = %d AND restore_id = %d",
			fenceRecord, restoreID, t.tableID, t.owner)); err != nil {
			return errors.Annotate(err, "cannot take over a fence left by an unregistered restore task")
		}
	}
	if err := se.Execute(ctx, "COMMIT"); err != nil {
		return errors.Annotate(err, "cannot take over a fence left by an unregistered restore task")
	}
	for _, t := range takeovers {
		log.Info("took over the masking policy fence of an unregistered restore task",
			zap.String("table", t.table), zap.Uint64("previous restore task", t.owner), zap.Uint64("restore task", restoreID))
	}
	return nil
}

// restoredMaskingPolicy is a target-time policy bound to the restored table.
type restoredMaskingPolicy struct {
	tableID, columnID int64
	name              string // lower-cased policy name
	sql               string
}

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

// loadTableByID tells a table that is absent from the schema from one whose
// metadata cannot be loaded. InfoSchema.TableByID reports both as not found,
// and treating the second as a dropped table would lose its policies.
func loadTableByID(ctx context.Context, is infoschema.InfoSchema, tableID int64) (table.Table, bool, error) {
	if tbl, ok := is.TableByID(ctx, tableID); ok {
		return tbl, true, nil
	}
	item, ok := is.TableItemByID(tableID)
	if !ok {
		return nil, false, nil
	}
	tbl, err := is.TableByName(ctx, item.DBName, item.TableName)
	if err != nil {
		return nil, false, errors.Annotatef(err, "cannot load table %s.%s (ID %d) for masking policy restore",
			item.DBName.O, item.TableName.O, tableID)
	}
	return tbl, true, nil
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
func prepareMaskingPolicyStaging(ctx context.Context, dom *domain.Domain, se glue.Session, cfg *LogRestoreConfig, fromFullSnapshot, idMapPersisted bool) error {
	if !cfg.WithSysTable {
		return nil
	}
	for _, db := range cfg.tableMappingManager.DBReplaceMap {
		for sourceID, tbl := range db.TableMap {
			if !isSourceMaskingPolicyTable(db, tbl) {
				continue
			}
			target, ok, err := loadTableByID(ctx, dom.InfoSchema(), tbl.TableID)
			if err != nil {
				return err
			}
			if ok {
				targetDB, exists := dom.InfoSchema().SchemaByID(target.Meta().DBID)
				legacyStage := exists && isTemporaryMaskingPolicyTable(targetDB.Name.O, target.Meta().Name.O)
				privateStage := exists && snapclient.IsMaskingPolicySnapshotTable(targetDB.Name.O, target.Meta().Name.O)
				if legacyStage || privateStage {
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
			if idMapPersisted || !cfg.tableMappingManager.IsTableNewToBase(sourceID, fromFullSnapshot, cfg.StartTS) {
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
			target, err = dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr(snapclient.MaskingPolicyStagingDB), pmodel.NewCIStr(name))
			if err != nil {
				return errors.Trace(err)
			}
			tbl.TableID = target.Meta().ID
		}
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
//
// A table may have to be fenced in Restore mode while its policies are swapped.
// reopensTables tells who switches such a table back to Normal: false when the
// caller does it for every selected table after this function, as a filtered
// restore does; true when nobody else does. This function then reopens exactly
// the tables that the restore task restoreID has fenced, in this attempt or in
// a failed one before it. A restore without a task ID (restoreID 0) cannot be
// told from another one, so it is refused before it has to fence a table.
//
// A task that failed without a persisted checkpoint is unregistered, and its
// rerun gets a new ID. isRegistered lets this task take over the fences of
// such a task, which can never come back to lower them itself. Without it,
// every fence of another task is left alone.
func restoreMaskingPoliciesAfterLogReplay(
	ctx context.Context, dom *domain.Domain, se glue.Session, schemas *stream.SchemasReplace,
	reopensTables bool, restoreID uint64, isRegistered func(context.Context, uint64) (bool, error),
) error {
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
				stage, ok, err := loadTableByID(ctx, is, tbl.TableID)
				if err != nil {
					return err
				}
				if !ok {
					return errors.New("PiTR masking policy staging table disappeared")
				}
				staging[tbl.TableID] = stage.Meta()
				continue
			}
			if db.FilteredOut || tbl.FilteredOut || utils.IsSysOrTempSysDB(tbl.EffectiveDBName(db)) {
				continue
			}
			target, ok, err := loadTableByID(ctx, is, tbl.TableID)
			if err != nil {
				return err
			}
			if ok {
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
	var desired []restoredMaskingPolicy
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
			statement, err := snapclient.BuildCreateMaskingPolicySQL(row.GetString(0), db.Name.O, target.Name.O,
				col.Name.O, expr, row.GetString(4), row.GetString(5))
			if err != nil {
				return errors.Trace(err)
			}
			desired = append(desired, restoredMaskingPolicy{
				tableID: target.ID, columnID: col.ID, name: strings.ToLower(row.GetString(0)), sql: statement,
			})
		}
	}
	// Replace the entire policy set of selected tables, so drops and retries
	// cannot leave a stale baseline policy behind. Preserve unrelated targets.
	policies, err := is.AllMaskingPolicies(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	dropPolicy := func(policy *model.MaskingPolicyInfo) error {
		tbl := targetTables[policy.TableID]
		db, _ := is.SchemaByID(tbl.DBID)
		sql := fmt.Sprintf("ALTER TABLE %s DROP MASKING POLICY %s", utils.EncloseDBAndTable(db.Name.O, tbl.Name.O), utils.EncloseName(policy.Name.O))
		return errors.Trace(se.Execute(ctx, sql))
	}
	createPolicy := func(policy restoredMaskingPolicy) error {
		return errors.Annotate(se.Execute(ctx, policy.sql), "cannot rebuild masking policy after PiTR log replay")
	}
	// The tables may already be readable, and every statement is a separate
	// DDL, so a failure can stop the replacement anywhere. Never leave a
	// readable table without the policies of either the baseline or the target
	// time:
	//   - CREATE OR REPLACE swaps a policy that keeps its name and column
	//     atomically;
	//   - a policy with no target-time counterpart is dropped only after
	//     everything else has succeeded;
	//   - an existing policy that collides with its successor on the column or
	//     on the name has to be dropped first. The table is switched to Restore
	//     mode before that drop, which rejects reads and writes until the
	//     successor exists. A failure keeps the table in Restore mode, and a
	//     retry of the restore resumes from there.
	// The mode of a table does not tell who set it, so the fence is recorded
	// with its restore task before it is raised whenever this function has to
	// lower it again.
	fenceRecord := utils.EncloseDBAndTable(snapclient.MaskingPolicyStagingDB, snapclient.MaskingPolicyFenceTableName)
	readFenceOwners := func() (map[int64]uint64, error) { return readMaskingPolicyFenceOwners(ctx, dom, se) }

	fenced := make(map[int64]struct{})
	fenceTable := func(tableID int64) error {
		tbl := targetTables[tableID]
		if _, ok := fenced[tableID]; ok || tbl.Mode != model.TableModeNormal {
			return nil
		}
		if reopensTables {
			if len(fenced) == 0 {
				if err := se.Execute(ctx, "CREATE TABLE IF NOT EXISTS "+fenceRecord+
					" (table_id BIGINT PRIMARY KEY, restore_id BIGINT UNSIGNED NOT NULL)"); err != nil {
					return errors.Annotate(err, "cannot record tables fenced for masking policy rebuild")
				}
			}
			// The table is readable, so a record left for it is stale.
			if err := se.Execute(ctx, fmt.Sprintf("REPLACE INTO %s VALUES (%d, %d)", fenceRecord, tableID, restoreID)); err != nil {
				return errors.Annotate(err, "cannot record tables fenced for masking policy rebuild")
			}
		}
		if err := se.AlterTableMode(ctx, tbl.DBID, tbl.ID, model.TableModeRestore); err != nil {
			return errors.Annotatef(err, "cannot fence table %s while rebuilding its masking policies", tbl.Name.O)
		}
		fenced[tableID] = struct{}{}
		return nil
	}
	sort.Slice(desired, func(i, j int) bool { return desired[i].sql < desired[j].sql })
	var stale []*model.MaskingPolicyInfo
	blockers := make(map[int][]*model.MaskingPolicyInfo)
	for _, policy := range policies {
		if _, ok := targetTables[policy.TableID]; !ok {
			continue
		}
		blocked, replaced := false, false
		for i, d := range desired {
			if d.tableID != policy.TableID {
				continue
			}
			sameColumn, sameName := d.columnID == policy.ColumnID, d.name == policy.Name.L
			if sameColumn && sameName {
				replaced = true
				break
			}
			if sameColumn || sameName {
				blockers[i] = append(blockers[i], policy)
				blocked = true
			}
		}
		if !replaced && !blocked {
			stale = append(stale, policy)
		}
	}
	if reopensTables {
		// Refuse before changing any policy when the fence of a table could
		// not be attributed to this restore task afterwards.
		owners, err := readFenceOwners()
		if err != nil {
			return err
		}
		var foreign []string
		var takeovers []fenceTakeover
		for tableID, owner := range owners {
			tbl, selected := targetTables[tableID]
			if !selected || tbl.Mode != model.TableModeRestore || (restoreID != 0 && owner == restoreID) {
				continue
			}
			// The record says the table is fenced only because a policy swap
			// was interrupted. When its owner is gone for good, this task
			// rebuilds the policies of the table anyway and may take the
			// fence over.
			if restoreID != 0 && isRegistered != nil {
				registered, err := isRegistered(ctx, owner)
				if err != nil {
					return errors.Trace(err)
				}
				if !registered {
					takeovers = append(takeovers, fenceTakeover{tableID: tableID, owner: owner, table: tbl.Name.O})
					continue
				}
			}
			foreign = append(foreign, fmt.Sprintf("%s (restore task %d)", tbl.Name.O, owner))
		}
		if len(foreign) > 0 {
			sort.Strings(foreign)
			return errors.Errorf("cannot rebuild masking policies: these tables are in Restore mode because "+
				"another restore task was replacing their policies and did not finish: %s", strings.Join(foreign, ", "))
		}
		// Only now that every fence is known to be attributable: record the
		// takeovers first, so a failure of this task leaves fences that its
		// own retry can lower. All of them change owner, or none does.
		if err := takeOverFences(ctx, se, takeovers, restoreID); err != nil {
			return err
		}
		if restoreID == 0 {
			for i, d := range desired {
				if tbl := targetTables[d.tableID]; len(blockers[i]) > 0 && tbl.Mode == model.TableModeNormal {
					return errors.Errorf("cannot replace masking policy %s on table %s: the replacement has to keep the "+
						"table in Restore mode in between, which needs a restore task ID to be undone", blockers[i][0].Name.O, tbl.Name.O)
				}
			}
		}
	}
	for i, d := range desired {
		if len(blockers[i]) == 0 {
			if err := createPolicy(d); err != nil {
				return err
			}
		}
	}
	dropped := make(map[int64]struct{})
	for i, d := range desired {
		if len(blockers[i]) == 0 {
			continue
		}
		if err := fenceTable(d.tableID); err != nil {
			return err
		}
		for _, policy := range blockers[i] {
			if _, ok := dropped[policy.ID]; ok {
				continue
			}
			if err := dropPolicy(policy); err != nil {
				return err
			}
			dropped[policy.ID] = struct{}{}
		}
		if err := createPolicy(d); err != nil {
			return err
		}
	}
	for _, policy := range stale {
		if err := dropPolicy(policy); err != nil {
			return err
		}
	}
	if err := dom.Reload(); err != nil {
		return errors.Trace(err)
	}
	if !reopensTables {
		return nil
	}
	// Every policy is in place. Reopen the tables recorded as fenced by this
	// restore task, in this attempt or in a failed one before it, and no other.
	owners, err := readFenceOwners()
	if err != nil || owners == nil {
		return err
	}
	is = dom.InfoSchema()
	for tableID, owner := range owners {
		// Another task can have recorded an ID before creating its table.
		// Absence therefore does not authorize deleting somebody else's fence.
		if owner != restoreID {
			continue
		}

		// A table that exists but cannot be loaded says nothing about its
		// mode: keep its record.
		tbl, ok, err := loadTableByID(ctx, is, tableID)
		if err != nil {
			return err
		}
		if ok {
			if _, selected := targetTables[tableID]; !selected {
				continue
			}
			if tbl.Meta().Mode == model.TableModeRestore {
				if owner != restoreID {
					continue
				}
				if err := se.AlterTableMode(ctx, tbl.Meta().DBID, tableID, model.TableModeNormal); err != nil {
					return errors.Annotatef(err, "cannot reopen table %s after rebuilding its masking policies", tbl.Meta().Name.O)
				}
			}
		}
		// The table is readable again or dropped: the record has no use. The
		// record table itself is kept: another restore task may be writing its
		// own fences to it, and dropping it could lose them.
		if err := se.Execute(ctx, fmt.Sprintf("DELETE FROM %s WHERE table_id = %d AND restore_id = %d", fenceRecord, tableID, owner)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// configureSnapshotMaskingPolicies intersects the snapshot manifest with this
// restore's selected tables. Legacy backups are conservative unless the policy
// table is provably empty. PiTR must also protect policies introduced in logs.
func configureSnapshotMaskingPolicies(
	cfg *SnapshotRestoreConfig, tables map[int64]*metautil.Table,
	manifest *metautil.MaskingPolicyManifest, isPiTR bool,
) {
	if manifest != nil {
		cfg.maskingPolicyTableIDs = make(map[int64]struct{})
		for _, id := range manifest.TableIDs {
			if _, ok := tables[id]; ok {
				cfg.maskingPolicyTableIDs[id] = struct{}{}
			}
		}
	}
	if isPiTR {
		return
	}
	empty := manifest != nil && len(cfg.maskingPolicyTableIDs) == 0
	for _, tbl := range tables {
		if !isTemporaryMaskingPolicyTable(tbl.DB.Name.O, tbl.Info.Name.O) {
			continue
		}
		hasFiles := false
		for _, files := range tbl.FilesOfPhysicals {
			hasFiles = hasFiles || len(files) > 0
		}
		empty = empty || manifest == nil && tbl.TotalKvs == 0 && tbl.TotalBytes == 0 && !hasFiles
	}
	if !empty {
		return
	}
	cfg.skipSnapshotMaskingPolicy = true
	for id, tbl := range tables {
		if isTemporaryMaskingPolicyTable(tbl.DB.Name.O, tbl.Info.Name.O) {
			delete(tables, id)
		}
	}
}

func maskingPolicySnapshotTableName(restoreID uint64) string {
	if restoreID == 0 {
		return ""
	}
	return snapclient.MaskingPolicySnapshotTableName(restoreID)
}

// isolateMaskingPolicySnapshot changes only the target plan; the backup's
// source names and IDs remain available for policy rebinding and PiTR mapping.
func isolateMaskingPolicySnapshot(plan *restoreNamePlan, cfg *RestoreConfig) error {
	for _, table := range plan.tables {
		if !isTemporaryMaskingPolicyTable(table.Source.DB.Name.O, table.Source.Info.Name.O) {
			continue
		}
		if cfg.RestoreID == 0 {
			return errors.New("masking policy staging requires a restore task ID")
		}
		db := table.TargetDB.Clone()
		db.Name = pmodel.NewCIStr(mysql.SystemDB)
		table.TargetDB = db
		table.TargetInfo.Name = pmodel.NewCIStr(maskingPolicySnapshotTableName(cfg.RestoreID))
		used := false
		for _, database := range plan.databases {
			if database.Target.Name.L == db.Name.L {
				used = true
				break
			}
		}
		if !used {
			plan.databases = append(plan.databases, &restoreutils.DatabaseRestorePlan{
				Source: &metautil.Database{Info: table.Source.DB}, Target: db,
			})
		}
	}
	plan.targetTables = plan.targetTables[:0]
	for _, table := range plan.tables {
		plan.targetTables = append(plan.targetTables, table.TargetTable())
	}
	return nil
}
