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
	"encoding/hex"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore/nameroute"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/stream"
	brutils "github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/filter"
)

type restoreNamePlan struct {
	databases    []*restoreutils.DatabaseRestorePlan
	tables       []*restoreutils.TableRestorePlan
	targetTables []*metautil.Table
}

// restoreNameSources contains the source names visible at the PiTR restore
// point. Snapshot metadata remains immutable; these names are used only to
// resolve a stable target for each upstream object ID.
type restoreNameSources struct {
	databases map[int64]ast.CIStr
	tables    map[int64]nameroute.ObjectName
	objects   []nameroute.ObjectName
}

func (cfg *RestoreConfig) getNameRouter() (*nameroute.Router, error) {
	if cfg.nameRouter != nil {
		return cfg.nameRouter, nil
	}
	router, err := nameroute.Parse(cfg.Rename)
	if err != nil {
		return nil, errors.Annotate(err, "invalid restore rename configuration")
	}
	if err := validateRenameRuleSchemas(router); err != nil {
		return nil, err
	}
	cfg.nameRouter = router
	return router, nil
}

func (cfg *RestoreConfig) hasNameRouting() bool {
	return len(cfg.Rename) > 0
}

func (cfg *RestoreConfig) nameRouteFingerprint() (string, error) {
	if !cfg.hasNameRouting() {
		return "", nil
	}
	router, err := cfg.getNameRouter()
	if err != nil {
		return "", err
	}
	fingerprint := router.Fingerprint()
	return hex.EncodeToString(fingerprint[:]), nil
}

func (cfg *RestoreConfig) validateNameRouting(cmdName string) error {
	if _, err := cfg.getNameRouter(); err != nil {
		return err
	}
	if !cfg.hasNameRouting() {
		return nil
	}
	if cfg.NoSchema {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--%s cannot be used with --%s", FlagRename, flagNoSchema)
	}
	if cfg.WithSysTable {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--%s requires --%s=false", FlagRename, flagWithSysTable)
	}
	if cmdName == RawRestoreCmd || cmdName == TxnRestoreCmd {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--%s is not supported by %s", FlagRename, cmdName)
	}
	if cfg.FullBackupType == FullBackupTypeEBS {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--%s is not supported by EBS restore", FlagRename)
	}
	return nil
}

func validateRenameRuleSchemas(router *nameroute.Router) error {
	for _, rule := range router.Rules() {
		for _, object := range []nameroute.ObjectName{rule.Source, rule.Target} {
			name := object.Schema.L
			if filter.IsSystemSchema(name) || isBRTemporarySchema(name) || isCheckpointSchema(name) {
				return errors.Annotatef(berrors.ErrInvalidArgument,
					"restore rename does not support system, temporary system, or checkpoint schema %s", object.Schema.O)
			}
		}
	}
	return nil
}

func isBRTemporarySchema(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), brutils.TemporaryDBName("").L)
}

func isCheckpointSchema(name string) bool {
	lowerName := strings.ToLower(name)
	return strings.HasPrefix(lowerName, strings.ToLower(checkpoint.LogRestoreCheckpointDatabaseName)) ||
		strings.HasPrefix(lowerName, strings.ToLower(checkpoint.SnapshotRestoreCheckpointDatabaseName)) ||
		strings.HasPrefix(lowerName, strings.ToLower(checkpoint.CustomSSTRestoreCheckpointDatabaseName))
}

func buildRestoreNamePlan(
	router *nameroute.Router,
	dbs []*metautil.Database,
	tables []*metautil.Table,
	sources *restoreNameSources,
) (*restoreNamePlan, error) {
	objects := make([]nameroute.ObjectName, 0, len(dbs)+len(tables))
	if sources != nil {
		objects = sources.objects
	} else {
		for _, db := range dbs {
			objects = append(objects, nameroute.ObjectName{Schema: db.Info.Name})
		}
		for _, table := range tables {
			objects = append(objects, nameroute.ObjectName{Schema: table.DB.Name, Table: table.Info.Name})
		}
	}
	if err := router.ValidateTargets(objects); err != nil {
		return nil, errors.Annotate(err, "invalid restore rename targets")
	}

	matchedRules := make([]bool, len(router.Rules()))
	markMatched := func(schema, table ast.CIStr) {
		for i, rule := range router.Rules() {
			if rule.Source.Schema.L != schema.L {
				continue
			}
			if !rule.Source.IsTable() || rule.Source.Table.L == table.L {
				matchedRules[i] = true
			}
		}
	}
	for _, object := range objects {
		markMatched(object.Schema, object.Table)
	}

	plan := &restoreNamePlan{
		databases:    make([]*restoreutils.DatabaseRestorePlan, 0, len(dbs)),
		tables:       make([]*restoreutils.TableRestorePlan, 0, len(tables)),
		targetTables: make([]*metautil.Table, 0, len(tables)),
	}
	targetDBs := make(map[string]*restoreutils.DatabaseRestorePlan)
	addTargetDB := func(source *metautil.Database, sourceInfo *model.DBInfo, targetName ast.CIStr) (*model.DBInfo, error) {
		if existing, ok := targetDBs[targetName.L]; ok {
			if !databaseRestoreSettingsCompatible(sourceInfo, existing.Source.Info) {
				return nil, errors.Annotatef(berrors.ErrInvalidArgument,
					"source schemas %s and %s routed to target schema %s have incompatible charset, collation, or placement policy",
					existing.Source.Info.Name.O, sourceInfo.Name.O, targetName.O)
			}
			if sourceDBPrecedes(sourceInfo, existing.Source.Info) {
				target := sourceInfo.Clone()
				target.Name = targetName
				*existing.Target = *target
				existing.Source = source
			}
			return existing.Target, nil
		}
		target := sourceInfo.Clone()
		target.Name = targetName
		databasePlan := &restoreutils.DatabaseRestorePlan{Source: source, Target: target}
		targetDBs[targetName.L] = databasePlan
		plan.databases = append(plan.databases, databasePlan)
		return target, nil
	}

	databaseByID := make(map[int64]*metautil.Database, len(dbs))
	for _, db := range dbs {
		databaseByID[db.Info.ID] = db
	}

	tableDBIDs := make(map[int64]struct{}, len(tables))
	for _, table := range tables {
		tableDBIDs[table.DB.ID] = struct{}{}
		sourceObject := nameroute.ObjectName{Schema: table.DB.Name, Table: table.Info.Name}
		if sources != nil {
			if routedSource, ok := sources.tables[table.Info.ID]; ok {
				sourceObject = routedSource
			}
		}
		targetSchema, targetTable, _ := router.Route(sourceObject.Schema, sourceObject.Table)
		if len(router.Rules()) > 0 && table.Info.View != nil {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument,
				"restore rename does not support selected view %s.%s", sourceObject.Schema.O, sourceObject.Table.O)
		}
		if len(router.Rules()) > 0 && len(table.Info.ForeignKeys) > 0 {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument,
				"restore rename does not support selected table %s.%s with foreign keys", sourceObject.Schema.O, sourceObject.Table.O)
		}
		sourceDB := databaseByID[table.DB.ID]
		targetDB, err := addTargetDB(sourceDB, table.DB, targetSchema)
		if err != nil {
			return nil, err
		}
		targetInfo := table.Info.Clone()
		targetInfo.Name = targetTable
		tablePlan := &restoreutils.TableRestorePlan{Source: table, TargetDB: targetDB, TargetInfo: targetInfo}
		plan.tables = append(plan.tables, tablePlan)
		plan.targetTables = append(plan.targetTables, tablePlan.TargetTable())
	}
	// A table route creates only the schemas actually used by its targets. Keep
	// an independent database plan only for a selected empty schema.
	for _, db := range dbs {
		if _, hasSelectedTable := tableDBIDs[db.Info.ID]; hasSelectedTable {
			continue
		}
		sourceSchema := db.Info.Name
		if sources != nil {
			if routedSource, ok := sources.databases[db.Info.ID]; ok {
				sourceSchema = routedSource
			}
		}
		targetSchema, _, _ := router.Route(sourceSchema, ast.CIStr{})
		if _, err := addTargetDB(db, db.Info, targetSchema); err != nil {
			return nil, err
		}
	}

	rules := router.Rules()
	for i, matched := range matchedRules {
		if !matched {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument,
				"restore rename rule for source %s does not match any selected object", formatRouteObject(rules[i].Source))
		}
	}
	return plan, nil
}

func sourceDBPrecedes(candidate, existing *model.DBInfo) bool {
	if candidate.Name.L != existing.Name.L {
		return candidate.Name.L < existing.Name.L
	}
	return candidate.ID < existing.ID
}

func databaseRestoreSettingsCompatible(first, second *model.DBInfo) bool {
	if !strings.EqualFold(first.Charset, second.Charset) || !strings.EqualFold(first.Collate, second.Collate) {
		return false
	}
	if first.PlacementPolicyRef == nil || second.PlacementPolicyRef == nil {
		return first.PlacementPolicyRef == nil && second.PlacementPolicyRef == nil
	}
	return first.PlacementPolicyRef.Name.L == second.PlacementPolicyRef.Name.L
}

func buildPiTRRestoreNameSources(
	history *stream.LogBackupTableHistoryManager,
	tracker *brutils.PiTRIdTracker,
	dbs []*metautil.Database,
	tables []*metautil.Table,
) *restoreNameSources {
	sources := &restoreNameSources{
		databases: make(map[int64]ast.CIStr),
		tables:    make(map[int64]nameroute.ObjectName),
	}
	snapshotDBs := make(map[int64]*metautil.Database, len(dbs))
	for _, db := range dbs {
		snapshotDBs[db.Info.ID] = db
	}
	snapshotTables := make(map[int64]*metautil.Table, len(tables))
	for _, table := range tables {
		snapshotTables[table.Info.ID] = table
	}

	for dbID := range tracker.DBIds {
		if dbName, ok := history.GetDBNameByID(dbID); ok {
			sources.databases[dbID] = ast.NewCIStr(dbName)
		} else if db, ok := snapshotDBs[dbID]; ok {
			sources.databases[dbID] = db.Info.Name
		}
	}
	for tableID := range tracker.TableIdToDBIds {
		if locations, ok := history.GetTableHistory()[tableID]; ok && !locations[1].IsPartition {
			latest := locations[1]
			if dbName, exists := history.GetDBNameByID(latest.DbID); exists {
				sources.tables[tableID] = nameroute.ObjectName{
					Schema: ast.NewCIStr(dbName),
					Table:  ast.NewCIStr(latest.TableName),
				}
				continue
			}
		}
		if table, ok := snapshotTables[tableID]; ok {
			sources.tables[tableID] = nameroute.ObjectName{Schema: table.DB.Name, Table: table.Info.Name}
		}
	}

	dbIDs := make([]int64, 0, len(sources.databases))
	for dbID := range sources.databases {
		dbIDs = append(dbIDs, dbID)
	}
	sort.Slice(dbIDs, func(i, j int) bool { return dbIDs[i] < dbIDs[j] })
	for _, dbID := range dbIDs {
		sources.objects = append(sources.objects, nameroute.ObjectName{Schema: sources.databases[dbID]})
	}
	tableIDs := make([]int64, 0, len(sources.tables))
	for tableID := range sources.tables {
		tableIDs = append(tableIDs, tableID)
	}
	sort.Slice(tableIDs, func(i, j int) bool { return tableIDs[i] < tableIDs[j] })
	for _, tableID := range tableIDs {
		sources.objects = append(sources.objects, sources.tables[tableID])
	}
	return sources
}

func formatRouteObject(object nameroute.ObjectName) string {
	if object.IsTable() {
		return brutils.EncloseDBAndTable(object.Schema.O, object.Table.O)
	}
	return brutils.EncloseName(object.Schema.O)
}

func routeTargetTables(router *nameroute.Router, tables []*metautil.Table) []*metautil.Table {
	targets := make([]*metautil.Table, 0, len(tables))
	for _, table := range tables {
		targetSchema, targetTable, _ := router.Route(table.DB.Name, table.Info.Name)
		targetDBInfo := table.DB.Clone()
		targetDBInfo.Name = targetSchema
		targetInfo := table.Info.Clone()
		targetInfo.Name = targetTable
		targets = append(targets, &metautil.Table{DB: targetDBInfo, Info: targetInfo})
	}
	return targets
}

// applyNameRoutesToTableMapping binds routes to upstream table IDs before the
// snapshot and log phases diverge. The table history's latest location is used
// so an upstream rename or move cannot cause the same table ID to pick multiple
// targets while replaying older metadata.
func applyNameRoutesToTableMapping(
	router *nameroute.Router,
	history *stream.LogBackupTableHistoryManager,
	mapping *stream.TableMappingManager,
) {
	type sourceDB struct {
		name string
	}
	sourceDBs := make(map[int64]sourceDB, len(mapping.DBReplaceMap))
	for dbID, dbReplace := range mapping.DBReplaceMap {
		sourceDBs[dbID] = sourceDB{name: dbReplace.Name}
	}

	for dbID, dbReplace := range mapping.DBReplaceMap {
		sourceName := sourceDBs[dbID].name
		targetSchema, _, schemaRouted := router.Route(ast.NewCIStr(sourceName), ast.CIStr{})
		// DBReplace.Name is persisted in PitrDBMap and marks that a schema-level
		// route owns the parent DB mapping. Exact table routes deliberately leave
		// the source parent name unchanged.
		if schemaRouted {
			dbReplace.Name = targetSchema.O
		}
	}

	tableHistory := history.GetTableHistory()
	for parentDBID, dbReplace := range mapping.DBReplaceMap {
		for tableID, tableReplace := range dbReplace.TableMap {
			sourceDBName := sourceDBs[parentDBID].name
			sourceTableName := tableReplace.Name
			if locations, ok := tableHistory[tableID]; ok {
				latest := locations[1]
				if dbName, exists := history.GetDBNameByID(latest.DbID); exists {
					sourceDBName = dbName
				}
				sourceTableName = latest.TableName
			}
			targetDB, targetTable, matched := router.Route(ast.NewCIStr(sourceDBName), ast.NewCIStr(sourceTableName))
			if !matched {
				continue
			}
			tableReplace.Name = targetTable.O
			tableReplace.TargetDBName = targetDB.O
			if targetDB.L == ast.NewCIStr(dbReplace.Name).L {
				tableReplace.TargetDBID = dbReplace.DbID
				continue
			}
			// A cross-schema route can share its target with more than one
			// historical DBReplace. Leave the ID unresolved here so the mapping
			// manager can bind every same-name alias deterministically.
			tableReplace.TargetDBID = 0
		}
	}
}
