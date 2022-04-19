// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"go.uber.org/zap"
)

// DB is a TiDB instance, not thread-safe.
type DB struct {
	se glue.Session
}

type UniqueTableName struct {
	DB    string
	Table string
}

type DDLJobFilterRule func(ddlJob *model.Job) bool

var incrementalRestoreActionBlockList = map[model.ActionType]struct{}{
	model.ActionSetTiFlashReplica:          {},
	model.ActionUpdateTiFlashReplicaStatus: {},
	model.ActionLockTable:                  {},
	model.ActionUnlockTable:                {},
}

// NewDB returns a new DB.
func NewDB(g glue.Glue, store kv.Storage, policyMode string) (*DB, bool, error) {
	se, err := g.CreateSession(store)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	// The session may be nil in raw kv mode
	if se == nil {
		return nil, false, nil
	}
	// Set SQL mode to None for avoiding SQL compatibility problem
	err = se.Execute(context.Background(), "set @@sql_mode=''")
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	supportPolicy := false
	if len(policyMode) != 0 {
		// Set placement mode for handle placement policy.
		err = se.Execute(context.Background(), fmt.Sprintf("set @@tidb_placement_mode='%s';", policyMode))
		if err != nil {
			if variable.ErrUnknownSystemVar.Equal(err) {
				// not support placement policy, just ignore it
				log.Warn("target tidb not support tidb_placement_mode, ignore create policies", zap.Error(err))
			} else {
				return nil, false, errors.Trace(err)
			}
		} else {
			log.Info("set tidb_placement_mode success", zap.String("mode", policyMode))
			supportPolicy = true
		}
	}
	return &DB{
		se: se,
	}, supportPolicy, nil
}

// ExecDDL executes the query of a ddl job.
func (db *DB) ExecDDL(ctx context.Context, ddlJob *model.Job) error {
	var err error
	tableInfo := ddlJob.BinlogInfo.TableInfo
	dbInfo := ddlJob.BinlogInfo.DBInfo
	switch ddlJob.Type {
	case model.ActionCreateSchema:
		err = db.se.CreateDatabase(ctx, dbInfo)
		if err != nil {
			log.Error("create database failed", zap.Stringer("db", dbInfo.Name), zap.Error(err))
		}
		return errors.Trace(err)
	case model.ActionCreateTable:
		err = db.se.CreateTable(ctx, model.NewCIStr(ddlJob.SchemaName), tableInfo)
		if err != nil {
			log.Error("create table failed",
				zap.Stringer("db", dbInfo.Name),
				zap.Stringer("table", tableInfo.Name),
				zap.Error(err))
		}
		return errors.Trace(err)
	}

	if ddlJob.Query == "" {
		log.Warn("query of ddl job is empty, ignore it",
			zap.Stringer("type", ddlJob.Type),
			zap.String("db", ddlJob.SchemaName))
		return nil
	}

	if tableInfo != nil {
		switchDBSQL := fmt.Sprintf("use %s;", utils.EncloseName(ddlJob.SchemaName))
		err = db.se.Execute(ctx, switchDBSQL)
		if err != nil {
			log.Error("switch db failed",
				zap.String("query", switchDBSQL),
				zap.String("db", ddlJob.SchemaName),
				zap.Error(err))
			return errors.Trace(err)
		}
	}
	err = db.se.Execute(ctx, ddlJob.Query)
	if err != nil {
		log.Error("execute ddl query failed",
			zap.String("query", ddlJob.Query),
			zap.String("db", ddlJob.SchemaName),
			zap.Int64("historySchemaVersion", ddlJob.BinlogInfo.SchemaVersion),
			zap.Error(err))
	}
	return errors.Trace(err)
}

// UpdateStatsMeta update count and snapshot ts in mysql.stats_meta
func (db *DB) UpdateStatsMeta(ctx context.Context, tableID int64, restoreTS uint64, count uint64) error {
	sysDB := mysql.SystemDB
	statsMetaTbl := "stats_meta"

	// set restoreTS to snapshot and version which is used to update stats_meta
	err := db.se.ExecuteInternal(
		ctx,
		"update %n.%n set snapshot = %?, version = %?, count = %? where table_id = %?",
		sysDB,
		statsMetaTbl,
		restoreTS,
		restoreTS,
		count,
		tableID,
	)
	if err != nil {
		log.Error("execute update sql failed", zap.Error(err))
	}
	return nil
}

// CreatePlacementPolicy check whether cluster support policy and create the policy.
func (db *DB) CreatePlacementPolicy(ctx context.Context, policy *model.PolicyInfo) error {
	err := db.se.CreatePlacementPolicy(ctx, policy)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("create placement policy succeed", zap.Stringer("name", policy.Name))
	return nil
}

// CreateDatabase executes a CREATE DATABASE SQL.
func (db *DB) CreateDatabase(ctx context.Context, schema *model.DBInfo) error {
	err := db.se.CreateDatabase(ctx, schema)
	if err != nil {
		log.Error("create database failed", zap.Stringer("db", schema.Name), zap.Error(err))
	}
	return errors.Trace(err)
}

//
func (db *DB) restoreSequence(ctx context.Context, table *metautil.Table) error {
	var restoreMetaSQL string
	var err error
	if table.Info.IsSequence() {
		setValFormat := fmt.Sprintf("do setval(%s.%s, %%d);",
			utils.EncloseName(table.DB.Name.O),
			utils.EncloseName(table.Info.Name.O))
		if table.Info.Sequence.Cycle {
			increment := table.Info.Sequence.Increment
			// TiDB sequence's behaviour is designed to keep the same pace
			// among all nodes within the same cluster. so we need restore round.
			// Here is a hack way to trigger sequence cycle round > 0 according to
			// https://github.com/pingcap/br/pull/242#issuecomment-631307978
			// TODO use sql to set cycle round
			nextSeqSQL := fmt.Sprintf("do nextval(%s.%s);",
				utils.EncloseName(table.DB.Name.O),
				utils.EncloseName(table.Info.Name.O))
			var setValSQL string
			if increment < 0 {
				setValSQL = fmt.Sprintf(setValFormat, table.Info.Sequence.MinValue)
			} else {
				setValSQL = fmt.Sprintf(setValFormat, table.Info.Sequence.MaxValue)
			}
			err = db.se.Execute(ctx, setValSQL)
			if err != nil {
				log.Error("restore meta sql failed",
					zap.String("query", setValSQL),
					zap.Stringer("db", table.DB.Name),
					zap.Stringer("table", table.Info.Name),
					zap.Error(err))
				return errors.Trace(err)
			}
			// trigger cycle round > 0
			err = db.se.Execute(ctx, nextSeqSQL)
			if err != nil {
				log.Error("restore meta sql failed",
					zap.String("query", nextSeqSQL),
					zap.Stringer("db", table.DB.Name),
					zap.Stringer("table", table.Info.Name),
					zap.Error(err))
				return errors.Trace(err)
			}
		}
		restoreMetaSQL = fmt.Sprintf(setValFormat, table.Info.AutoIncID)
		err = db.se.Execute(ctx, restoreMetaSQL)
	}
	if err != nil {
		log.Error("restore meta sql failed",
			zap.String("query", restoreMetaSQL),
			zap.Stringer("db", table.DB.Name),
			zap.Stringer("table", table.Info.Name),
			zap.Error(err))
		return errors.Trace(err)
	}
	return errors.Trace(err)
}

func (db *DB) CreateTablePostRestore(ctx context.Context, table *metautil.Table, toBeCorrectedTables map[UniqueTableName]bool) error {

	var restoreMetaSQL string
	var err error
	switch {
	case table.Info.IsView():
		return nil
	case table.Info.IsSequence():
		err = db.restoreSequence(ctx, table)
		if err != nil {
			return errors.Trace(err)
		}
	// only table exists in restored cluster during incremental restoration should do alter after creation.
	case toBeCorrectedTables[UniqueTableName{table.DB.Name.String(), table.Info.Name.String()}]:
		if utils.NeedAutoID(table.Info) {
			restoreMetaSQL = fmt.Sprintf(
				"alter table %s.%s auto_increment = %d;",
				utils.EncloseName(table.DB.Name.O),
				utils.EncloseName(table.Info.Name.O),
				table.Info.AutoIncID)
		} else if table.Info.PKIsHandle && table.Info.ContainsAutoRandomBits() {
			restoreMetaSQL = fmt.Sprintf(
				"alter table %s.%s auto_random_base = %d",
				utils.EncloseName(table.DB.Name.O),
				utils.EncloseName(table.Info.Name.O),
				table.Info.AutoRandID)
		} else {
			log.Info("table exists in incremental ddl jobs, but don't need to be altered",
				zap.Stringer("db", table.DB.Name),
				zap.Stringer("table", table.Info.Name))
			return nil
		}
		err = db.se.Execute(ctx, restoreMetaSQL)
		if err != nil {
			log.Error("restore meta sql failed",
				zap.String("query", restoreMetaSQL),
				zap.Stringer("db", table.DB.Name),
				zap.Stringer("table", table.Info.Name),
				zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

// CreateTables execute a internal CREATE TABLES.
func (db *DB) CreateTables(ctx context.Context, tables []*metautil.Table,
	ddlTables map[UniqueTableName]bool, supportPolicy bool, policyMap *sync.Map) error {
	if batchSession, ok := db.se.(glue.BatchCreateTableSession); ok {
		m := map[string][]*model.TableInfo{}
		for _, table := range tables {
			m[table.DB.Name.L] = append(m[table.DB.Name.L], table.Info)
			if !supportPolicy {
				log.Info("set placementPolicyRef to nil when target tidb not support policy",
					zap.Stringer("table", table.Info.Name), zap.Stringer("db", table.DB.Name))
				table.Info.ClearPlacement()
			} else {
				if err := db.ensureTablePlacementPolicies(ctx, table.Info, policyMap); err != nil {
					return errors.Trace(err)
				}
			}
		}
		if err := batchSession.CreateTables(ctx, m); err != nil {
			return err
		}

		for _, table := range tables {
			err := db.CreateTablePostRestore(ctx, table, ddlTables)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// CreateTable executes a CREATE TABLE SQL.
func (db *DB) CreateTable(ctx context.Context, table *metautil.Table,
	ddlTables map[UniqueTableName]bool, supportPolicy bool, policyMap *sync.Map) error {
	if !supportPolicy {
		log.Info("set placementPolicyRef to nil when target tidb not support policy",
			zap.Stringer("table", table.Info.Name), zap.Stringer("db", table.DB.Name))
		table.Info.ClearPlacement()
	} else {
		if err := db.ensureTablePlacementPolicies(ctx, table.Info, policyMap); err != nil {
			return errors.Trace(err)
		}
	}

	err := db.se.CreateTable(ctx, table.DB.Name, table.Info)
	if err != nil {
		log.Error("create table failed",
			zap.Stringer("db", table.DB.Name),
			zap.Stringer("table", table.Info.Name),
			zap.Error(err))
		return errors.Trace(err)
	}

	err = db.CreateTablePostRestore(ctx, table, ddlTables)
	if err != nil {
		return errors.Trace(err)
	}

	return err
}

// Close closes the connection.
func (db *DB) Close() {
	db.se.Close()
}

func (db *DB) ensurePlacementPolicy(ctx context.Context, policyName model.CIStr, policies *sync.Map) error {
	if policies == nil {
		return nil
	}

	if policy, ok := policies.LoadAndDelete(policyName.L); ok {
		return db.CreatePlacementPolicy(ctx, policy.(*model.PolicyInfo))
	}

	// This means policy already created
	return nil
}

func (db *DB) ensureTablePlacementPolicies(ctx context.Context, tableInfo *model.TableInfo, policies *sync.Map) error {
	if tableInfo.PlacementPolicyRef != nil {
		if err := db.ensurePlacementPolicy(ctx, tableInfo.PlacementPolicyRef.Name, policies); err != nil {
			return err
		}
	}

	if tableInfo.Partition != nil {
		for _, def := range tableInfo.Partition.Definitions {
			if def.PlacementPolicyRef == nil {
				continue
			}

			if err := db.ensurePlacementPolicy(ctx, def.PlacementPolicyRef.Name, policies); err != nil {
				return err
			}
		}
	}

	return nil
}

// FilterDDLJobs filters ddl jobs.
func FilterDDLJobs(allDDLJobs []*model.Job, tables []*metautil.Table) (ddlJobs []*model.Job) {
	// Sort the ddl jobs by schema version in descending order.
	sort.Slice(allDDLJobs, func(i, j int) bool {
		return allDDLJobs[i].BinlogInfo.SchemaVersion > allDDLJobs[j].BinlogInfo.SchemaVersion
	})
	dbs := getDatabases(tables)
	for _, db := range dbs {
		// These maps is for solving some corner case.
		// e.g. let "t=2" indicates that the id of database "t" is 2, if the ddl execution sequence is:
		// rename "a" to "b"(a=1) -> drop "b"(b=1) -> create "b"(b=2) -> rename "b" to "a"(a=2)
		// Which we cannot find the "create" DDL by name and id directly.
		// To cover †his case, we must find all names and ids the database/table ever had.
		dbIDs := make(map[int64]bool)
		dbIDs[db.ID] = true
		dbNames := make(map[string]bool)
		dbNames[db.Name.String()] = true
		for _, job := range allDDLJobs {
			if job.BinlogInfo.DBInfo != nil {
				if dbIDs[job.SchemaID] || dbNames[job.BinlogInfo.DBInfo.Name.String()] {
					ddlJobs = append(ddlJobs, job)
					// The the jobs executed with the old id, like the step 2 in the example above.
					dbIDs[job.SchemaID] = true
					// For the jobs executed after rename, like the step 3 in the example above.
					dbNames[job.BinlogInfo.DBInfo.Name.String()] = true
				}
			}
		}
	}

	for _, table := range tables {
		tableIDs := make(map[int64]bool)
		tableIDs[table.Info.ID] = true
		tableNames := make(map[UniqueTableName]bool)
		name := UniqueTableName{table.DB.Name.String(), table.Info.Name.String()}
		tableNames[name] = true
		for _, job := range allDDLJobs {
			if job.BinlogInfo.TableInfo != nil {
				name = UniqueTableName{job.SchemaName, job.BinlogInfo.TableInfo.Name.String()}
				if tableIDs[job.TableID] || tableNames[name] {
					ddlJobs = append(ddlJobs, job)
					tableIDs[job.TableID] = true
					// For truncate table, the id may be changed
					tableIDs[job.BinlogInfo.TableInfo.ID] = true
					tableNames[name] = true
				}
			}
		}
	}
	return ddlJobs
}

// FilterDDLJobByRules if one of rules returns true, the job in srcDDLJobs will be filtered.
func FilterDDLJobByRules(srcDDLJobs []*model.Job, rules ...DDLJobFilterRule) (dstDDLJobs []*model.Job) {
	dstDDLJobs = make([]*model.Job, 0, len(srcDDLJobs))
	for _, ddlJob := range srcDDLJobs {
		passed := true
		for _, rule := range rules {
			if rule(ddlJob) {
				passed = false
				break
			}
		}

		if passed {
			dstDDLJobs = append(dstDDLJobs, ddlJob)
		}
	}

	return
}

// DDLJobBlockListRule rule for filter ddl job with type in block list.
func DDLJobBlockListRule(ddlJob *model.Job) bool {
	return checkIsInActions(ddlJob.Type, incrementalRestoreActionBlockList)
}

func getDatabases(tables []*metautil.Table) (dbs []*model.DBInfo) {
	dbIDs := make(map[int64]bool)
	for _, table := range tables {
		if !dbIDs[table.DB.ID] {
			dbs = append(dbs, table.DB)
			dbIDs[table.DB.ID] = true
		}
	}
	return
}

func checkIsInActions(action model.ActionType, actions map[model.ActionType]struct{}) bool {
	_, ok := actions[action]
	return ok
}
