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

package domain

import (
	"archive/zip"
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	domain_metrics "github.com/pingcap/tidb/pkg/domain/metrics"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	// PlanReplayerSQLMetaFile indicates sql meta path for plan replayer
	PlanReplayerSQLMetaFile = "sql_meta.toml"
	// PlanReplayerConfigFile indicates config file path for plan replayer
	PlanReplayerConfigFile = "config.toml"
	// PlanReplayerMetaFile meta file path for plan replayer
	PlanReplayerMetaFile = "meta.txt"
	// PlanReplayerVariablesFile indicates for session variables file path for plan replayer
	PlanReplayerVariablesFile = "variables.toml"
	// PlanReplayerTiFlashReplicasFile indicates for table tiflash replica file path for plan replayer
	PlanReplayerTiFlashReplicasFile = "table_tiflash_replica.txt"
	// PlanReplayerSessionBindingFile indicates session binding file path for plan replayer
	PlanReplayerSessionBindingFile = "session_bindings.sql"
	// PlanReplayerGlobalBindingFile indicates global binding file path for plan replayer
	PlanReplayerGlobalBindingFile = "global_bindings.sql"
	// PlanReplayerSchemaMetaFile indicates the schema meta
	PlanReplayerSchemaMetaFile = "schema_meta.txt"
	// PlanReplayerErrorMessageFile is the file name for error messages
	PlanReplayerErrorMessageFile = "errors.txt"
)

const (
	// PlanReplayerSQLMetaStartTS indicates the startTS in plan replayer sql meta
	PlanReplayerSQLMetaStartTS = "startTS"
	// PlanReplayerTaskMetaIsCapture indicates whether this task is capture task
	PlanReplayerTaskMetaIsCapture = "isCapture"
	// PlanReplayerTaskMetaIsContinues indicates whether this task is continues task
	PlanReplayerTaskMetaIsContinues = "isContinues"
	// PlanReplayerTaskMetaSQLDigest indicates the sql digest of this task
	PlanReplayerTaskMetaSQLDigest = "sqlDigest"
	// PlanReplayerTaskMetaPlanDigest indicates the plan digest of this task
	PlanReplayerTaskMetaPlanDigest = "planDigest"
	// PlanReplayerTaskEnableHistoricalStats indicates whether the task is using historical stats
	PlanReplayerTaskEnableHistoricalStats = "enableHistoricalStats"
	// PlanReplayerHistoricalStatsTS indicates the expected TS of the historical stats if it's specified by the user.
	PlanReplayerHistoricalStatsTS = "historicalStatsTS"
)

type tableNamePair struct {
	DBName    string
	TableName string
	IsView    bool
}

type tableNameExtractor struct {
	ctx      context.Context
	executor sqlexec.RestrictedSQLExecutor
	is       infoschema.InfoSchema
	curDB    ast.CIStr
	names    map[tableNamePair]struct{}
	cteNames map[string]struct{}
	err      error
}

func (tne *tableNameExtractor) getTablesAndViews() (map[tableNamePair]struct{}, error) {
	r := make(map[tableNamePair]struct{})
	for tablePair := range tne.names {
		if tablePair.IsView {
			r[tablePair] = struct{}{}
			continue
		}
		// remove cte in table names
		_, ok := tne.cteNames[tablePair.TableName]
		if !ok {
			r[tablePair] = struct{}{}
		}
		// if the table has a foreign key, we need to add the referenced table to the list
		err := findFK(tne.is, tablePair.DBName, tablePair.TableName, r)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func findFK(is infoschema.InfoSchema, dbName, tableName string, tableMap map[tableNamePair]struct{}) error {
	tblInfo, err := is.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tableName))
	if err != nil {
		return err
	}
	for _, fk := range tblInfo.Meta().ForeignKeys {
		key := tableNamePair{
			DBName:    fk.RefSchema.L,
			TableName: fk.RefTable.L,
			IsView:    false,
		}
		// Skip already visited tables to prevent infinite recursion in case of circular foreign key definitions.
		if _, ok := tableMap[key]; ok {
			continue
		}
		tableMap[key] = struct{}{}
		err := findFK(is, key.DBName, key.TableName, tableMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (*tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*ast.TableName); ok {
		return in, true
	}
	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if tne.err != nil {
		return in, true
	}
	if t, ok := in.(*ast.TableName); ok {
		isView, err := tne.handleIsView(t)
		if err != nil {
			tne.err = err
			return in, true
		}
		schema := t.Schema
		if schema.L == "" {
			schema = tne.curDB
		}
		if tne.is.TableExists(schema, t.Name) {
			tp := tableNamePair{DBName: schema.L, TableName: t.Name.L, IsView: isView}
			tne.names[tp] = struct{}{}
		}
	} else if s, ok := in.(*ast.SelectStmt); ok {
		if s.With != nil && len(s.With.CTEs) > 0 {
			for _, cte := range s.With.CTEs {
				tne.cteNames[cte.Name.L] = struct{}{}
			}
		}
	}
	return in, true
}

func (tne *tableNameExtractor) handleIsView(t *ast.TableName) (bool, error) {
	schema := t.Schema
	if schema.L == "" {
		schema = tne.curDB
	}
	table := t.Name
	isView := infoschema.TableIsView(tne.is, schema, table)
	if !isView {
		return false, nil
	}
	viewTbl, err := tne.is.TableByName(context.Background(), schema, table)
	if err != nil {
		return false, err
	}
	sql := viewTbl.Meta().View.SelectStmt
	node, err := tne.executor.ParseWithParams(tne.ctx, sql)
	if err != nil {
		return false, err
	}
	node.Accept(tne)
	return true, nil
}

// DumpPlanReplayerInfo will dump the information about sqls.
// The files will be organized into the following format:
//
// Single SQL dump:
//
//	|-sql_meta.toml
//	|-meta.txt
//	|-schema/...
//	|-view/...
//	|-stats/...
//	|-statsMem/...
//	|-config.toml
//	|-table_tiflash_replica.txt
//	|-variables.toml
//	|-bindings.sql
//	|-sql/sql0.sql
//	|-explain.txt
//
// Multiple SQL dump (PLAN REPLAYER DUMP EXPLAIN ( "sql1", "sql2", ... )):
//
//	|-(same as above)
//	|-sql/sql0.sql
//	|-sql/sql1.sql
//	|-...
//	|-explain/explain0.txt
//	|-explain/explain1.txt
//	|-...
func DumpPlanReplayerInfo(ctx context.Context, sctx sessionctx.Context,
	task *PlanReplayerDumpTask,
) (err error) {
	zf := task.Zf
	fileName := task.FileName
	sessionVars := task.SessionVars
	execStmts := task.ExecStmts
	zw := zip.NewWriter(zf)
	var records []PlanReplayerStatusRecord
	var errMsgs []string
	sqls := make([]string, 0)
	for _, execStmt := range task.ExecStmts {
		sqls = append(sqls, execStmt.Text())
	}
	if task.IsCapture {
		logutil.BgLogger().Info("start to dump plan replayer result", zap.String("category", "plan-replayer-dump"),
			zap.String("sql-digest", task.SQLDigest),
			zap.String("plan-digest", task.PlanDigest),
			zap.Strings("sql", sqls),
			zap.Bool("isContinues", task.IsContinuesCapture))
	} else {
		logutil.BgLogger().Info("start to dump plan replayer result", zap.String("category", "plan-replayer-dump"),
			zap.Strings("sqls", sqls))
	}
	defer func() {
		errMsg := ""
		if err != nil {
			if task.IsCapture {
				logutil.BgLogger().Info("dump file failed", zap.String("category", "plan-replayer-dump"),
					zap.String("sql-digest", task.SQLDigest),
					zap.String("plan-digest", task.PlanDigest),
					zap.Strings("sql", sqls),
					zap.Bool("isContinues", task.IsContinuesCapture))
			} else {
				logutil.BgLogger().Info("start to dump plan replayer result", zap.String("category", "plan-replayer-dump"),
					zap.Strings("sqls", sqls))
			}
			errMsg = err.Error()
			domain_metrics.PlanReplayerDumpTaskFailed.Inc()
		} else {
			domain_metrics.PlanReplayerDumpTaskSuccess.Inc()
		}
		err1 := zw.Close()
		if err1 != nil {
			logutil.BgLogger().Warn("Closing zip writer failed", zap.String("category", "plan-replayer-dump"), zap.Error(err1), zap.String("filename", fileName))
			errMsg = errMsg + "," + err1.Error()
		}
		err2 := zf.Close()
		if err2 != nil {
			logutil.BgLogger().Warn("Closing zip file failed", zap.String("category", "plan-replayer-dump"), zap.Error(err2), zap.String("filename", fileName))
			errMsg = errMsg + "," + err2.Error()
		}
		if len(errMsg) > 0 {
			for i, record := range records {
				record.FailedReason = errMsg
				records[i] = record
			}
		}
		insertPlanReplayerStatus(ctx, sctx, records)
	}()
	// Dump SQLMeta
	if err = dumpSQLMeta(zw, task); err != nil {
		return err
	}

	// Dump config
	if err = dumpConfig(zw); err != nil {
		return err
	}

	// Dump meta
	if err = dumpMeta(zw); err != nil {
		return err
	}
	// Retrieve current DB
	dbName := ast.NewCIStr(sessionVars.CurrentDB)
	do := GetDomain(sctx)

	// Retrieve all tables
	pairs, err := extractTableNames(ctx, sctx, execStmts, dbName)
	if err != nil {
		return errors.AddStack(fmt.Errorf("plan replayer: invalid SQL text, err: %v", err))
	}

	// Dump Schema and View
	if err = dumpSchemas(sctx, zw, pairs); err != nil {
		return err
	}

	// Dump tables tiflash replicas
	if err = dumpTiFlashReplica(sctx, zw, pairs); err != nil {
		return err
	}

	// For continuous capture task, we dump stats in storage only if EnableHistoricalStatsForCapture is disabled.
	// For manual plan replayer dump command or capture, we directly dump stats in storage
	if task.IsCapture && task.IsContinuesCapture {
		if !vardef.EnableHistoricalStatsForCapture.Load() {
			// Dump stats
			fallbackMsg, err := dumpStats(zw, pairs, do, 0)
			if err != nil {
				return err
			}
			if len(fallbackMsg) > 0 {
				errMsgs = append(errMsgs, fallbackMsg)
			}
		} else {
			failpoint.Inject("shouldDumpStats", func(val failpoint.Value) {
				if val.(bool) {
					panic("shouldDumpStats")
				}
			})
		}
	} else {
		// Dump stats
		fallbackMsg, err := dumpStats(zw, pairs, do, task.HistoricalStatsTS)
		if err != nil {
			return err
		}
		if len(fallbackMsg) > 0 {
			errMsgs = append(errMsgs, fallbackMsg)
		}
	}

	if err = dumpStatsMemStatus(zw, pairs, do); err != nil {
		return err
	}

	// Dump variables
	if err = dumpVariables(sctx, sessionVars, zw); err != nil {
		return err
	}

	// Dump sql
	if err = dumpSQLs(execStmts, zw); err != nil {
		return err
	}

	// Dump session bindings
	if len(task.SessionBindings) > 0 {
		if err = dumpSessionBindRecords(task.SessionBindings, zw); err != nil {
			return err
		}
	} else {
		if err = dumpSessionBindings(sctx, zw); err != nil {
			return err
		}
	}

	// Dump global bindings
	if err = dumpGlobalBindings(sctx, zw); err != nil {
		return err
	}

	if len(task.EncodedPlan) > 0 {
		records = generateRecords(task)
		if err = dumpEncodedPlan(sctx, zw, task.EncodedPlan); err != nil {
			return err
		}
	} else {
		// Dump explain
		if err = dumpPlanReplayerExplain(sctx, zw, task, &records); err != nil {
			errMsgs = append(errMsgs, err.Error())
		}
	}

	if err = dumpDebugTrace(zw, task.DebugTrace); err != nil {
		return err
	}

	if len(errMsgs) > 0 {
		if err = dumpErrorMsgs(zw, errMsgs); err != nil {
			return err
		}
	}
	return nil
}

func generateRecords(task *PlanReplayerDumpTask) []PlanReplayerStatusRecord {
	records := make([]PlanReplayerStatusRecord, 0)
	if len(task.ExecStmts) > 0 {
		for _, execStmt := range task.ExecStmts {
			records = append(records, PlanReplayerStatusRecord{
				SQLDigest:  task.SQLDigest,
				PlanDigest: task.PlanDigest,
				OriginSQL:  execStmt.Text(),
				Token:      task.FileName,
			})
		}
	}
	return records
}

