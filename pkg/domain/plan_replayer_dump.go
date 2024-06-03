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
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	domain_metrics "github.com/pingcap/tidb/pkg/domain/metrics"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/printer"
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
	curDB    model.CIStr
	names    map[tableNamePair]struct{}
	cteNames map[string]struct{}
	err      error
}

func (tne *tableNameExtractor) getTablesAndViews() map[tableNamePair]struct{} {
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
	}
	return r
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
		if tne.is.TableExists(t.Schema, t.Name) {
			tp := tableNamePair{DBName: t.Schema.L, TableName: t.Name.L, IsView: isView}
			if tp.DBName == "" {
				tp.DBName = tne.curDB.L
			}
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
	viewTbl, err := tne.is.TableByName(schema, table)
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
/*
 |-sql_meta.toml
 |-meta.txt
 |-schema
 |	 |-schema_meta.txt
 |	 |-db1.table1.schema.txt
 |	 |-db2.table2.schema.txt
 |	 |-....
 |-view
 | 	 |-db1.view1.view.txt
 |	 |-db2.view2.view.txt
 |	 |-....
 |-stats
 |   |-stats1.json
 |   |-stats2.json
 |   |-....
 |-statsMem
 |   |-stats1.txt
 |   |-stats2.txt
 |   |-....
 |-config.toml
 |-table_tiflash_replica.txt
 |-variables.toml
 |-bindings.sql
 |-sql
 |   |-sql1.sql
 |   |-sql2.sql
 |	 |-....
 |-explain.txt
*/
func DumpPlanReplayerInfo(ctx context.Context, sctx sessionctx.Context,
	task *PlanReplayerDumpTask) (err error) {
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
			logutil.BgLogger().Error("Closing zip writer failed", zap.String("category", "plan-replayer-dump"), zap.Error(err), zap.String("filename", fileName))
			errMsg = errMsg + "," + err1.Error()
		}
		err2 := zf.Close()
		if err2 != nil {
			logutil.BgLogger().Error("Closing zip file failed", zap.String("category", "plan-replayer-dump"), zap.Error(err), zap.String("filename", fileName))
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
	dbName := model.NewCIStr(sessionVars.CurrentDB)
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
		if !variable.EnableHistoricalStatsForCapture.Load() {
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
			return err
		}
	}

	if task.DebugTrace != nil {
		if err = dumpDebugTrace(zw, task.DebugTrace); err != nil {
			return err
		}
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

func dumpSQLMeta(zw *zip.Writer, task *PlanReplayerDumpTask) error {
	cf, err := zw.Create(PlanReplayerSQLMetaFile)
	if err != nil {
		return errors.AddStack(err)
	}
	varMap := make(map[string]string)
	varMap[PlanReplayerSQLMetaStartTS] = strconv.FormatUint(task.StartTS, 10)
	varMap[PlanReplayerTaskMetaIsCapture] = strconv.FormatBool(task.IsCapture)
	varMap[PlanReplayerTaskMetaIsContinues] = strconv.FormatBool(task.IsContinuesCapture)
	varMap[PlanReplayerTaskMetaSQLDigest] = task.SQLDigest
	varMap[PlanReplayerTaskMetaPlanDigest] = task.PlanDigest
	varMap[PlanReplayerTaskEnableHistoricalStats] = strconv.FormatBool(variable.EnableHistoricalStatsForCapture.Load())
	if task.HistoricalStatsTS > 0 {
		varMap[PlanReplayerHistoricalStatsTS] = strconv.FormatUint(task.HistoricalStatsTS, 10)
	}
	if err := toml.NewEncoder(cf).Encode(varMap); err != nil {
		return errors.AddStack(err)
	}
	return nil
}

func dumpConfig(zw *zip.Writer) error {
	cf, err := zw.Create(PlanReplayerConfigFile)
	if err != nil {
		return errors.AddStack(err)
	}
	if err := toml.NewEncoder(cf).Encode(config.GetGlobalConfig()); err != nil {
		return errors.AddStack(err)
	}
	return nil
}

func dumpMeta(zw *zip.Writer) error {
	mt, err := zw.Create(PlanReplayerMetaFile)
	if err != nil {
		return errors.AddStack(err)
	}
	_, err = mt.Write([]byte(printer.GetTiDBInfo()))
	if err != nil {
		return errors.AddStack(err)
	}
	return nil
}

func dumpTiFlashReplica(ctx sessionctx.Context, zw *zip.Writer, pairs map[tableNamePair]struct{}) error {
	bf, err := zw.Create(PlanReplayerTiFlashReplicasFile)
	if err != nil {
		return errors.AddStack(err)
	}
	is := GetDomain(ctx).InfoSchema()
	for pair := range pairs {
		dbName := model.NewCIStr(pair.DBName)
		tableName := model.NewCIStr(pair.TableName)
		t, err := is.TableByName(dbName, tableName)
		if err != nil {
			logutil.BgLogger().Warn("failed to find table info", zap.Error(err),
				zap.String("dbName", dbName.L), zap.String("tableName", tableName.L))
			continue
		}
		if t.Meta().TiFlashReplica != nil && t.Meta().TiFlashReplica.Count > 0 {
			row := []string{
				pair.DBName, pair.TableName, strconv.FormatUint(t.Meta().TiFlashReplica.Count, 10),
			}
			fmt.Fprintf(bf, "%s\n", strings.Join(row, "\t"))
		}
	}
	return nil
}

func dumpSchemas(ctx sessionctx.Context, zw *zip.Writer, pairs map[tableNamePair]struct{}) error {
	tables := make(map[tableNamePair]struct{})
	for pair := range pairs {
		err := getShowCreateTable(pair, zw, ctx)
		if err != nil {
			return err
		}
		if !pair.IsView {
			tables[pair] = struct{}{}
		}
	}
	return dumpSchemaMeta(zw, tables)
}

func dumpSchemaMeta(zw *zip.Writer, tables map[tableNamePair]struct{}) error {
	zf, err := zw.Create(fmt.Sprintf("schema/%v", PlanReplayerSchemaMetaFile))
	if err != nil {
		return err
	}
	for table := range tables {
		_, err := fmt.Fprintf(zf, "%s.%s;", table.DBName, table.TableName)
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpStatsMemStatus(zw *zip.Writer, pairs map[tableNamePair]struct{}, do *Domain) error {
	statsHandle := do.StatsHandle()
	is := do.InfoSchema()
	for pair := range pairs {
		if pair.IsView {
			continue
		}
		tbl, err := is.TableByName(model.NewCIStr(pair.DBName), model.NewCIStr(pair.TableName))
		if err != nil {
			return err
		}
		tblStats := statsHandle.GetTableStats(tbl.Meta())
		if tblStats == nil {
			continue
		}
		statsMemFw, err := zw.Create(fmt.Sprintf("statsMem/%v.%v.txt", pair.DBName, pair.TableName))
		if err != nil {
			return errors.AddStack(err)
		}
		fmt.Fprintf(statsMemFw, "[INDEX]\n")
		for _, indice := range tblStats.Indices {
			fmt.Fprintf(statsMemFw, "%s\n", fmt.Sprintf("%s=%s", indice.Info.Name.String(), indice.StatusToString()))
		}
		fmt.Fprintf(statsMemFw, "[COLUMN]\n")
		for _, col := range tblStats.Columns {
			fmt.Fprintf(statsMemFw, "%s\n", fmt.Sprintf("%s=%s", col.Info.Name.String(), col.StatusToString()))
		}
	}
	return nil
}

func dumpStats(zw *zip.Writer, pairs map[tableNamePair]struct{}, do *Domain, historyStatsTS uint64) (string, error) {
	allFallBackTbls := make([]string, 0)
	for pair := range pairs {
		if pair.IsView {
			continue
		}
		jsonTbl, fallBackTbls, err := getStatsForTable(do, pair, historyStatsTS)
		if err != nil {
			return "", err
		}
		statsFw, err := zw.Create(fmt.Sprintf("stats/%v.%v.json", pair.DBName, pair.TableName))
		if err != nil {
			return "", errors.AddStack(err)
		}
		data, err := json.Marshal(jsonTbl)
		if err != nil {
			return "", errors.AddStack(err)
		}
		_, err = statsFw.Write(data)
		if err != nil {
			return "", errors.AddStack(err)
		}
		allFallBackTbls = append(allFallBackTbls, fallBackTbls...)
	}
	var msg string
	if len(allFallBackTbls) > 0 {
		msg = "Historical stats for " + strings.Join(allFallBackTbls, ", ") + " are unavailable, fallback to latest stats"
	}
	return msg, nil
}

func dumpSQLs(execStmts []ast.StmtNode, zw *zip.Writer) error {
	for i, stmtExec := range execStmts {
		zf, err := zw.Create(fmt.Sprintf("sql/sql%v.sql", i))
		if err != nil {
			return err
		}
		_, err = zf.Write([]byte(stmtExec.Text()))
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpVariables(sctx sessionctx.Context, sessionVars *variable.SessionVars, zw *zip.Writer) error {
	varMap := make(map[string]string)
	for _, v := range variable.GetSysVars() {
		if v.IsNoop && !variable.EnableNoopVariables.Load() {
			continue
		}
		if infoschema.SysVarHiddenForSem(sctx, v.Name) {
			continue
		}
		value, err := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), v.Name)
		if err != nil {
			return errors.Trace(err)
		}
		varMap[v.Name] = value
	}
	vf, err := zw.Create(PlanReplayerVariablesFile)
	if err != nil {
		return errors.AddStack(err)
	}
	if err := toml.NewEncoder(vf).Encode(varMap); err != nil {
		return errors.AddStack(err)
	}
	return nil
}

func dumpSessionBindRecords(records []bindinfo.Bindings, zw *zip.Writer) error {
	sRows := make([][]string, 0)
	for _, bindData := range records {
		for _, hint := range bindData {
			sRows = append(sRows, []string{
				hint.OriginalSQL,
				hint.BindSQL,
				hint.Db,
				hint.Status,
				hint.CreateTime.String(),
				hint.UpdateTime.String(),
				hint.Charset,
				hint.Collation,
				hint.Source,
			})
		}
	}
	bf, err := zw.Create(PlanReplayerSessionBindingFile)
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		fmt.Fprintf(bf, "%s\n", strings.Join(row, "\t"))
	}
	return nil
}

func dumpSessionBindings(ctx sessionctx.Context, zw *zip.Writer) error {
	recordSets, err := ctx.GetSQLExecutor().Execute(context.Background(), "show bindings")
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], true)
	if err != nil {
		return err
	}
	bf, err := zw.Create(PlanReplayerSessionBindingFile)
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		fmt.Fprintf(bf, "%s\n", strings.Join(row, "\t"))
	}
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func dumpGlobalBindings(ctx sessionctx.Context, zw *zip.Writer) error {
	recordSets, err := ctx.GetSQLExecutor().Execute(context.Background(), "show global bindings")
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], false)
	if err != nil {
		return err
	}
	bf, err := zw.Create(PlanReplayerGlobalBindingFile)
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		fmt.Fprintf(bf, "%s\n", strings.Join(row, "\t"))
	}
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func dumpEncodedPlan(ctx sessionctx.Context, zw *zip.Writer, encodedPlan string) error {
	var recordSets []sqlexec.RecordSet
	var err error
	recordSets, err = ctx.GetSQLExecutor().Execute(context.Background(), fmt.Sprintf("select tidb_decode_plan('%s')", encodedPlan))
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], false)
	if err != nil {
		return err
	}
	fw, err := zw.Create("explain/sql.txt")
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
	}
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func dumpExplain(ctx sessionctx.Context, zw *zip.Writer, isAnalyze bool, sqls []string, emptyAsNil bool) (debugTraces []any, err error) {
	fw, err := zw.Create("explain.txt")
	if err != nil {
		return nil, errors.AddStack(err)
	}
	ctx.GetSessionVars().InPlanReplayer = true
	defer func() {
		ctx.GetSessionVars().InPlanReplayer = false
	}()
	for i, sql := range sqls {
		var recordSets []sqlexec.RecordSet
		if isAnalyze {
			// Explain analyze
			recordSets, err = ctx.GetSQLExecutor().Execute(context.Background(), fmt.Sprintf("explain analyze %s", sql))
			if err != nil {
				return nil, err
			}
		} else {
			// Explain
			recordSets, err = ctx.GetSQLExecutor().Execute(context.Background(), fmt.Sprintf("explain %s", sql))
			if err != nil {
				return nil, err
			}
		}
		debugTrace := ctx.GetSessionVars().StmtCtx.OptimizerDebugTrace
		debugTraces = append(debugTraces, debugTrace)
		sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], emptyAsNil)
		if err != nil {
			return nil, err
		}
		for _, row := range sRows {
			fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
		}
		if len(recordSets) > 0 {
			if err := recordSets[0].Close(); err != nil {
				return nil, err
			}
		}
		if i < len(sqls)-1 {
			fmt.Fprintf(fw, "<--------->\n")
		}
	}
	return
}

func dumpPlanReplayerExplain(ctx sessionctx.Context, zw *zip.Writer, task *PlanReplayerDumpTask, records *[]PlanReplayerStatusRecord) error {
	sqls := make([]string, 0)
	for _, execStmt := range task.ExecStmts {
		sql := execStmt.Text()
		sqls = append(sqls, sql)
		*records = append(*records, PlanReplayerStatusRecord{
			OriginSQL: sql,
			Token:     task.FileName,
		})
	}
	debugTraces, err := dumpExplain(ctx, zw, task.Analyze, sqls, false)
	task.DebugTrace = debugTraces
	return err
}

// extractTableNames extracts table names from the given stmts.
func extractTableNames(ctx context.Context, sctx sessionctx.Context,
	execStmts []ast.StmtNode, curDB model.CIStr) (map[tableNamePair]struct{}, error) {
	tableExtractor := &tableNameExtractor{
		ctx:      ctx,
		executor: sctx.GetRestrictedSQLExecutor(),
		is:       GetDomain(sctx).InfoSchema(),
		curDB:    curDB,
		names:    make(map[tableNamePair]struct{}),
		cteNames: make(map[string]struct{}),
	}
	for _, execStmt := range execStmts {
		execStmt.Accept(tableExtractor)
	}
	if tableExtractor.err != nil {
		return nil, tableExtractor.err
	}
	return tableExtractor.getTablesAndViews(), nil
}

func getStatsForTable(do *Domain, pair tableNamePair, historyStatsTS uint64) (*util.JSONTable, []string, error) {
	is := do.InfoSchema()
	h := do.StatsHandle()
	tbl, err := is.TableByName(model.NewCIStr(pair.DBName), model.NewCIStr(pair.TableName))
	if err != nil {
		return nil, nil, err
	}
	if historyStatsTS > 0 {
		return h.DumpHistoricalStatsBySnapshot(pair.DBName, tbl.Meta(), historyStatsTS)
	}
	jt, err := h.DumpStatsToJSON(pair.DBName, tbl.Meta(), nil, true)
	return jt, nil, err
}

func getShowCreateTable(pair tableNamePair, zw *zip.Writer, ctx sessionctx.Context) error {
	recordSets, err := ctx.GetSQLExecutor().Execute(context.Background(), fmt.Sprintf("show create table `%v`.`%v`", pair.DBName, pair.TableName))
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], false)
	if err != nil {
		return err
	}
	var fw io.Writer
	if pair.IsView {
		fw, err = zw.Create(fmt.Sprintf("view/%v.%v.view.txt", pair.DBName, pair.TableName))
		if err != nil {
			return errors.AddStack(err)
		}
		if len(sRows) == 0 || len(sRows[0]) != 4 {
			return fmt.Errorf("plan replayer: get create view %v.%v failed", pair.DBName, pair.TableName)
		}
	} else {
		fw, err = zw.Create(fmt.Sprintf("schema/%v.%v.schema.txt", pair.DBName, pair.TableName))
		if err != nil {
			return errors.AddStack(err)
		}
		if len(sRows) == 0 || len(sRows[0]) != 2 {
			return fmt.Errorf("plan replayer: get create table %v.%v failed", pair.DBName, pair.TableName)
		}
	}
	fmt.Fprintf(fw, "create database if not exists `%v`; use `%v`;", pair.DBName, pair.DBName)
	fmt.Fprintf(fw, "%s", sRows[0][1])
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func resultSetToStringSlice(ctx context.Context, rs sqlexec.RecordSet, emptyAsNil bool) ([][]string, error) {
	rows, err := getRows(ctx, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i, row := range rows {
		iRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				iRow[j], err = d.ToString()
				if err != nil {
					return nil, err
				}
				if len(iRow[j]) < 1 && emptyAsNil {
					iRow[j] = "<nil>"
				}
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}

func getRows(ctx context.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	req := rs.NewChunk(nil)
	// Must reuse `req` for imitating server.(*clientConn).writeChunks
	for {
		err := rs.Next(ctx, req)
		if err != nil {
			return nil, err
		}
		if req.NumRows() == 0 {
			break
		}

		iter := chunk.NewIterator4Chunk(req.CopyConstruct())
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func dumpDebugTrace(zw *zip.Writer, debugTraces []any) error {
	for i, trace := range debugTraces {
		fw, err := zw.Create(fmt.Sprintf("debug_trace/debug_trace%d.json", i))
		if err != nil {
			return errors.AddStack(err)
		}
		err = dumpOneDebugTrace(fw, trace)
		if err != nil {
			return errors.AddStack(err)
		}
	}
	return nil
}

func dumpOneDebugTrace(w io.Writer, debugTrace any) error {
	jsonEncoder := json.NewEncoder(w)
	// If we do not set this to false, ">", "<", "&"... will be escaped to "\u003c","\u003e", "\u0026"...
	jsonEncoder.SetEscapeHTML(false)
	return jsonEncoder.Encode(debugTrace)
}

func dumpErrorMsgs(zw *zip.Writer, msgs []string) error {
	mt, err := zw.Create(PlanReplayerErrorMessageFile)
	if err != nil {
		return errors.AddStack(err)
	}
	for _, msg := range msgs {
		_, err = mt.Write([]byte(msg))
		if err != nil {
			return errors.AddStack(err)
		}
		_, err = mt.Write([]byte{'\n'})
		if err != nil {
			return errors.AddStack(err)
		}
	}
	return nil
}
