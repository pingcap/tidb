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
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/printer"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

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
	varMap[PlanReplayerTaskEnableHistoricalStats] = strconv.FormatBool(vardef.EnableHistoricalStatsForCapture.Load())
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

func dumpTiFlashReplica(sctx sessionctx.Context, zw *zip.Writer, pairs map[tableNamePair]struct{}) error {
	bf, err := zw.Create(PlanReplayerTiFlashReplicasFile)
	if err != nil {
		return errors.AddStack(err)
	}
	is := GetDomain(sctx).InfoSchema()
	ctx := infoschema.WithRefillOption(context.Background(), false)
	for pair := range pairs {
		dbName := ast.NewCIStr(pair.DBName)
		tableName := ast.NewCIStr(pair.TableName)
		t, err := is.TableByName(ctx, dbName, tableName)
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
	ctx := infoschema.WithRefillOption(context.Background(), false)
	for pair := range pairs {
		if pair.IsView {
			continue
		}
		tbl, err := is.TableByName(ctx, ast.NewCIStr(pair.DBName), ast.NewCIStr(pair.TableName))
		if err != nil {
			return err
		}
		tblStats := statsHandle.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
		if tblStats == nil {
			continue
		}
		statsMemFw, err := zw.Create(fmt.Sprintf("statsMem/%v.%v.txt", pair.DBName, pair.TableName))
		if err != nil {
			return errors.AddStack(err)
		}
		fmt.Fprintf(statsMemFw, "[INDEX]\n")
		tblStats.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
			fmt.Fprintf(statsMemFw, "%s\n", fmt.Sprintf("%s=%s", idx.Info.Name.String(), idx.StatusToString()))
			return false
		})
		fmt.Fprintf(statsMemFw, "[COLUMN]\n")
		tblStats.ForEachColumnImmutable(func(_ int64, c *statistics.Column) bool {
			fmt.Fprintf(statsMemFw, "%s\n", fmt.Sprintf("%s=%s", c.Info.Name.String(), c.StatusToString()))
			return false
		})
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
		if v.IsNoop && !vardef.EnableNoopVariables.Load() {
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

func dumpSessionBindRecords(records [][]*bindinfo.Binding, zw *zip.Writer) error {
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
	ctx.GetSessionVars().InPlanReplayer = true
	defer func() {
		ctx.GetSessionVars().InPlanReplayer = false
	}()

	// If there are multiple SQLs, write separate explain files
	useSeparateFiles := len(sqls) > 1

	// For single SQL, create explain.txt once before the loop
	var fw io.Writer
	if !useSeparateFiles && len(sqls) > 0 {
		fw, err = zw.Create("explain.txt")
		if err != nil {
			return nil, errors.AddStack(err)
		}
	}

	for i, sql := range sqls {
		// For multiple SQLs, create a separate file for each
		if useSeparateFiles {
			fw, err = zw.Create(fmt.Sprintf("explain/explain%v.txt", i))
			if err != nil {
				return nil, errors.AddStack(err)
			}
		}

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

		// For single SQL, add separator between multiple explains in the same file
		if !useSeparateFiles && i < len(sqls)-1 {
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

