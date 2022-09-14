// Copyright 2021 PingCAP, Inc.
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

package executor

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

var _ Executor = &PlanReplayerSingleExec{}
var _ Executor = &PlanReplayerLoadExec{}

const (
	configFile          = "config.toml"
	metaFile            = "meta.txt"
	variablesFile       = "variables.toml"
	sqlFile             = "sqls.sql"
	tiFlashReplicasFile = "table_tiflash_replica.txt"
	sessionBindingFile  = "session_bindings.sql"
	globalBindingFile   = "global_bindings.sql"
	explainFile         = "explain.txt"
)

// PlanReplayerSingleExec represents a plan replayer executor.
type PlanReplayerSingleExec struct {
	baseExecutor
	ExecStmt ast.StmtNode
	Analyze  bool

	endFlag bool
}

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

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
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
		tp := tableNamePair{DBName: t.Schema.L, TableName: t.Name.L, IsView: isView}
		if tp.DBName == "" {
			tp.DBName = tne.curDB.L
		}
		if _, ok := tne.names[tp]; !ok {
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
	isView := tne.is.TableIsView(schema, table)
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

// Next implements the Executor Next interface.
func (e *PlanReplayerSingleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.endFlag {
		return nil
	}
	if e.ExecStmt == nil {
		return errors.New("plan replayer: sql is empty")
	}
	res, err := e.dumpSingle(ctx, domain.GetPlanReplayerDirName())
	if err != nil {
		return err
	}
	req.AppendString(0, res)
	e.endFlag = true
	return nil
}

// dumpSingle will dump the information about a single sql.
// The files will be organized into the following format:
/*
 |-meta.txt
 |-schema
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
 |-config.toml
 |-table_tiflash_replica.txt
 |-variables.toml
 |-bindings.sql
 |-sqls.sql
 |_explain
     |-explain.txt
*/
func (e *PlanReplayerSingleExec) dumpSingle(ctx context.Context, path string) (fileName string, err error) {
	// Create path
	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return "", errors.AddStack(err)
	}

	// Generate key and create zip file
	time := time.Now().UnixNano()
	b := make([]byte, 16)
	//nolint: gosec
	_, err = rand.Read(b)
	if err != nil {
		return "", err
	}
	key := base64.URLEncoding.EncodeToString(b)
	fileName = fmt.Sprintf("replayer_single_%v_%v.zip", key, time)
	zf, err := os.Create(filepath.Join(path, fileName))
	if err != nil {
		return "", errors.AddStack(err)
	}

	// Create zip writer
	zw := zip.NewWriter(zf)
	defer func() {
		err = zw.Close()
		if err != nil {
			logutil.BgLogger().Error("Closing zip writer failed", zap.Error(err), zap.String("filename", fileName))
		}
		err = zf.Close()
		if err != nil {
			logutil.BgLogger().Error("Closing zip file failed", zap.Error(err), zap.String("filename", fileName))
		}
	}()

	// Dump config
	if err = dumpConfig(zw); err != nil {
		return "", err
	}

	// Dump meta
	if err = dumpMeta(zw); err != nil {
		return "", err
	}

	// Retrieve current DB
	sessionVars := e.ctx.GetSessionVars()
	dbName := model.NewCIStr(sessionVars.CurrentDB)
	do := domain.GetDomain(e.ctx)

	// Retrieve all tables
	pairs, err := e.extractTableNames(ctx, e.ExecStmt, dbName)
	if err != nil {
		return "", errors.AddStack(fmt.Errorf("plan replayer: invalid SQL text, err: %v", err))
	}

	// Dump Schema and View
	if err = dumpSchemas(e.ctx, zw, pairs); err != nil {
		return "", err
	}

	// Dump tables tiflash replicas
	if err = dumpTiFlashReplica(e.ctx, zw, pairs); err != nil {
		return "", err
	}

	// Dump stats
	if err = dumpStats(zw, pairs, do); err != nil {
		return "", err
	}

	// Dump variables
	if err = dumpVariables(e.ctx, zw); err != nil {
		return "", err
	}

	// Dump sql
	sql, err := zw.Create(sqlFile)
	if err != nil {
		return "", nil
	}
	_, err = sql.Write([]byte(e.ExecStmt.Text()))
	if err != nil {
		return "", err
	}

	// Dump session bindings
	if err = dumpSessionBindings(e.ctx, zw); err != nil {
		return "", err
	}

	// Dump global bindings
	if err = dumpGlobalBindings(e.ctx, zw); err != nil {
		return "", err
	}

	// Dump explain
	if err = dumpExplain(e.ctx, zw, e.ExecStmt.Text(), e.Analyze); err != nil {
		return "", err
	}

	return fileName, nil
}

func dumpConfig(zw *zip.Writer) error {
	cf, err := zw.Create(configFile)
	if err != nil {
		return errors.AddStack(err)
	}
	if err := toml.NewEncoder(cf).Encode(config.GetGlobalConfig()); err != nil {
		return errors.AddStack(err)
	}
	return nil
}

func dumpMeta(zw *zip.Writer) error {
	mt, err := zw.Create(metaFile)
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
	bf, err := zw.Create(tiFlashReplicasFile)
	if err != nil {
		return errors.AddStack(err)
	}
	is := domain.GetDomain(ctx).InfoSchema()
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
	for pair := range pairs {
		err := getShowCreateTable(pair, zw, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpStats(zw *zip.Writer, pairs map[tableNamePair]struct{}, do *domain.Domain) error {
	for pair := range pairs {
		if pair.IsView {
			continue
		}
		jsonTbl, err := getStatsForTable(do, pair)
		if err != nil {
			return err
		}
		statsFw, err := zw.Create(fmt.Sprintf("stats/%v.%v.json", pair.DBName, pair.TableName))
		if err != nil {
			return errors.AddStack(err)
		}
		data, err := json.Marshal(jsonTbl)
		if err != nil {
			return errors.AddStack(err)
		}
		_, err = statsFw.Write(data)
		if err != nil {
			return errors.AddStack(err)
		}
	}
	return nil
}

func dumpVariables(ctx sessionctx.Context, zw *zip.Writer) error {
	varMap := make(map[string]string)
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "show variables")
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], false)
	if err != nil {
		return err
	}
	vf, err := zw.Create(variablesFile)
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		varMap[row[0]] = row[1]
	}
	if err := toml.NewEncoder(vf).Encode(varMap); err != nil {
		return errors.AddStack(err)
	}
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func dumpSessionBindings(ctx sessionctx.Context, zw *zip.Writer) error {
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "show bindings")
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], true)
	if err != nil {
		return err
	}
	bf, err := zw.Create(sessionBindingFile)
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
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), "show global bindings")
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], false)
	if err != nil {
		return err
	}
	bf, err := zw.Create(globalBindingFile)
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

func dumpExplain(ctx sessionctx.Context, zw *zip.Writer, sql string, isAnalyze bool) error {
	var recordSets []sqlexec.RecordSet
	var err error
	if isAnalyze {
		// Explain analyze
		recordSets, err = ctx.(sqlexec.SQLExecutor).Execute(context.Background(), fmt.Sprintf("explain analyze %s", sql))
		if err != nil {
			return err
		}
	} else {
		// Explain
		recordSets, err = ctx.(sqlexec.SQLExecutor).Execute(context.Background(), fmt.Sprintf("explain %s", sql))
		if err != nil {
			return err
		}
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], false)
	if err != nil {
		return err
	}
	fw, err := zw.Create(explainFile)
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

func (e *PlanReplayerSingleExec) extractTableNames(ctx context.Context,
	ExecStmt ast.StmtNode, curDB model.CIStr) (map[tableNamePair]struct{}, error) {
	tableExtractor := &tableNameExtractor{
		ctx:      ctx,
		executor: e.ctx.(sqlexec.RestrictedSQLExecutor),
		is:       domain.GetDomain(e.ctx).InfoSchema(),
		curDB:    curDB,
		names:    make(map[tableNamePair]struct{}),
		cteNames: make(map[string]struct{}),
	}
	ExecStmt.Accept(tableExtractor)
	if tableExtractor.err != nil {
		return nil, tableExtractor.err
	}
	r := make(map[tableNamePair]struct{})
	for tablePair := range tableExtractor.names {
		if tablePair.IsView {
			r[tablePair] = struct{}{}
			continue
		}
		// remove cte in table names
		_, ok := tableExtractor.cteNames[tablePair.TableName]
		if !ok {
			r[tablePair] = struct{}{}
		}
	}
	return r, nil
}

func getStatsForTable(do *domain.Domain, pair tableNamePair) (*handle.JSONTable, error) {
	is := do.InfoSchema()
	h := do.StatsHandle()
	tbl, err := is.TableByName(model.NewCIStr(pair.DBName), model.NewCIStr(pair.TableName))
	if err != nil {
		return nil, err
	}
	js, err := h.DumpStatsToJSON(pair.DBName, tbl.Meta(), nil)
	return js, err
}

func getShowCreateTable(pair tableNamePair, zw *zip.Writer, ctx sessionctx.Context) error {
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.Background(), fmt.Sprintf("show create table `%v`.`%v`", pair.DBName, pair.TableName))
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

// PlanReplayerLoadExec represents a plan replayer load executor.
type PlanReplayerLoadExec struct {
	baseExecutor
	info *PlanReplayerLoadInfo
}

// PlanReplayerLoadInfo contains file path and session context.
type PlanReplayerLoadInfo struct {
	Path string
	Ctx  sessionctx.Context
}

type planReplayerLoadKeyType int

func (k planReplayerLoadKeyType) String() string {
	return "plan_replayer_load_var"
}

// PlanReplayerLoadVarKey is a variable key for plan replayer load.
const PlanReplayerLoadVarKey planReplayerLoadKeyType = 0

// Next implements the Executor Next interface.
func (e *PlanReplayerLoadExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if len(e.info.Path) == 0 {
		return errors.New("plan replayer: file path is empty")
	}
	val := e.ctx.Value(PlanReplayerLoadVarKey)
	if val != nil {
		e.ctx.SetValue(PlanReplayerLoadVarKey, nil)
		return errors.New("plan replayer: previous plan replayer load option isn't closed normally, please try again")
	}
	e.ctx.SetValue(PlanReplayerLoadVarKey, e.info)
	return nil
}

func loadSetTiFlashReplica(ctx sessionctx.Context, z *zip.Reader) error {
	for _, zipFile := range z.File {
		if strings.Compare(zipFile.Name, tiFlashReplicasFile) == 0 {
			v, err := zipFile.Open()
			if err != nil {
				return errors.AddStack(err)
			}
			//nolint: errcheck,all_revive
			defer v.Close()
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(v)
			if err != nil {
				return errors.AddStack(err)
			}
			rows := strings.Split(buf.String(), "\n")
			for _, row := range rows {
				if len(row) < 1 {
					continue
				}
				r := strings.Split(row, "\t")
				if len(r) < 3 {
					logutil.BgLogger().Debug("plan replayer: skip error",
						zap.Error(errors.New("setting tiflash replicas failed")))
					continue
				}
				dbName := r[0]
				tableName := r[1]
				c := context.Background()
				// Though we record tiflash replica in txt, we only set 1 tiflash replica as it's enough for reproduce the plan
				sql := fmt.Sprintf("alter table %s.%s set tiflash replica 1", dbName, tableName)
				_, err = ctx.(sqlexec.SQLExecutor).Execute(c, sql)
				logutil.BgLogger().Debug("plan replayer: skip error", zap.Error(err))
			}
		}
	}
	return nil
}

func loadAllBindings(ctx sessionctx.Context, z *zip.Reader) error {
	for _, f := range z.File {
		if strings.Compare(f.Name, sessionBindingFile) == 0 {
			err := loadBindings(ctx, f, true)
			if err != nil {
				return err
			}
		} else if strings.Compare(f.Name, globalBindingFile) == 0 {
			err := loadBindings(ctx, f, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func loadBindings(ctx sessionctx.Context, f *zip.File, isSession bool) error {
	r, err := f.Open()
	if err != nil {
		return errors.AddStack(err)
	}
	//nolint: errcheck
	defer r.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		return errors.AddStack(err)
	}
	if len(buf.String()) < 1 {
		return nil
	}
	bindings := strings.Split(buf.String(), "\n")
	for _, binding := range bindings {
		cols := strings.Split(binding, "\t")
		if len(cols) < 3 {
			continue
		}
		originSQL := cols[0]
		bindingSQL := cols[1]
		enabled := cols[3]
		if strings.Compare(enabled, "enabled") == 0 {
			sql := fmt.Sprintf("CREATE %s BINDING FOR %s USING %s", func() string {
				if isSession {
					return "SESSION"
				}
				return "GLOBAL"
			}(), originSQL, bindingSQL)
			c := context.Background()
			_, err = ctx.(sqlexec.SQLExecutor).Execute(c, sql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func loadVariables(ctx sessionctx.Context, z *zip.Reader) error {
	unLoadVars := make([]string, 0)
	for _, zipFile := range z.File {
		if strings.Compare(zipFile.Name, variablesFile) == 0 {
			varMap := make(map[string]string)
			v, err := zipFile.Open()
			if err != nil {
				return errors.AddStack(err)
			}
			//nolint: errcheck,all_revive
			defer v.Close()
			_, err = toml.DecodeReader(v, &varMap)
			if err != nil {
				return errors.AddStack(err)
			}
			vars := ctx.GetSessionVars()
			for name, value := range varMap {
				sysVar := variable.GetSysVar(name)
				if sysVar == nil {
					unLoadVars = append(unLoadVars, name)
					logutil.BgLogger().Warn(fmt.Sprintf("skip set variable %s:%s", name, value), zap.Error(err))
					continue
				}
				sVal, err := sysVar.Validate(vars, value, variable.ScopeSession)
				if err != nil {
					unLoadVars = append(unLoadVars, name)
					logutil.BgLogger().Warn(fmt.Sprintf("skip variable %s:%s", name, value), zap.Error(err))
					continue
				}
				err = vars.SetSystemVar(name, sVal)
				if err != nil {
					unLoadVars = append(unLoadVars, name)
					logutil.BgLogger().Warn(fmt.Sprintf("skip set variable %s:%s", name, value), zap.Error(err))
					continue
				}
			}
		}
	}
	if len(unLoadVars) > 0 {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("variables set failed:%s", strings.Join(unLoadVars, ",")))
	}
	return nil
}

// createSchemaAndItems creates schema and tables or views
func createSchemaAndItems(ctx sessionctx.Context, f *zip.File) error {
	r, err := f.Open()
	if err != nil {
		return errors.AddStack(err)
	}
	//nolint: errcheck
	defer r.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		return errors.AddStack(err)
	}
	sqls := strings.Split(buf.String(), ";")
	if len(sqls) != 3 {
		return errors.New("plan replayer: create schema and tables failed")
	}
	c := context.Background()
	// create database if not exists
	_, err = ctx.(sqlexec.SQLExecutor).Execute(c, sqls[0])
	logutil.BgLogger().Debug("plan replayer: skip error", zap.Error(err))
	// use database
	_, err = ctx.(sqlexec.SQLExecutor).Execute(c, sqls[1])
	if err != nil {
		return err
	}
	// create table or view
	_, err = ctx.(sqlexec.SQLExecutor).Execute(c, sqls[2])
	if err != nil {
		return err
	}
	return nil
}

func loadStats(ctx sessionctx.Context, f *zip.File) error {
	jsonTbl := &handle.JSONTable{}
	r, err := f.Open()
	if err != nil {
		return errors.AddStack(err)
	}
	//nolint: errcheck
	defer r.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		return errors.AddStack(err)
	}
	if err := json.Unmarshal(buf.Bytes(), jsonTbl); err != nil {
		return errors.AddStack(err)
	}
	do := domain.GetDomain(ctx)
	h := do.StatsHandle()
	if h == nil {
		return errors.New("plan replayer: hanlde is nil")
	}
	return h.LoadStatsFromJSON(ctx.GetInfoSchema().(infoschema.InfoSchema), jsonTbl)
}

// Update updates the data of the corresponding table.
func (e *PlanReplayerLoadInfo) Update(data []byte) error {
	b := bytes.NewReader(data)
	z, err := zip.NewReader(b, int64(len(data)))
	if err != nil {
		return errors.AddStack(err)
	}

	// load variable
	err = loadVariables(e.Ctx, z)
	if err != nil {
		return err
	}

	// build schema and table first
	for _, zipFile := range z.File {
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "schema") == 0 {
			err = createSchemaAndItems(e.Ctx, zipFile)
			if err != nil {
				return err
			}
		}
	}

	// set tiflash replica if exists
	err = loadSetTiFlashReplica(e.Ctx, z)
	if err != nil {
		return err
	}

	// build view next
	for _, zipFile := range z.File {
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "view") == 0 {
			err = createSchemaAndItems(e.Ctx, zipFile)
			if err != nil {
				return err
			}
		}
	}

	// load stats
	for _, zipFile := range z.File {
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "stats") == 0 {
			err = loadStats(e.Ctx, zipFile)
			if err != nil {
				return err
			}
		}
	}

	err = loadAllBindings(e.Ctx, z)
	if err != nil {
		logutil.BgLogger().Warn("load bindings failed", zap.Error(err))
		e.Ctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("load bindings failed, err:%v", err))
	}
	return nil
}
