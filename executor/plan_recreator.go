// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"archive/zip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/sqlexec"
	"math/rand"
	"os"
	"strings"
	"time"
)

const recreatorPath string = "/tmp/recreator"
const remainedInterval float64 = 3

// PlanRecreatorExec represents a plan recreator executor.
type PlanRecreatorSingleExec struct {
	baseExecutor
	info *PlanRecreatorSingleInfo
}

// PlanRecreatorInfo saves the information of plan recreator operation.
type PlanRecreatorSingleInfo struct {
	ExecStmt ast.StmtNode
	Analyze  bool
	Load     bool
	File     string
	Ctx      sessionctx.Context
}

type tableNamePair struct {
	DBName    string
	TableName string
}

type tableNameExtractor struct {
	curDB string
	names map[tableNamePair]struct{}
}

type fileInfo struct {
	StartTime time.Time
	Token     [16]byte
}

type fileList struct {
	FileInfo map[string]fileInfo
	TokenMap map[[16]byte]string
}

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*ast.TableName); ok {
		return in, true
	}
	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if t, ok := in.(*ast.TableName); ok {
		tp := tableNamePair{DBName: t.Schema.L, TableName: t.Name.L}
		if tp.DBName == "" {
			tp.DBName = tne.curDB
		}
		if _, ok := tne.names[tp]; !ok {
			tne.names[tp] = struct{}{}
		}
	}
	return in, true
}

// planRecreatorVarKeyType is a dummy type to avoid naming collision in context.
type planRecreatorVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k planRecreatorVarKeyType) String() string {
	return "plan_recreator_var"
}

// planRecreatorFileListType is a dummy type to avoid naming collision in context.
type planRecreatorFileListType int

// String defines a Stringer function for debugging and pretty printing.
func (k planRecreatorFileListType) String() string {
	return "plan_recreator_file_list"
}

// PlanRecreatorVarKey is a variable key for load statistic.
const PlanRecreatorVarKey planRecreatorVarKeyType = 0
const PlanRecreatorFileList planRecreatorFileListType = 0

// Next implements the Executor Next interface.
func (e *PlanRecreatorSingleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.info.ExecStmt == nil {
		return errors.New("Plan Recreator: sql is empty.")
	}
	val := e.ctx.Value(PlanRecreatorVarKey)
	if val != nil {
		e.ctx.SetValue(PlanRecreatorVarKey, nil)
		return errors.New("Plan Recreator: previous plan recreator option isn't closed normally.")
	}
	e.ctx.SetValue(PlanRecreatorVarKey, e.info)
	return nil
}

// Close implements the Executor Close interface.
func (e *PlanRecreatorSingleExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *PlanRecreatorSingleExec) Open(ctx context.Context) error {
	return nil
}

// Process dose the export/import work for reproducing sql queries.
func (e *PlanRecreatorSingleInfo) Process() (interface{}, error) {
	// TODO: plan recreator load will be developed later
	if e.Load {
		return nil, nil
	} else {
		return e.dumpSingle()
	}
}

func (e *PlanRecreatorSingleInfo) dumpSingle() (interface{}, error) {
	// Create path
	err := os.MkdirAll(recreatorPath, os.ModePerm)
	if err != nil {
		return nil, errors.New("Plan Recreator: cannot create plan recreator path.")
	}

	// Create zip file
	startTime := time.Now()
	fileName := fmt.Sprintf("recreator_single_%v.zip", startTime.UnixNano())
	zf, err := os.Create(recreatorPath + "/" + fileName)
	if err != nil {
		return nil, errors.New("Plan Recreator: cannot create zip file.")
	}
	val := e.Ctx.Value(PlanRecreatorFileList)
	if val == nil {
		e.Ctx.SetValue(PlanRecreatorFileList, fileList{FileInfo: make(map[string]fileInfo), TokenMap: make(map[[16]byte]string)})
	} else {
		// Clean outdated files
		Flist := val.(fileList).FileInfo
		TList := val.(fileList).TokenMap
		for k, v := range Flist {
			if time.Since(v.StartTime).Minutes() > remainedInterval {
				err := os.Remove(recreatorPath + "/" + k)
				if err != nil {
					logutil.BgLogger().Warn(fmt.Sprintf("Cleaning outdated file %s failed.", k))
				}
				delete(Flist, k)
				delete(TList, v.Token)
			}
		}
	}
	// Generate Token
	token := md5.Sum([]byte(fmt.Sprintf("%s%d", fileName, rand.Int63())))
	e.Ctx.Value(PlanRecreatorFileList).(fileList).FileInfo[fileName] = fileInfo{StartTime: startTime, Token: token}
	e.Ctx.Value(PlanRecreatorFileList).(fileList).TokenMap[token] = fileName

	// Create zip writer
	zw := zip.NewWriter(zf)
	defer func() {
		err := zw.Close()
		if err != nil {
			logutil.BgLogger().Warn("Closing zip writer failed.")
		}
		err = zf.Close()
		if err != nil {
			logutil.BgLogger().Warn("Closing zip file failed.")
		}
	}()

	// Dump config
	cf, err := zw.Create("config.toml")
	if err != nil {
		return nil, err
	}
	if err := toml.NewEncoder(cf).Encode(config.GetGlobalConfig()); err != nil {
		return nil, err
	}

	// Dump meta
	mt, err := zw.Create("meta.txt")
	if err != nil {
		return nil, err
	}
	_, err = mt.Write([]byte(printer.GetTiDBInfo()))
	if err != nil {
		return nil, err
	}

	// Retrieve current DB
	sessionVars := e.Ctx.GetSessionVars()
	dbName := model.NewCIStr(sessionVars.CurrentDB)
	do := domain.GetDomain(e.Ctx)

	// Retrieve all tables
	pairs, err := extractTableNames(e.ExecStmt, dbName.L)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Plan Recreator: invalid SQL text, err: %v", err))
	}

	// Dump stats
	for pair := range pairs {
		jsonTbl, err := getStatsForTable(do, pair)
		if err != nil {
			return nil, err
		}
		statsFw, err := zw.Create(fmt.Sprintf("stats/%v.%v.json", pair.DBName, pair.TableName))
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(jsonTbl)
		if err != nil {
			return nil, err
		}
		_, err = statsFw.Write(data)
		if err != nil {
			return nil, err
		}
	}

	// Dump schema
	for pair := range pairs {
		err = getShowCreateTable(pair, zw, e.Ctx)
		if err != nil {
			return nil, err
		}
	}

	// Dump variables
	varMap := make(map[string]string)
	recordSets, err := e.Ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), "show variables")
	if len(recordSets) > 0 {
		defer recordSets[0].Close()
	}
	if err != nil {
		return nil, err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0])
	if err != nil {
		return nil, err
	}
	vf, err := zw.Create("variables.toml")
	if err != nil {
		return nil, err
	}
	for _, row := range sRows {
		varMap[row[0]] = row[1]
	}
	if err := toml.NewEncoder(vf).Encode(varMap); err != nil {
		return nil, err
	}

	// Dump sql
	sql, err := zw.Create("sqls.sql")
	if err != nil {
		return nil, nil
	}
	sql.Write([]byte(e.ExecStmt.Text()))

	// Dump bindings
	normSql, _ := parser.NormalizeDigest(e.ExecStmt.Text())
	recordSets, err = e.Ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("show bindings where Original_sql='%s'", normSql))
	if len(recordSets) > 0 {
		defer recordSets[0].Close()
	}
	if err != nil {
		return nil, err
	}
	sRows, err = resultSetToStringSlice(context.Background(), recordSets[0])
	if err != nil {
		return nil, err
	}
	bf, err := zw.Create("bindings.sql")
	if err != nil {
		return nil, err
	}
	for _, row := range sRows {
		fmt.Fprintf(bf, "%s\n", strings.Join(row, "\t"))
	}

	// Dump explain
	if e.Analyze {
		// Explain analyze
		recordSets, err = e.Ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("explain analyze %s", e.ExecStmt.Text()))
		if err != nil {
			return nil, err
		}
	} else {
		// Explain
		recordSets, err = e.Ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("explain %s", e.ExecStmt.Text()))
		if err != nil {
			return nil, err
		}
	}
	if len(recordSets) > 0 {
		defer recordSets[0].Close()
	}
	sRows, err = resultSetToStringSlice(context.Background(), recordSets[0])
	if err != nil {
		return nil, err
	}
	fw, err := zw.Create("explain.txt")
	if err != nil {
		return nil, err
	}
	for _, row := range sRows {
		fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
	}
	return hex.EncodeToString(token[:]), nil
}

func extractTableNames(ExecStmt ast.StmtNode, curDB string) (map[tableNamePair]struct{}, error) {
	extractor := &tableNameExtractor{
		curDB: curDB,
		names: make(map[tableNamePair]struct{}),
	}
	ExecStmt.Accept(extractor)
	return extractor.names, nil
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
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("show create table `%v`.`%v`", pair.DBName, pair.TableName))
	if len(recordSets) > 0 {
		defer recordSets[0].Close()
	}
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0])
	if err != nil {
		return err
	}
	fw, err := zw.Create(fmt.Sprintf("schema/%v.%v.schema.txt", pair.DBName, pair.TableName))
	if err != nil {
		return err
	}
	for _, row := range sRows {
		fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
	}
	return nil
}

func resultSetToStringSlice(ctx context.Context, rs sqlexec.RecordSet) ([][]string, error) {
	rows, err := getRows4Test(ctx, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i := range rows {
		row := rows[i]
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
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}

func getRows4Test(ctx context.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	req := rs.NewChunk()
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
