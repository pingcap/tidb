// Copyright 2019 PingCAP, Inc.
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

package server

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

type sqlInfoFetcher struct {
	store tikv.Storage
	do    *domain.Domain
	s     session.Session
}

type tableNamePair struct {
	DBName    string
	TableName string
}

type tableNameExtractor struct {
	curDB string
	names map[tableNamePair]struct{}
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

func (sh *sqlInfoFetcher) zipInfoForSQL(w http.ResponseWriter, r *http.Request) {
	var err error
	sh.s, err = session.CreateSession(sh.store)
	if err != nil {
		serveError(w, http.StatusInternalServerError, fmt.Sprintf("create session failed, err: %v", err))
		return
	}
	defer sh.s.Close()
	sh.do = domain.GetDomain(sh.s)
	reqCtx := r.Context()
	sql := r.FormValue("sql")
	pprofTimeString := r.FormValue("pprof_time")
	timeoutString := r.FormValue("timeout")
	curDB := strings.ToLower(r.FormValue("current_db"))
	if curDB != "" {
		_, err = sh.s.Execute(reqCtx, fmt.Sprintf("use %v", curDB))
		if err != nil {
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("use database %v failed, err: %v", curDB, err))
			return
		}
	}
	var (
		pprofTime int
		timeout   int
	)
	if pprofTimeString != "" {
		pprofTime, err = strconv.Atoi(pprofTimeString)
		if err != nil {
			serveError(w, http.StatusBadRequest, "invalid value for pprof_time, please input a int value larger than 5")
			return
		}
	}
	if pprofTimeString != "" && pprofTime < 5 {
		serveError(w, http.StatusBadRequest, "pprof time is too short, please input a int value larger than 5")
	}
	if timeoutString != "" {
		timeout, err = strconv.Atoi(timeoutString)
		if err != nil {
			serveError(w, http.StatusBadRequest, "invalid value for timeout")
			return
		}
	}
	if timeout < pprofTime {
		timeout = pprofTime
	}
	pairs, err := sh.extractTableNames(sql, curDB)
	if err != nil {
		serveError(w, http.StatusBadRequest, fmt.Sprintf("invalid SQL text, err: %v", err))
		return
	}
	zw := zip.NewWriter(w)
	defer func() {
		logutil.LogErrStack(zw.Close())
	}()
	for pair := range pairs {
		jsonTbl, err := sh.getStatsForTable(pair)
		if err != nil {
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.stats.err.txt", pair.DBName, pair.TableName), err)
			logutil.LogErrStack(err)
			continue
		}
		statsFw, err := zw.Create(fmt.Sprintf("%v.%v.json", pair.DBName, pair.TableName))
		if err != nil {
			logutil.LogErrStack(err)
			continue
		}
		data, err := json.Marshal(jsonTbl)
		if err != nil {
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.stats.err.txt", pair.DBName, pair.TableName), err)
			logutil.LogErrStack(err)
			continue
		}
		_, err = statsFw.Write(data)
		if err != nil {
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.stats.err.txt", pair.DBName, pair.TableName), err)
			logutil.LogErrStack(err)
			continue
		}
	}
	for pair := range pairs {
		err = sh.getShowCreateTable(pair, zw)
		if err != nil {
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.schema.err.txt", pair.DBName, pair.TableName), err)
			logutil.LogErrStack(err)
			return
		}
	}
	// If we don't catch profile. We just get a explain result.
	if pprofTime == 0 {
		recordSets, err := sh.s.(sqlexec.SQLExecutor).Execute(reqCtx, fmt.Sprintf("explain %s", sql))
		if len(recordSets) > 0 {
			defer terror.Call(recordSets[0].Close)
		}
		if err != nil {
			err = sh.writeErrFile(zw, "explain.err.txt", err)
			logutil.LogErrStack(err)
			return
		}
		sRows, err := session.ResultSetToStringSlice(reqCtx, sh.s, recordSets[0])
		if err != nil {
			err = sh.writeErrFile(zw, "explain.err.txt", err)
			logutil.LogErrStack(err)
			return
		}
		fw, err := zw.Create("explain.txt")
		if err != nil {
			logutil.LogErrStack(err)
			return
		}
		for _, row := range sRows {
			fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
		}
	} else {
		// Otherwise we catch a profile and run `EXPLAIN ANALYZE` result.
		ctx, cancelFunc := context.WithCancel(reqCtx)
		timer := time.NewTimer(time.Second * time.Duration(timeout))
		resultChan := make(chan *explainAnalyzeResult)
		go sh.getExplainAnalyze(ctx, sql, resultChan)
		errChan := make(chan error)
		var buf bytes.Buffer
		go sh.catchCPUProfile(reqCtx, pprofTime, &buf, errChan)
		select {
		case result := <-resultChan:
			timer.Stop()
			cancelFunc()
			if result.err != nil {
				err = sh.writeErrFile(zw, "explain_analyze.err.txt", result.err)
				logutil.LogErrStack(err)
				return
			}
			if len(result.rows) == 0 {
				break
			}
			fw, err := zw.Create("explain_analyze.txt")
			if err != nil {
				logutil.LogErrStack(err)
				break
			}
			for _, row := range result.rows {
				fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
			}
		case <-timer.C:
			cancelFunc()
		}
		err = dumpCPUProfile(errChan, &buf, zw)
		if err != nil {
			err = sh.writeErrFile(zw, "profile.err.txt", err)
			logutil.LogErrStack(err)
			return
		}
	}
}

func dumpCPUProfile(errChan chan error, buf *bytes.Buffer, zw *zip.Writer) error {
	err := <-errChan
	if err != nil {
		return err
	}
	fw, err := zw.Create("profile")
	if err != nil {
		return err
	}
	_, err = fw.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (sh *sqlInfoFetcher) writeErrFile(zw *zip.Writer, name string, err error) error {
	fw, err1 := zw.Create(name)
	if err1 != nil {
		return err1
	}
	fmt.Fprintf(fw, "error: %v", err)
	return nil
}

type explainAnalyzeResult struct {
	rows [][]string
	err  error
}

func (sh *sqlInfoFetcher) getExplainAnalyze(ctx context.Context, sql string, resultChan chan<- *explainAnalyzeResult) {
	recordSets, err := sh.s.(sqlexec.SQLExecutor).Execute(ctx, fmt.Sprintf("explain analyze %s", sql))
	if len(recordSets) > 0 {
		defer terror.Call(recordSets[0].Close)
	}
	if err != nil {
		resultChan <- &explainAnalyzeResult{err: err}
		return
	}
	rows, err := session.ResultSetToStringSlice(ctx, sh.s, recordSets[0])
	if err != nil {
		logutil.LogErrStack(err)
		return
	}
	resultChan <- &explainAnalyzeResult{rows: rows}
}

func (sh *sqlInfoFetcher) catchCPUProfile(ctx context.Context, sec int, buf *bytes.Buffer, errChan chan<- error) {
	if err := pprof.StartCPUProfile(buf); err != nil {
		errChan <- err
		return
	}
	sleepWithCtx(ctx, time.Duration(sec)*time.Second)
	pprof.StopCPUProfile()
	errChan <- nil
}

func (sh *sqlInfoFetcher) getStatsForTable(pair tableNamePair) (*handle.JSONTable, error) {
	is := sh.do.InfoSchema()
	h := sh.do.StatsHandle()
	tbl, err := is.TableByName(model.NewCIStr(pair.DBName), model.NewCIStr(pair.TableName))
	if err != nil {
		return nil, err
	}
	js, err := h.DumpStatsToJSON(pair.DBName, tbl.Meta(), nil)
	return js, err
}

func (sh *sqlInfoFetcher) getShowCreateTable(pair tableNamePair, zw *zip.Writer) error {
	recordSets, err := sh.s.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("show create table `%v`.`%v`", pair.DBName, pair.TableName))
	if len(recordSets) > 0 {
		defer terror.Call(recordSets[0].Close)
	}
	if err != nil {
		return err
	}
	sRows, err := session.ResultSetToStringSlice(context.Background(), sh.s, recordSets[0])
	if err != nil {
		logutil.LogErrStack(err)
		return nil
	}
	fw, err := zw.Create(fmt.Sprintf("%v.%v.schema.txt", pair.DBName, pair.TableName))
	if err != nil {
		logutil.LogErrStack(err)
		return nil
	}
	for _, row := range sRows {
		fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
	}
	return nil
}

func (sh *sqlInfoFetcher) extractTableNames(sql, curDB string) (map[tableNamePair]struct{}, error) {
	p := parser.New()
	charset, collation := sh.s.GetSessionVars().GetCharsetInfo()
	stmts, _, err := p.Parse(sql, charset, collation)
	if err != nil {
		return nil, err
	}
	if len(stmts) > 1 {
		return nil, errors.Errorf("Only 1 statement is allowed")
	}
	extractor := &tableNameExtractor{
		curDB: curDB,
		names: make(map[tableNamePair]struct{}),
	}
	stmts[0].Accept(extractor)
	return extractor.names, nil
}
