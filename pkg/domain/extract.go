// Copyright 2023 PingCAP, Inc.
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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// ExtractMetaFile indicates meta file for extract
	ExtractMetaFile = "extract_meta.txt"
)

const (
	// ExtractTaskType indicates type of extract task
	ExtractTaskType = "taskType"
	// ExtractPlanTaskSkipStats indicates skip stats for extract plan task
	ExtractPlanTaskSkipStats = "SkipStats"
)

// ExtractType indicates type
type ExtractType uint8

const (
	// ExtractPlanType indicates extract plan task
	ExtractPlanType ExtractType = iota
)

func taskTypeToString(t ExtractType) string {
	if t == ExtractPlanType {
		return "Plan"
	}
	return "Unknown"
}

// ExtractHandle handles the extractWorker to run extract the information task like Plan or any others.
// extractHandle will provide 2 mode for extractWorker:
// 1. submit a background extract task, the response will be returned after the task is started to be solved
// 2. submit a task and wait until the task is solved, the result will be returned to the response.
type ExtractHandle struct {
	worker *extractWorker
}

// NewExtractHandler new extract handler
func NewExtractHandler(sctxs []sessionctx.Context) *ExtractHandle {
	h := &ExtractHandle{}
	h.worker = newExtractWorker(sctxs[0], false)
	return h
}

// ExtractTask extract tasks
func (h *ExtractHandle) ExtractTask(ctx context.Context, task *ExtractTask) (string, error) {
	// TODO: support background job later
	if task.IsBackgroundJob {
		return "", nil
	}
	return h.worker.extractTask(ctx, task)
}

type extractWorker struct {
	ctx                context.Context
	sctx               sessionctx.Context
	isBackgroundWorker bool
	sync.Mutex
}

// ExtractTask indicates task
type ExtractTask struct {
	ExtractType     ExtractType
	IsBackgroundJob bool

	// Param for Extract Plan
	SkipStats      bool
	UseHistoryView bool

	// variables for plan task type
	Begin time.Time
	End   time.Time
}

// NewExtractPlanTask returns extract plan task
func NewExtractPlanTask(begin, end time.Time) *ExtractTask {
	return &ExtractTask{
		Begin:       begin,
		End:         end,
		ExtractType: ExtractPlanType,
	}
}

func newExtractWorker(sctx sessionctx.Context, isBackgroundWorker bool) *extractWorker {
	return &extractWorker{
		sctx:               sctx,
		isBackgroundWorker: isBackgroundWorker,
	}
}

func (w *extractWorker) extractTask(ctx context.Context, task *ExtractTask) (string, error) {
	if task.ExtractType == ExtractPlanType {
		return w.extractPlanTask(ctx, task)
	}
	return "", errors.New("unknown extract task")
}

func (w *extractWorker) extractPlanTask(ctx context.Context, task *ExtractTask) (string, error) {
	if task.UseHistoryView && !config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return "", errors.New("tidb_stmt_summary_enable_persistent should be enabled for extract task")
	}
	records, err := w.collectRecords(ctx, task)
	if err != nil {
		logutil.BgLogger().Error("collect stmt summary records failed for extract plan task", zap.Error(err))
		return "", err
	}
	p, err := w.packageExtractPlanRecords(ctx, records)
	if err != nil {
		logutil.BgLogger().Error("package stmt summary records failed for extract plan task", zap.Error(err))
		return "", err
	}
	return w.dumpExtractPlanPackage(task, p)
}

func (w *extractWorker) collectRecords(ctx context.Context, task *ExtractTask) (map[stmtSummaryHistoryKey]*stmtSummaryHistoryRecord, error) {
	w.Lock()
	defer w.Unlock()
	exec := w.sctx.GetRestrictedSQLExecutor()
	ctx1 := kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	sourceTable := "STATEMENTS_SUMMARY_HISTORY"
	if !task.UseHistoryView {
		sourceTable = "STATEMENTS_SUMMARY"
	}
	rows, _, err := exec.ExecRestrictedSQL(ctx1, nil, fmt.Sprintf("SELECT STMT_TYPE, DIGEST, PLAN_DIGEST,QUERY_SAMPLE_TEXT, BINARY_PLAN, TABLE_NAMES, SAMPLE_USER FROM INFORMATION_SCHEMA.%s WHERE SUMMARY_END_TIME > '%s' AND SUMMARY_BEGIN_TIME < '%s'",
		sourceTable, task.Begin.Format(types.TimeFormat), task.End.Format(types.TimeFormat)))
	if err != nil {
		return nil, err
	}
	collectMap := make(map[stmtSummaryHistoryKey]*stmtSummaryHistoryRecord, 0)
	for _, row := range rows {
		record := &stmtSummaryHistoryRecord{}
		record.stmtType = row.GetString(0)
		record.digest = row.GetString(1)
		record.planDigest = row.GetString(2)
		record.sql = row.GetString(3)
		record.binaryPlan = row.GetString(4)
		tableNames := row.GetString(5)
		key := stmtSummaryHistoryKey{
			digest:     record.digest,
			planDigest: record.planDigest,
		}
		record.userName = row.GetString(6)
		record.tables = make([]tableNamePair, 0)
		setRecord, err := w.handleTableNames(tableNames, record)
		if err != nil {
			return nil, err
		}
		if setRecord && checkRecordValid(record) {
			collectMap[key] = record
		}
	}
	return collectMap, nil
}

func (w *extractWorker) handleTableNames(tableNames string, record *stmtSummaryHistoryRecord) (bool, error) {
	is := GetDomain(w.sctx).InfoSchema()
	for _, t := range strings.Split(tableNames, ",") {
		names := strings.Split(t, ".")
		if len(names) != 2 {
			return false, nil
		}
		dbName := names[0]
		tblName := names[1]
		record.schemaName = dbName
		// skip internal schema record
		switch strings.ToLower(record.schemaName) {
		case util.PerformanceSchemaName.L, util.InformationSchemaName.L, util.MetricSchemaName.L, "mysql":
			return false, nil
		}
		exists := is.TableExists(model.NewCIStr(dbName), model.NewCIStr(tblName))
		if !exists {
			return false, nil
		}
		t, err := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
		if err != nil {
			return false, err
		}
		record.tables = append(record.tables, tableNamePair{DBName: dbName, TableName: tblName, IsView: t.Meta().IsView()})
	}
	return true, nil
}

func checkRecordValid(r *stmtSummaryHistoryRecord) bool {
	if r.stmtType != "Select" {
		return false
	}
	if r.schemaName == "" {
		return false
	}
	if r.planDigest == "" {
		return false
	}
	return true
}

func (w *extractWorker) packageExtractPlanRecords(ctx context.Context, records map[stmtSummaryHistoryKey]*stmtSummaryHistoryRecord) (*extractPlanPackage, error) {
	p := &extractPlanPackage{}
	p.records = records
	p.tables = make(map[tableNamePair]struct{}, 0)
	for _, record := range records {
		// skip the sql which has been cut off
		if strings.Contains(record.sql, "(len:") {
			record.skip = true
			continue
		}
		plan, err := w.decodeBinaryPlan(ctx, record.binaryPlan)
		if err != nil {
			return nil, err
		}
		record.plan = plan
		for _, tbl := range record.tables {
			p.tables[tbl] = struct{}{}
		}
	}
	if err := w.handleIsView(ctx, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (w *extractWorker) handleIsView(ctx context.Context, p *extractPlanPackage) error {
	is := GetDomain(w.sctx).InfoSchema()
	tne := &tableNameExtractor{
		ctx:      ctx,
		executor: w.sctx.GetRestrictedSQLExecutor(),
		is:       is,
		curDB:    model.NewCIStr(""),
		names:    make(map[tableNamePair]struct{}),
		cteNames: make(map[string]struct{}),
	}
	for v := range p.tables {
		if v.IsView {
			v, err := is.TableByName(model.NewCIStr(v.DBName), model.NewCIStr(v.TableName))
			if err != nil {
				return err
			}
			sql := v.Meta().View.SelectStmt
			node, err := tne.executor.ParseWithParams(tne.ctx, sql)
			if err != nil {
				return err
			}
			node.Accept(tne)
		}
	}
	if tne.err != nil {
		return tne.err
	}
	r := tne.getTablesAndViews()
	for t := range r {
		p.tables[t] = struct{}{}
	}
	return nil
}

func (w *extractWorker) decodeBinaryPlan(ctx context.Context, bPlan string) (string, error) {
	exec := w.sctx.GetRestrictedSQLExecutor()
	ctx1 := kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	rows, _, err := exec.ExecRestrictedSQL(ctx1, nil, fmt.Sprintf("SELECT tidb_decode_binary_plan('%s')", bPlan))
	if err != nil {
		return "", err
	}
	plan := rows[0].GetString(0)
	return strings.Trim(plan, "\n"), nil
}

// dumpExtractPlanPackage will dump the information about sqls collected in stmt_summary_history
// The files will be organized into the following format:
/*
 |-extract_meta.txt
 |-meta.txt
 |-config.toml
 |-variables.toml
 |-bindings.sql
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
 |-table_tiflash_replica.txt
 |-sql
 |   |-digest1.sql
 |   |-digest2.sql
 |	 |-....
 |-skippedSQLs
 |	 |-digest1.sql
 |	 |-...
*/
func (w *extractWorker) dumpExtractPlanPackage(task *ExtractTask, p *extractPlanPackage) (name string, err error) {
	f, name, err := GenerateExtractFile()
	if err != nil {
		return "", err
	}
	zw := zip.NewWriter(f)
	defer func() {
		if err != nil {
			logutil.BgLogger().Error("dump extract plan task failed", zap.Error(err))
		}
		if err1 := zw.Close(); err1 != nil {
			logutil.BgLogger().Warn("close zip file failed", zap.String("file", name), zap.Error(err))
		}
		if err1 := f.Close(); err1 != nil {
			logutil.BgLogger().Warn("close file failed", zap.String("file", name), zap.Error(err))
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
	// dump extract plan task meta
	if err = dumpExtractMeta(task, zw); err != nil {
		return "", err
	}
	// Dump Schema and View
	if err = dumpSchemas(w.sctx, zw, p.tables); err != nil {
		return "", err
	}
	// Dump tables tiflash replicas
	if err = dumpTiFlashReplica(w.sctx, zw, p.tables); err != nil {
		return "", err
	}
	// Dump variables
	if err = dumpVariables(w.sctx, w.sctx.GetSessionVars(), zw); err != nil {
		return "", err
	}
	// Dump global bindings
	if err = dumpGlobalBindings(w.sctx, zw); err != nil {
		return "", err
	}
	// Dump stats
	if !task.SkipStats {
		if _, err = dumpStats(zw, p.tables, GetDomain(w.sctx), 0); err != nil {
			return "", err
		}
	}
	// Dump sqls and plan
	if err = dumpSQLRecords(p.records, zw); err != nil {
		return "", err
	}
	return name, nil
}

func dumpSQLRecords(records map[stmtSummaryHistoryKey]*stmtSummaryHistoryRecord, zw *zip.Writer) error {
	for key, record := range records {
		if record.skip {
			err := dumpSQLRecord(record, fmt.Sprintf("skippedSQLs/%v.json", key.digest), zw)
			if err != nil {
				return err
			}
		} else {
			err := dumpSQLRecord(record, fmt.Sprintf("SQLs/%v.json", key.digest), zw)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type singleSQLRecord struct {
	Schema     string `json:"schema"`
	Plan       string `json:"plan"`
	SQL        string `json:"sql"`
	Digest     string `json:"digest"`
	BinaryPlan string `json:"binaryPlan"`
	UserName   string `json:"userName"`
}

// dumpSQLRecord dumps sql records into one file for each record, the format is in json.
func dumpSQLRecord(record *stmtSummaryHistoryRecord, path string, zw *zip.Writer) error {
	zf, err := zw.Create(path)
	if err != nil {
		return err
	}
	singleSQLRecord := &singleSQLRecord{
		Schema:     record.schemaName,
		Plan:       record.plan,
		SQL:        record.sql,
		Digest:     record.digest,
		BinaryPlan: record.binaryPlan,
		UserName:   record.userName,
	}
	content, err := json.Marshal(singleSQLRecord)
	if err != nil {
		return err
	}
	_, err = zf.Write(content)
	if err != nil {
		return err
	}
	return nil
}

func dumpExtractMeta(task *ExtractTask, zw *zip.Writer) error {
	cf, err := zw.Create(ExtractMetaFile)
	if err != nil {
		return errors.AddStack(err)
	}
	varMap := make(map[string]string)
	varMap[ExtractTaskType] = taskTypeToString(task.ExtractType)
	if task.ExtractType == ExtractPlanType {
		varMap[ExtractPlanTaskSkipStats] = strconv.FormatBool(task.SkipStats)
	}

	if err := toml.NewEncoder(cf).Encode(varMap); err != nil {
		return errors.AddStack(err)
	}
	return nil
}

type extractPlanPackage struct {
	tables  map[tableNamePair]struct{}
	records map[stmtSummaryHistoryKey]*stmtSummaryHistoryRecord
}

type stmtSummaryHistoryKey struct {
	digest     string
	planDigest string
}

type stmtSummaryHistoryRecord struct {
	stmtType   string
	schemaName string
	tables     []tableNamePair
	digest     string
	planDigest string
	sql        string
	binaryPlan string
	userName   string

	plan string
	skip bool
}

// GenerateExtractFile generates extract stmt file
func GenerateExtractFile() (*os.File, string, error) {
	path := GetExtractTaskDirName()
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	fileName, err := generateExtractStmtFile()
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	zf, err := os.Create(filepath.Join(path, fileName))
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	return zf, fileName, err
}

func generateExtractStmtFile() (string, error) {
	// Generate key and create zip file
	time := time.Now().UnixNano()
	b := make([]byte, 16)
	//nolint: gosec
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	key := base64.URLEncoding.EncodeToString(b)
	return fmt.Sprintf("extract_%v_%v.zip", key, time), nil
}

// GetExtractTaskDirName get extract dir name
func GetExtractTaskDirName() string {
	tidbLogDir := filepath.Dir(config.GetGlobalConfig().Log.File.Filename)
	return filepath.Join(tidbLogDir, "extract")
}
