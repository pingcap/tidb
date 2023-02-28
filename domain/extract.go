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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	// ExtractMetaFile indicates meta file for extract
	ExtractMetaFile = "meta.txt"
)

const (
	// ExtractTaskType indicates type of extract task
	ExtractTaskType = "taskType"
)

// ExtractType indicates type
type ExtractType uint8

const (
	// ExtractPlanType indicates extract plan task
	ExtractPlanType ExtractType = iota
)

func taskTypeToString(t ExtractType) string {
	switch t {
	case ExtractPlanType:
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
	switch task.ExtractType {
	case ExtractPlanType:
		return w.extractPlanTask(ctx, task)
	}
	return "", errors.New("unknown extract task")
}

func (w *extractWorker) extractPlanTask(ctx context.Context, task *ExtractTask) (string, error) {
	if !config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
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
	return w.dumpExtractPlanPackage(p)
}

func (w *extractWorker) collectRecords(ctx context.Context, task *ExtractTask) (map[stmtSummaryHistoryKey]stmtSummaryHistoryRecord, error) {
	w.Lock()
	defer w.Unlock()
	exec := w.sctx.(sqlexec.RestrictedSQLExecutor)
	ctx1 := kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	rows, _, err := exec.ExecRestrictedSQL(ctx1, nil, fmt.Sprintf("SELECT STMT_TYPE, TABLE_NAMES, DIGEST, PLAN_DIGEST,QUERY_SAMPLE_TEXT, BINARY_PLAN FROM INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY WHERE SUMMARY_END_TIME > '%s' OR SUMMARY_BEGIN_TIME < '%s'",
		task.Begin.Format(types.TimeFormat), task.End.Format(types.TimeFormat)))
	if err != nil {
		return nil, err
	}
	collectMap := make(map[stmtSummaryHistoryKey]stmtSummaryHistoryRecord, 0)
	is := GetDomain(w.sctx).InfoSchema()
	for _, row := range rows {
		record := stmtSummaryHistoryRecord{}
		record.stmtType = row.GetString(0)
		record.digest = row.GetString(2)
		record.planDigest = row.GetString(3)
		record.sql = row.GetString(4)
		record.binaryPlan = row.GetString(5)
		key := stmtSummaryHistoryKey{
			digest:     record.digest,
			planDigest: record.planDigest,
		}
		record.tables = make([]tableNamePair, 0)
		tables := row.GetString(1)
		setRecord := true

		for _, t := range strings.Split(tables, ",") {
			names := strings.Split(t, ".")
			if len(names) != 2 {
				setRecord = false
				break
			}
			dbName := names[0]
			tblName := names[1]
			t, err := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
			if err != nil {
				return nil, err
			}
			record.schemaName = dbName
			// skip internal schema record
			switch strings.ToLower(record.schemaName) {
			case util.PerformanceSchemaName.L, util.InformationSchemaName.L, util.MetricSchemaName.L, "mysql":
				setRecord = false
			}
			if !setRecord {
				break
			}
			record.tables = append(record.tables, tableNamePair{DBName: dbName, TableName: tblName, IsView: t.Meta().IsView()})
		}
		if setRecord && checkRecordValid(record) {
			collectMap[key] = record
		}
	}
	return collectMap, nil
}

func checkRecordValid(r stmtSummaryHistoryRecord) bool {
	if r.schemaName == "" {
		return false
	}
	if r.planDigest == "" {
		return false
	}
	return true
}

func (w *extractWorker) packageExtractPlanRecords(ctx context.Context, records map[stmtSummaryHistoryKey]stmtSummaryHistoryRecord) (*extractPlanPackage, error) {
	p := &extractPlanPackage{}
	p.sqls = make([]string, 0)
	p.plans = make([]string, 0)
	p.skippedSQLs = make([]string, 0)
	p.tables = make(map[tableNamePair]struct{}, 0)
	for _, record := range records {
		// skip the sql which has been cut off
		if strings.Contains(record.sql, "(len:") {
			p.skippedSQLs = append(p.skippedSQLs, record.sql)
			continue
		}
		p.sqls = append(p.sqls, record.sql)
		plan, err := w.decodeBinaryPlan(ctx, record.binaryPlan)
		if err != nil {
			return nil, err
		}
		p.plans = append(p.plans, plan)
		for _, tbl := range record.tables {
			p.tables[tbl] = struct{}{}
		}
	}
	return p, nil
}

func (w *extractWorker) decodeBinaryPlan(ctx context.Context, bPlan string) (string, error) {
	exec := w.sctx.(sqlexec.RestrictedSQLExecutor)
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
 |-table_tiflash_replica.txt
 |-sql
 |   |-sqls.sql
 |   |-skippedSQLs.sql
*/
func (w *extractWorker) dumpExtractPlanPackage(p *extractPlanPackage) (name string, err error) {
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

	// dump extract plan task meta
	if err = dumpExtractMeta(ExtractPlanType, zw); err != nil {
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
	// Dump stats
	if err = dumpStats(zw, p.tables, GetDomain(w.sctx)); err != nil {
		return "", err
	}
	// Dump sqls
	if err = dumpExtractPlanSQLs(p.sqls, p.skippedSQLs, zw); err != nil {
		return "", err
	}
	// dump plans
	if err = dumpExtractPlans(p.plans, zw); err != nil {
		return "", err
	}
	return name, nil
}

func dumpExtractPlanSQLs(sqls, skippedSQLs []string, zw *zip.Writer) error {
	if err := dumpTargetSQLs(sqls, "sql/sqls.sql", zw); err != nil {
		return err
	}
	return dumpTargetSQLs(skippedSQLs, "sql/skippedSQLs.sql", zw)
}

func dumpExtractPlans(plans []string, zw *zip.Writer) error {
	zf, err := zw.Create("plans.txt")
	if err != nil {
		return err
	}
	for i, plan := range plans {
		_, err = zf.Write([]byte(plan))
		if err != nil {
			return err
		}
		if i < len(plans)-1 {
			_, err = zf.Write([]byte("\n<--------->\n"))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func dumpTargetSQLs(sqls []string, path string, zw *zip.Writer) error {
	zf, err := zw.Create(path)
	if err != nil {
		return err
	}
	for _, sql := range sqls {
		_, err = zf.Write([]byte(fmt.Sprintf("%s;\n", sql)))
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpExtractMeta(t ExtractType, zw *zip.Writer) error {
	cf, err := zw.Create(ExtractMetaFile)
	if err != nil {
		return errors.AddStack(err)
	}
	varMap := make(map[string]string)
	varMap[ExtractTaskType] = taskTypeToString(t)
	if err := toml.NewEncoder(cf).Encode(varMap); err != nil {
		return errors.AddStack(err)
	}
	return nil
}

type extractPlanPackage struct {
	sqls        []string
	plans       []string
	skippedSQLs []string
	tables      map[tableNamePair]struct{}
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
