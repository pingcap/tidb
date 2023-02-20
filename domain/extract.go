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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// ExtractType indicates type
type ExtractType uint8

const (
	// ExtractPlanType indicates extract plan task
	ExtractPlanType ExtractType = iota
)

// extractHandle handles the extractWorker to run extract the information task like Plan or any others.
// extractHandle will provide 2 mode for extractWorker:
// 1. submit a background extract task, the response will be returned after the task is started to be solved
// 2. submit a task and wait until the task is solved, the result will be returned to the response.
type extractHandle struct {
	worker *extractWorker
}

// NewExtractHandler new extract handler
func NewExtractHandler(sctxs []sessionctx.Context) *extractHandle {
	h := &extractHandle{}
	h.worker = newExtractWorker(sctxs[0], false)
	return h
}

// ExtractTask extract tasks
func (h *extractHandle) ExtractTask(ctx context.Context, task *ExtractTask, isBackgroundTask bool) (string, error) {
	if !config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return "", errors.New("tidb_stmt_summary_enable_persistent should be enabled for extract task")
	}
	// TODO: support background job later
	if isBackgroundTask {
		return "", nil
	}
	return h.worker.extractTask(ctx, task)
}

type extractWorker struct {
	ctx                context.Context
	sctx               sessionctx.Context
	isBackgroundWorker bool
}

// ExtractTask indicates task
type ExtractTask struct {
	extractType ExtractType
	Begin       time.Time
	End         time.Time
}

// NewExtractPlanTask returns extract plan task
func NewExtractPlanTask(begin, end time.Time) *ExtractTask {
	return &ExtractTask{
		Begin:       begin,
		End:         end,
		extractType: ExtractPlanType,
	}
}

func newExtractWorker(sctx sessionctx.Context, isBackgroundWorker bool) *extractWorker {
	return &extractWorker{
		sctx:               sctx,
		isBackgroundWorker: isBackgroundWorker,
	}
}

func (w *extractWorker) extractTask(ctx context.Context, task *ExtractTask) (string, error) {
	switch task.extractType {
	case ExtractPlanType:
		return w.extractPlanTask(ctx, task)
	}
	return "", nil
}

func (w *extractWorker) extractPlanTask(ctx context.Context, task *ExtractTask) (string, error) {
	records, err := w.collectRecords(ctx, task)
	if err != nil {
		return "", err
	}
	p := w.packageExtractPlanRecords(records)
	return w.dumpExtractPlanPackage(p)
}

func (w *extractWorker) collectRecords(ctx context.Context, task *ExtractTask) (map[stmtSummaryHistoryKey]stmtSummaryHistoryRecord, error) {
	exec := w.sctx.(sqlexec.RestrictedSQLExecutor)
	ctx1 := kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	rows, _, err := exec.ExecRestrictedSQL(ctx1, nil, fmt.Sprintf("SELECT STMT_TYPE, SCHEMA_NAME, TABLE_NAMES, DIGEST, PLAN_DIGEST,QUERY_SAMPLE_TEXT FROM INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY WHERE SUMMARY_END_TIME > '%s' OR SUMMARY_BEGIN_TIME < '%s'",
		task.Begin.Format(types.TimeFormat), task.End.Format(types.TimeFormat)))
	if err != nil {
		return nil, err
	}
	collectMap := make(map[stmtSummaryHistoryKey]stmtSummaryHistoryRecord, 0)
	for _, row := range rows {
		record := stmtSummaryHistoryRecord{}
		record.stmtType = row.GetString(0)
		record.schemaName = row.GetString(1)
		record.digest = row.GetString(3)
		record.planDigest = row.GetString(4)
		record.sql = row.GetString(5)
		if !checkRecordValid(record) {
			continue
		}
		key := stmtSummaryHistoryKey{
			digest:     record.digest,
			planDigest: record.planDigest,
		}
		record.tables = make([]tableNamePair, 0)
		tables := row.GetString(2)
		setRecord := true

		for _, t := range strings.Split(tables, ",") {
			names := strings.Split(t, ".")
			if len(names) != 2 {
				setRecord = false
				break
			}
			dbName := names[0]
			tblName := names[1]
			// TODO: support check isView here
			record.tables = append(record.tables, tableNamePair{DBName: dbName, TableName: tblName, IsView: false})
		}
		if setRecord {
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

func (w *extractWorker) packageExtractPlanRecords(records map[stmtSummaryHistoryKey]stmtSummaryHistoryRecord) *extractPlanPackage {
	p := &extractPlanPackage{}
	p.sqls = make([]string, 0)
	p.skippedSQLs = make([]string, 0)
	p.tables = make(map[tableNamePair]struct{}, 0)
	for _, record := range records {
		if strings.Contains(record.sql, "(len:") {
			p.skippedSQLs = append(p.skippedSQLs, record.sql)
			continue
		}
		// TODO: skip internal schema record

		p.sqls = append(p.sqls, record.sql)
		for _, tbl := range record.tables {
			p.tables[tbl] = struct{}{}
		}
	}
	return p
}

func (w *extractWorker) dumpExtractPlanPackage(p *extractPlanPackage) (string, error) {
	f, name, err := GenerateExtractFile()
	if err != nil {
		return "", err
	}
	zw := zip.NewWriter(f)
	defer func() {
		if err1 := zw.Close(); err1 != nil {
			logutil.BgLogger().Warn("close zip file failed", zap.String("file", name), zap.Error(err))
		}
		if err1 := f.Close(); err1 != nil {
			logutil.BgLogger().Warn("close file failed", zap.String("file", name), zap.Error(err))
		}
	}()

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
	if err = dumpExtractPlanSQLs(p.sqls, zw); err != nil {
		return "", err
	}
	return name, nil
}

func dumpExtractPlanSQLs(sqls []string, zw *zip.Writer) error {
	zf, err := zw.Create("sql/sqls.sql")
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

type extractPlanPackage struct {
	sqls        []string
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
