// Copyright 2020 PingCAP, Inc.
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
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/storage"
	brtask "github.com/pingcap/br/pkg/task"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb-lightning/lightning"
	"github.com/pingcap/tidb-lightning/lightning/checkpoints"
	"github.com/pingcap/tidb-lightning/lightning/common"
	importcfg "github.com/pingcap/tidb-lightning/lightning/config"
	importglue "github.com/pingcap/tidb-lightning/lightning/glue"
	importlog "github.com/pingcap/tidb-lightning/lightning/log"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	pd "github.com/tikv/pd/client"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	defaultImportID = "tidb_import_"

	// current commands/status that displayed in progress
	// there're still some commands didn't list here, whereas they are used as arguments of StartProgress in BR
	cmdWait        = "Wait"
	cmdBackup      = "Backup"
	cmdRestore     = "Restore"
	cmdImport      = "Import"
	cmdPostProcess = "Post-process"
	cmdStop        = "Stop"
	cmdFinish      = "Finish"
)

var (
	// CreateSessionForBRIEFunc is used to solve cycle import when use session.CreateSession
	CreateSessionForBRIEFunc func(sessionctx.Context) (checkpoints.Session, error)
)

// brieTaskProgress tracks a task's current progress.
type brieTaskProgress struct {
	// cmd is the name of the step the BRIE task is currently performing.
	// We didn't keep it consistent with below two numeric fields. That is to say, user may see
	// Import 99% -> Import 100% -> Post Process 100% , where 100% always means "Post Process"
	// To avoid concurrency problems, always use getter/setter
	cmd atomic2.String

	// the percentage of completeness is `(100%) * current / total`.
	// in order to keep those two fields consistent, we must acquire mutex firstly.
	lock struct {
		sync.Mutex
		current int64
		total   int64
	}
}

func (p *brieTaskProgress) getCmd() string {
	return p.cmd.Load()
}

func (p *brieTaskProgress) setCmd(c string) {
	p.cmd.Store(c)
}

// Inc implements glue.Progress
func (p *brieTaskProgress) Inc() {
	p.lock.Lock()
	p.lock.current += 1
	p.lock.Unlock()
}

// Close implements glue.Progress
func (p *brieTaskProgress) Close() {
	p.lock.Lock()
	p.lock.current = p.lock.total
	p.lock.Unlock()
}

// getFraction returns a real number whose range is [0, 100]
func (p *brieTaskProgress) getFraction() float64 {
	p.lock.Lock()
	if p.lock.total == 0 {
		p.lock.Unlock()
		return 0
	}
	result := 100 * float64(p.lock.current) / float64(p.lock.total)
	p.lock.Unlock()
	// progress of import task is estimated, so may exceed 100%. we simply adjust here
	if result > 100 {
		result = 100
	}
	return result
}

type brieTaskInfo struct {
	queueTime   types.Time
	kind        ast.BRIEKind
	storage     string
	connID      uint64
	originSQL   string
	execTime    atomic.Value
	finishTime  atomic.Value
	backupTS    uint64
	archiveSize uint64
	message     atomic2.String
}

func (b *brieTaskInfo) getExecTime() types.Time {
	inner := b.execTime.Load()
	if inner == nil {
		return types.ZeroTime
	}
	return inner.(types.Time)
}

func (b *brieTaskInfo) setExecTime(t types.Time) {
	b.execTime.Store(t)
}

func (b *brieTaskInfo) getFinishTime() types.Time {
	inner := b.finishTime.Load()
	if inner == nil {
		return types.ZeroTime
	}
	return inner.(types.Time)
}

func (b *brieTaskInfo) setFinishTime(t types.Time) {
	b.finishTime.Store(t)
}

func (b *brieTaskInfo) getBackupTS() uint64 {
	return atomic.LoadUint64(&b.backupTS)
}

func (b *brieTaskInfo) setBackupTS(ts uint64) {
	atomic.StoreUint64(&b.backupTS, ts)
}

func (b *brieTaskInfo) getArchiveSize() uint64 {
	return atomic.LoadUint64(&b.archiveSize)
}

func (b *brieTaskInfo) setArchiveSize(s uint64) {
	atomic.StoreUint64(&b.archiveSize, s)
}

func (b *brieTaskInfo) getMessage() string {
	return b.message.Load()
}

func (b *brieTaskInfo) SetMessage(m string) {
	b.message.Store(m)
}

type brieQueueItem struct {
	info     *brieTaskInfo
	progress *brieTaskProgress
	cancel   func()
}

type brieQueue struct {
	// currently only one BRIE task could run on one TiDB
	workerCh chan struct{}
}

// globalBRIEQueue is the BRIE execution queue. Only one BRIE task can be executed each time.
// TODO: perhaps copy the DDL Job queue so only one task can be executed in the whole cluster.
var globalBRIEQueue = &brieQueue{
	workerCh: make(chan struct{}, 1),
}

// registerTask registers a BRIE task in the queue.
func (bq *brieQueue) registerTask(
	ctx context.Context,
	info *brieTaskInfo,
	s sqlexec.RestrictedSQLExecutor,
) (context.Context, uint64, *brieQueueItem, error) {
	taskCtx, taskCancel := context.WithCancel(ctx)
	item := &brieQueueItem{
		info:     info,
		cancel:   taskCancel,
		progress: &brieTaskProgress{},
	}
	item.progress.lock.total = 1
	item.progress.setCmd(cmdWait)

	// use format.OutputFormat() to avoid SQL injection
	sql := fmt.Sprintf(`INSERT INTO mysql.brie_tasks (
				kind, origin_sql, queue_time, data_path, conn_id, status, progress, cancel
			) VALUES ('%s', '%s', '%s', '%s', %d, '%s', %f, %d);`,
		info.kind.String(),
		format.OutputFormat(info.originSQL),
		info.queueTime.String(),
		format.OutputFormat(info.storage),
		info.connID,
		item.progress.getCmd(),
		item.progress.getFraction(),
		0,
	)
	_, _, err := s.ExecRestrictedSQLWithContext(ctx, sql)
	if err != nil {
		return nil, 0, nil, err
	}
	sql = fmt.Sprintf("SELECT MAX(id) FROM mysql.brie_tasks WHERE conn_id = %d;", info.connID)
	rows, _, err := s.ExecRestrictedSQLWithContext(ctx, sql)
	if err != nil {
		return nil, 0, nil, err
	}
	if len(rows) != 1 {
		return nil, 0, nil, fmt.Errorf("%s didn't return one row, err: %v", sql, err)
	}
	r := rows[0]
	taskID := r.GetUint64(0)
	return taskCtx, taskID, item, nil
}

// acquireTask prepares to execute a BRIE task. Only one BRIE task can be
// executed at a time, and this function blocks until the task is ready.
func (bq *brieQueue) acquireTask(taskCtx context.Context, taskID uint64, se sqlexec.RestrictedSQLExecutor) (err error) {
	// wait until we are at the front of the queue.
	select {
	case bq.workerCh <- struct{}{}:
		defer func() {
			if err != nil {
				bq.releaseTask()
			}
		}()
		sql := fmt.Sprintf("SELECT cancel FROM mysql.brie_tasks WHERE id = %d;", taskID)
		rows, _, err := se.ExecRestrictedSQLWithContext(taskCtx, sql)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.Errorf("task with ID %d not found", taskID)
		}
		r := rows[0]
		if r.GetUint64(0) == 1 {
			return errors.Errorf("task with ID %d has been canceled", taskID)
		}
		return nil
	case <-taskCtx.Done():
		return taskCtx.Err()
	}
}

func (bq *brieQueue) releaseTask() {
	<-bq.workerCh
}

// cancelTask cleans task status in mysql.brie_tasks. please note that cancellation of ctx is outside of this function.
// delete param means if it should delete record or update cancel status of record. The latter is used in import tasks.
func (bq *brieQueue) cancelTask(ctx context.Context, taskID uint64, se sqlexec.RestrictedSQLExecutor, delete bool) {
	var sql string
	if delete {
		sql = fmt.Sprintf("DELETE FROM mysql.brie_tasks WHERE id = %d;", taskID)
	} else {
		sql = fmt.Sprintf("UPDATE mysql.brie_tasks SET cancel = 1 WHERE id = %d;", taskID)
	}

	_, _, err := se.ExecRestrictedSQLWithContext(ctx, sql)
	if err != nil {
		logutil.Logger(ctx).Error("failed to update BRIE task table to cancel",
			zap.Uint64("taskID", taskID),
			zap.Error(err))
	}
}

func (b *executorBuilder) parseTSString(ts string) (uint64, error) {
	sc := &stmtctx.StatementContext{TimeZone: b.ctx.GetSessionVars().Location()}
	t, err := types.ParseTime(sc, ts, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return 0, err
	}
	t1, err := t.GoTime(sc.TimeZone)
	if err != nil {
		return 0, err
	}
	return variable.GoTimeToTS(t1), nil
}

func (b *executorBuilder) buildBackupRestore(s *ast.BRIEStmt, schema *expression.Schema) Executor {
	e := &BRIEExec{
		baseExecutor: newBaseExecutor(b.ctx, schema, 0),
		info: &brieTaskInfo{
			kind:      s.Kind,
			originSQL: s.SecureText(),
		},
	}
	tidbCfg := config.GetGlobalConfig()

	// BACKUP and RESTORE shares same config
	var brCfg = brtask.Config{
		TLS: brtask.TLSConfig{
			CA:   tidbCfg.Security.ClusterSSLCA,
			Cert: tidbCfg.Security.ClusterSSLCert,
			Key:  tidbCfg.Security.ClusterSSLKey,
		},
		PD:          strings.Split(tidbCfg.Path, ","),
		Concurrency: 4,
		Checksum:    true,
		SendCreds:   true,
		LogProgress: true,
	}

	// handle s.Storage:
	// BACKUP ... TO **s.Storage**
	// RESTORE ... FROM **s.Storage**
	storageURL, err := url.Parse(s.Storage)
	if err != nil {
		b.err = ErrBRIEInvalidExternalStore.GenWithStackByArgs(s.Storage, err)
		return nil
	}
	switch storageURL.Scheme {
	case "s3":
		storage.ExtractQueryParameters(storageURL, &brCfg.S3)
	case "gs", "gcs":
		storage.ExtractQueryParameters(storageURL, &brCfg.GCS)
	default:
		break
	}
	brCfg.Storage = storageURL.String()
	e.info.storage = storageURL.String()
	for _, opt := range s.Options {
		switch opt.Tp {
		case ast.BRIEOptionRateLimit:
			brCfg.RateLimit = opt.UintValue
		case ast.BRIEOptionConcurrency:
			brCfg.Concurrency = uint32(opt.UintValue)
		case ast.BRIEOptionChecksum:
			brCfg.Checksum = opt.UintValue != 0
		case ast.BRIEOptionSendCreds:
			brCfg.SendCreds = opt.UintValue != 0
		}
	}

	// handle s.Tables:
	// BACKUP **s.Tables** TO ...
	// RESTORE **s.Tables** FROM ...
	switch {
	case len(s.Tables) != 0:
		tables := make([]filter.Table, 0, len(s.Tables))
		for _, tbl := range s.Tables {
			tables = append(tables, filter.Table{Name: tbl.Name.O, Schema: tbl.Schema.O})
		}
		brCfg.TableFilter = filter.NewTablesFilter(tables...)
	case len(s.Schemas) != 0:
		brCfg.TableFilter = filter.NewSchemasFilter(s.Schemas...)
	default:
		brCfg.TableFilter = filter.All()
	}

	if tidbCfg.LowerCaseTableNames != 0 {
		brCfg.TableFilter = filter.CaseInsensitive(brCfg.TableFilter)
	}

	// handle other options
	switch s.Kind {
	case ast.BRIEKindBackup:
		e.backupCfg = &brtask.BackupConfig{Config: brCfg}
		for _, opt := range s.Options {
			switch opt.Tp {
			case ast.BRIEOptionLastBackupTS:
				tso, err := b.parseTSString(opt.StrValue)
				if err != nil {
					b.err = ErrBRIEInvalidOption.GenWithStackByArgs(opt.StrValue, err)
					return nil
				}
				e.backupCfg.LastBackupTS = tso
			case ast.BRIEOptionLastBackupTSO:
				e.backupCfg.LastBackupTS = opt.UintValue
			case ast.BRIEOptionBackupTimeAgo:
				e.backupCfg.TimeAgo = time.Duration(opt.UintValue)
			case ast.BRIEOptionBackupTSO:
				e.backupCfg.BackupTS = opt.UintValue
			case ast.BRIEOptionBackupTS:
				tso, err := b.parseTSString(opt.StrValue)
				if err != nil {
					b.err = ErrBRIEInvalidOption.GenWithStackByArgs(opt.StrValue, err)
					return nil
				}
				e.backupCfg.BackupTS = tso
			}
		}

	case ast.BRIEKindRestore:
		e.restoreCfg = &brtask.RestoreConfig{Config: brCfg}
		for _, opt := range s.Options {
			switch opt.Tp {
			case ast.BRIEOptionOnline:
				e.restoreCfg.Online = opt.UintValue != 0
			}
		}
	}

	return e
}

func (b *executorBuilder) buildImport(s *ast.BRIEStmt, schema *expression.Schema) Executor {
	e := &BRIEExec{
		baseExecutor: newBaseExecutor(b.ctx, schema, 0),
		info: &brieTaskInfo{
			kind:      s.Kind,
			originSQL: s.SecureText(),
		},
	}
	tidbCfg := config.GetGlobalConfig()

	var (
		// IMPORT has two types of config, task config for each import task
		// global config for lightning as a long-term running server that serve many tasks
		importGlobalCfg = importcfg.NewGlobalConfig()
		importTaskCfg   = importcfg.NewConfig()
	)
	// set proper values to embedded lightning
	importGlobalCfg.App.StatusAddr = ":8289"
	importGlobalCfg.App.Level = tidbCfg.Log.Level
	importGlobalCfg.Security.CAPath = tidbCfg.Security.ClusterSSLCA
	importGlobalCfg.Security.CertPath = tidbCfg.Security.ClusterSSLCert
	importGlobalCfg.Security.KeyPath = tidbCfg.Security.ClusterSSLKey
	importGlobalCfg.TiDB.Port = int(tidbCfg.Port)
	importGlobalCfg.TiDB.PdAddr = strings.Split(tidbCfg.Path, ",")[0]

	// set Port and PdAddr to avoid lightning query TiDB HTTP API
	importTaskCfg.TiDB.Port = int(tidbCfg.Port)
	importTaskCfg.TiDB.PdAddr = strings.Split(tidbCfg.Path, ",")[0]
	// StatusPort is not used, but we could pass it to improve robust
	importTaskCfg.TiDB.StatusPort = int(tidbCfg.Status.StatusPort)
	importTaskCfg.TikvImporter.Backend = importcfg.BackendLocal
	importTaskCfg.TikvImporter.SortedKVDir = filepath.Join(tidbCfg.TempStoragePath, defaultImportID)
	importTaskCfg.Checkpoint.Schema = defaultImportID
	importTaskCfg.Checkpoint.Driver = importcfg.CheckpointDriverMySQL
	importTaskCfg.Mydumper.CSV.Header = false // TODO(lance6716): address this behaviour to user

	// handle s.Storage:
	// IMPORT ... FROM **s.Storage**
	storageURL, err := url.Parse(s.Storage)
	if err != nil {
		b.err = ErrBRIEInvalidExternalStore.GenWithStackByArgs(s.Storage, err)
		return nil
	}
	importTaskCfg.Mydumper.SourceDir = storageURL.String()
	e.info.storage = storageURL.String()

	// handle s.Tables:
	// IMPORT **s.Tables** FROM ...
	switch {
	case len(s.Tables) != 0:
		tbls := make([]string, 0, len(s.Tables))
		for _, tbl := range s.Tables {
			if tbl.Schema.L == "" {
				b.err = ErrBRIEInvalidTable.GenWithStackByArgs(fmt.Sprintf("please specify schema for %s in IMPORT", tbl.Name.O))
				return nil
			}
			tbls = append(tbls, fmt.Sprintf("%s.%s", tbl.Schema, tbl.Name))
		}
		importTaskCfg.Mydumper.Filter = tbls
	case len(s.Schemas) != 0:
		dbs := make([]string, 0, len(s.Schemas))
		for _, db := range s.Schemas {
			dbs = append(dbs, fmt.Sprintf("%s.*", db))
		}
		importTaskCfg.Mydumper.Filter = dbs
	default:
		// no filter means pass all
	}

	// handle options
	for _, opt := range s.Options {
		switch opt.Tp {
		case ast.BRIEOptionSkipSchemaFiles:
			importTaskCfg.Mydumper.NoSchema = opt.UintValue != 0
		case ast.BRIEOptionStrictFormat:
			importTaskCfg.Mydumper.StrictFormat = opt.UintValue != 0
		case ast.BRIEOptionCSVSeparator:
			importTaskCfg.Mydumper.CSV.Separator = opt.StrValue
		case ast.BRIEOptionCSVDelimiter:
			importTaskCfg.Mydumper.CSV.Delimiter = opt.StrValue
		case ast.BRIEOptionCSVHeader:
			if opt.UintValue == ast.BRIECSVHeaderIsColumns {
				importTaskCfg.Mydumper.CSV.Header = true
			} else {
				b.err = ErrBRIEInvalidOption.GenWithStackByArgs("CSV_HEADER", "only support FIELDS or COLUMNS to indicate it has header")
				return nil
			}
		case ast.BRIEOptionCSVNotNull:
			importTaskCfg.Mydumper.CSV.NotNull = opt.UintValue != 0
		case ast.BRIEOptionCSVNull:
			importTaskCfg.Mydumper.CSV.Null = opt.StrValue
		case ast.BRIEOptionCSVBackslashEscape:
			importTaskCfg.Mydumper.CSV.BackslashEscape = opt.UintValue != 0
		case ast.BRIEOptionCSVTrimLastSeparators:
			importTaskCfg.Mydumper.CSV.TrimLastSep = opt.UintValue != 0
		case ast.BRIEOptionChecksum:
			switch opt.UintValue {
			case uint64(ast.BRIEOptionLevelOff):
				importGlobalCfg.PostRestore.Checksum = importcfg.OpLevelOff
			case uint64(ast.BRIEOptionLevelOptional):
				importGlobalCfg.PostRestore.Checksum = importcfg.OpLevelOptional
			default:
				importGlobalCfg.PostRestore.Checksum = importcfg.OpLevelRequired
			}
		case ast.BRIEOptionAnalyze:
			switch opt.UintValue {
			case uint64(ast.BRIEOptionLevelOff):
				importGlobalCfg.PostRestore.Analyze = importcfg.OpLevelOff
			case uint64(ast.BRIEOptionLevelOptional):
				importGlobalCfg.PostRestore.Analyze = importcfg.OpLevelOptional
			default:
				importGlobalCfg.PostRestore.Analyze = importcfg.OpLevelRequired
			}
		}
	}

	e.importTaskCfg = importTaskCfg
	e.importGlobalCfg = importGlobalCfg

	return e
}

func (b *executorBuilder) buildBRIE(s *ast.BRIEStmt, schema *expression.Schema) Executor {
	tidbCfg := config.GetGlobalConfig()
	if tidbCfg.Store != "tikv" {
		// TODO: when support choose backend for IMPORT, allow mocktikv for tidb backend
		b.err = ErrBRIERequireTiKV.GenWithStackByArgs(s.Kind, tidbCfg.Store)
		return nil
	}

	switch s.Kind {
	case ast.BRIEKindBackup, ast.BRIEKindRestore:
		return b.buildBackupRestore(s, schema)
	case ast.BRIEKindImport:
		return b.buildImport(s, schema)
	default:
		b.err = ErrBRIEUnsupported.GenWithStackByArgs(s.Kind)
		return nil
	}
}

// BRIEExec represents an executor for BRIE statements (BACKUP, RESTORE, etc)
type BRIEExec struct {
	baseExecutor

	backupCfg       *brtask.BackupConfig
	restoreCfg      *brtask.RestoreConfig
	importGlobalCfg *importcfg.GlobalConfig
	importTaskCfg   *importcfg.Config
	info            *brieTaskInfo
}

// Next implements the Executor Next interface.
func (e *BRIEExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.info == nil {
		return nil
	}

	bq := globalBRIEQueue

	taskExecuted := false

	e.info.connID = e.ctx.GetSessionVars().ConnectionID
	e.info.queueTime = types.CurrentTime(mysql.TypeDatetime)

	sqlExecutor := e.ctx.(sqlexec.RestrictedSQLExecutor)

	taskCtx, taskID, item, err := bq.registerTask(ctx, e.info, sqlExecutor)
	if err != nil {
		return handleBRIEError(err, ErrBRIETaskMeta)
	}
	defer func() {
		if !taskExecuted {
			bq.cancelTask(ctx, taskID, sqlExecutor, e.info.kind != ast.BRIEKindImport)
		}
		item.cancel()
	}()

	glue := &tidbGlueSession{
		se:       e.ctx,
		progress: item.progress,
		info:     e.info,
	}

	// manually monitor the Killed status...
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if atomic.LoadUint32(&e.ctx.GetSessionVars().Killed) == 1 {
					bq.cancelTask(ctx, taskID, sqlExecutor, e.info.kind != ast.BRIEKindImport)
					item.cancel()
					return
				}
				glue.flush(taskCtx, taskID)
			case <-taskCtx.Done():
				return
			}
		}
	}()

	err = bq.acquireTask(taskCtx, taskID, sqlExecutor)
	if err != nil {
		return handleBRIEError(err, ErrBRIETaskMeta)
	}
	defer bq.releaseTask()

	e.info.setExecTime(types.CurrentTime(mysql.TypeDatetime))

	switch e.info.kind {
	case ast.BRIEKindBackup:
		err = handleBRIEError(brtask.RunBackup(taskCtx, glue, cmdBackup, e.backupCfg), ErrBRIEBackupFailed)
	case ast.BRIEKindRestore:
		err = handleBRIEError(brtask.RunRestore(taskCtx, glue, cmdRestore, e.restoreCfg), ErrBRIERestoreFailed)
	case ast.BRIEKindImport:
		l := lightning.New(e.importGlobalCfg)
		e.importTaskCfg.Checkpoint.Schema += strconv.FormatUint(taskID, 10)
		e.importTaskCfg.TikvImporter.SortedKVDir += strconv.FormatUint(taskID, 10)
		err2 := handleBRIEError(l.RunOnce(taskCtx, e.importTaskCfg, glue, logutil.Logger(ctx)), ErrBRIEImportFailed)
		if err2 != nil {
			e.info.SetMessage(err2.Error())
			item.progress.setCmd(cmdStop)
		} else {
			item.progress.Close()
			item.progress.setCmd(cmdFinish)
		}
	default:
		return ErrBRIEUnsupported.GenWithStackByArgs(e.info.kind)
	}
	e.info.setFinishTime(types.CurrentTime(mysql.TypeDatetime))
	glue.flush(ctx, taskID)
	taskExecuted = true
	if err != nil {
		return err
	}

	switch e.info.kind {
	case ast.BRIEKindBackup, ast.BRIEKindRestore:
		req.AppendString(0, e.info.storage)
		req.AppendUint64(1, e.info.getArchiveSize())
		req.AppendUint64(2, e.info.getBackupTS())
		req.AppendTime(3, e.info.queueTime)
		req.AppendTime(4, e.info.getExecTime())
	case ast.BRIEKindImport:
		req.AppendUint64(0, taskID)
		req.AppendString(1, item.progress.getCmd())
		req.AppendString(2, e.info.storage)
		req.AppendUint64(3, e.info.getArchiveSize())
		req.AppendTime(4, e.info.queueTime)
		req.AppendTime(5, e.info.getExecTime())
		req.AppendTime(6, e.info.getFinishTime())
		req.AppendString(7, e.info.getMessage())
	}

	e.info = nil
	return nil
}

func handleBRIEError(err error, terror *terror.Error) error {
	if err == nil {
		return nil
	}
	return terror.GenWithStackByArgs(err)
}

func (e *ShowExec) fetchShowBRIE(ctx context.Context, kind ast.BRIEKind) error {
	sql := fmt.Sprintf(`SELECT id, data_path, status, progress, queue_time, exec_time, finish_time, conn_id, message
			FROM mysql.brie_tasks WHERE kind = '%s';`, kind.String())
	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(ctx, sql)
	if err != nil {
		return err
	}

	for _, row := range rows {
		id := row.GetUint64(0)
		e.result.AppendUint64(0, id)
		e.result.AppendString(1, row.GetString(1))
		e.result.AppendString(2, row.GetString(2))
		e.result.AppendFloat32(3, row.GetFloat32(3))
		e.result.AppendTime(4, row.GetTime(4))
		e.result.AppendTime(5, row.GetTime(5))
		e.result.AppendTime(6, row.GetTime(6))
		e.result.AppendUint64(7, row.GetUint64(7))
		e.result.AppendString(8, row.GetString(8))
	}
	return nil
}

type tidbGlueSession struct {
	se       sessionctx.Context
	progress *brieTaskProgress
	info     *brieTaskInfo
}

// GetDomain implements glue.Glue
func (gs *tidbGlueSession) GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domain.GetDomain(gs.se), nil
}

// CreateSession implements glue.Glue
func (gs *tidbGlueSession) CreateSession(store kv.Storage) (glue.Session, error) {
	return gs, nil
}

// Execute implements glue.Session
func (gs *tidbGlueSession) Execute(ctx context.Context, sql string) error {
	_, err := gs.se.(sqlexec.SQLExecutor).Execute(ctx, sql)
	return err
}

// ExecuteWithLog implements importglue.SQLExecutor
func (gs *tidbGlueSession) ExecuteWithLog(ctx context.Context, sql string, purpose string, logger importlog.Logger) error {
	sqlExecutor := gs.se.(sqlexec.RestrictedSQLExecutor)
	return common.Retry(purpose, logger, func() error {
		_, _, err := sqlExecutor.ExecRestrictedSQLWithContext(ctx, sql)
		return err
	})
}

// ObtainStringWithLog implements importglue.SQLExecutor
func (gs *tidbGlueSession) ObtainStringWithLog(ctx context.Context, sql string, purpose string, logger importlog.Logger) (string, error) {
	sqlExecutor := gs.se.(sqlexec.RestrictedSQLExecutor)
	var result string
	err := common.Retry(purpose, logger, func() error {
		rows, _, err := sqlExecutor.ExecRestrictedSQLWithContext(ctx, sql)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.Errorf("empty result while expecting a string, purpose: %s, sql: %s", purpose, sql)
		}
		result = rows[0].GetString(0)
		return nil
	})
	return result, err
}

// CreateDatabase implements glue.Session
func (gs *tidbGlueSession) CreateDatabase(ctx context.Context, schema *model.DBInfo) error {
	d := domain.GetDomain(gs.se).DDL()
	schema = schema.Clone()
	if len(schema.Charset) == 0 {
		schema.Charset = mysql.DefaultCharset
	}
	return d.CreateSchemaWithInfo(gs.se, schema, ddl.OnExistIgnore, true)
}

// CreateTable implements glue.Session
func (gs *tidbGlueSession) CreateTable(ctx context.Context, dbName model.CIStr, table *model.TableInfo) error {
	d := domain.GetDomain(gs.se).DDL()

	// Clone() does not clone partitions yet :(
	table = table.Clone()
	if table.Partition != nil {
		newPartition := *table.Partition
		newPartition.Definitions = append([]model.PartitionDefinition{}, table.Partition.Definitions...)
		table.Partition = &newPartition
	}

	return d.CreateTableWithInfo(gs.se, dbName, table, ddl.OnExistIgnore, true)
}

// Close implements glue.Session
func (gs *tidbGlueSession) Close() {
}

// Open implements glue.Glue
func (gs *tidbGlueSession) Open(string, pd.SecurityOption) (kv.Storage, error) {
	return gs.se.GetStore(), nil
}

// OwnsStorage implements glue.Glue
func (gs *tidbGlueSession) OwnsStorage() bool {
	return false
}

// StartProgress implements glue.Glue
func (gs *tidbGlueSession) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	gs.progress.lock.Lock()
	gs.progress.setCmd(cmdName)
	gs.progress.lock.current = 0
	gs.progress.lock.total = total
	gs.progress.lock.Unlock()
	return gs.progress
}

// Record implements glue.Glue
func (gs *tidbGlueSession) Record(name string, value uint64) {
	switch name {
	case "BackupTS":
		gs.info.setBackupTS(value)
	case "Size":
		gs.info.setArchiveSize(value)
	// importglue.RecordEstimatedChunk is sent only once and before importglue.RecordFinishedChunk
	case importglue.RecordEstimatedChunk:
		gs.StartProgress(context.Background(), cmdImport, int64(value), false)
	case importglue.RecordFinishedChunk:
		current := int64(value)
		gs.progress.lock.Lock()
		if current >= gs.progress.lock.total {
			// TODO(lance6717): to be more exact, lightning could report status of restoring tables instead of chunks
			gs.progress.setCmd(cmdPostProcess)
		}
		gs.progress.lock.current = current
		gs.progress.lock.Unlock()
	}
}

func (gs *tidbGlueSession) flush(ctx context.Context, taskID uint64) {
	sql := fmt.Sprintf(`UPDATE mysql.brie_tasks SET
					exec_time = '%s',
					finish_time = '%s',
					ts = %d,
					data_size = %d,
					status = '%s',
					progress = %f,
					message = '%s'
				WHERE id = %d`,
		gs.info.getExecTime().String(),
		gs.info.getFinishTime().String(),
		gs.info.getBackupTS(),
		gs.info.getArchiveSize(),
		gs.progress.getCmd(),
		gs.progress.getFraction(),
		format.OutputFormat(gs.info.getMessage()),
		taskID)

	sqlExecutor := gs.se.(sqlexec.RestrictedSQLExecutor)
	_, _, err := sqlExecutor.ExecRestrictedSQLWithContext(ctx, sql)
	if err != nil {
		logutil.Logger(ctx).Error("failed to update BRIE task table",
			zap.Uint64("taskID", taskID),
			zap.Error(err))
	}
}

// OwnsSQLExecutor implements importglue.Glue
func (gs *tidbGlueSession) OwnsSQLExecutor() bool {
	return false
}

// GetSQLExecutor implements importglue.Glue
func (gs *tidbGlueSession) GetSQLExecutor() importglue.SQLExecutor {
	return gs
}

// GetParser implements importglue.Glue
func (gs *tidbGlueSession) GetParser() *parser.Parser {
	p := parser.New()
	p.SetSQLMode(gs.se.GetSessionVars().SQLMode)
	return p
}

// GetDB implements importglue.Glue
func (gs *tidbGlueSession) GetDB() (*sql.DB, error) {
	return nil, errors.New("tidbGlueSession can't perform GetDB")
}

// GetTables implements importglue.Glue
func (gs *tidbGlueSession) GetTables(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	is := domain.GetDomain(gs.se).InfoSchema()
	tables := is.SchemaTables(model.NewCIStr(schemaName))
	tbsInfo := make([]*model.TableInfo, len(tables))
	for i := range tbsInfo {
		tbsInfo[i] = tables[i].Meta()
	}
	return tbsInfo, nil
}

// GetSession implements importglue.Glue
func (gs *tidbGlueSession) GetSession() (checkpoints.Session, error) {
	s, err := CreateSessionForBRIEFunc(gs.se)
	if err != nil {
		return nil, errors.Annotate(err, "create new session in GlueCheckpointsDB")
	}
	return s, nil
}

// OpenCheckpointsDB implements importglue.Glue
func (gs *tidbGlueSession) OpenCheckpointsDB(ctx context.Context, c *importcfg.Config) (checkpoints.CheckpointsDB, error) {
	se, err := gs.GetSession()
	if err != nil {
		return nil, err
	}
	defer se.Close()
	return checkpoints.NewGlueCheckpointsDB(ctx, se, gs.GetSession, c.Checkpoint.Schema)
}
