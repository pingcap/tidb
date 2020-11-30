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
	"encoding/base64"
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
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	defaultImportID = "tidb_import_"
)

var (
	// CreateSessionForBRIEFunc is used to solve cycle import when use session.CreateSession
	CreateSessionForBRIEFunc func(sessionctx.Context) (checkpoints.Session, error)
)

// brieTaskProgress tracks a task's current progress.
type brieTaskProgress struct {
	// current progress of the task.
	// this field is atomically updated outside of the lock below.
	current int64

	// lock is the mutex protected the two fields below.
	lock sync.Mutex
	// cmd is the name of the step the BRIE task is currently performing.
	cmd string
	// total is the total progress of the task.
	// the percentage of completeness is `(100%) * current / total`.
	total int64
}

// Inc implements glue.Progress
func (p *brieTaskProgress) Inc() {
	atomic.AddInt64(&p.current, 1)
}

// Close implements glue.Progress
func (p *brieTaskProgress) Close() {
	p.lock.Lock()
	atomic.StoreInt64(&p.current, p.total)
	p.lock.Unlock()
}

// GetFraction returns a real number whose range is [0, 100]
func (p *brieTaskProgress) GetFraction() float64 {
	return float64(100*p.current) / float64(p.total)
}

type brieTaskInfo struct {
	queueTime types.Time
	kind      ast.BRIEKind
	storage   string
	connID    uint64
	originSQL string

	protected struct {
		sync.RWMutex
		execTime    types.Time
		finishTime  types.Time
		backupTS    uint64
		archiveSize uint64
		message     string
	}
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
// we should ensure returned *brieQueueItem is not nil
func (bq *brieQueue) registerTask(
	ctx context.Context,
	info *brieTaskInfo,
	s sqlexec.RestrictedSQLExecutor,
) (context.Context, uint64, *brieQueueItem, error) {
	taskCtx, taskCancel := context.WithCancel(ctx)
	item := &brieQueueItem{
		info:   info,
		cancel: taskCancel,
		progress: &brieTaskProgress{
			cmd:   "Wait",
			total: 1,
		},
	}

	// use base64 encode to avoid SQL injection
	sql := fmt.Sprintf(`INSERT INTO mysql.brie_tasks (
				kind, origin_sql, queue_time, data_path, conn_id, status, progress, cancel
			) VALUES ('%s', '%s', '%s', '%s', %d, '%s', %f, %d);`,
		info.kind.String(),
		base64.StdEncoding.EncodeToString([]byte(info.originSQL)),
		info.queueTime.String(),
		base64.StdEncoding.EncodeToString([]byte(info.storage)),
		info.connID,
		item.progress.cmd,
		item.progress.GetFraction(),
		0,
	)
	_, _, err := s.ExecRestrictedSQLWithContext(ctx, sql)
	if err != nil {
		return nil, 0, item, err
	}
	sql = fmt.Sprintf("SELECT MAX(id) FROM mysql.brie_tasks WHERE conn_id = %d;", info.connID)
	rows, _, err := s.ExecRestrictedSQLWithContext(ctx, sql)
	if err != nil {
		return nil, 0, item, err
	}
	if len(rows) != 1 {
		return nil, 0, item, fmt.Errorf("%s didn't return one row, err: %v", sql, err)
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
		b.err = errors.Annotate(err, "invalid destination URL")
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
					b.err = err
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
					b.err = err
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
		b.err = errors.Annotate(err, "invalid destination URL")
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
				b.err = errors.Errorf("please specify schema for %s in IMPORT", tbl.Name.O)
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
				b.err = errors.Errorf("CSV_HEADER only support FIELDS or COLUMNS to indicate it has header")
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
		b.err = errors.Errorf("%s requires tikv store, not %s", s.Kind, tidbCfg.Store)
		return nil
	}

	switch s.Kind {
	case ast.BRIEKindBackup, ast.BRIEKindRestore:
		return b.buildBackupRestore(s, schema)
	case ast.BRIEKindImport:
		return b.buildImport(s, schema)
	default:
		b.err = errors.Errorf("unsupported BRIE statement kind: %s", s.Kind)
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
		return err
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
				glue.Flush(taskCtx, taskID)
			case <-taskCtx.Done():
				return
			}
		}
	}()

	err = bq.acquireTask(taskCtx, taskID, sqlExecutor)
	if err != nil {
		return err
	}
	defer bq.releaseTask()

	e.info.protected.Lock()
	e.info.protected.execTime = types.CurrentTime(mysql.TypeDatetime)
	e.info.protected.Unlock()

	switch e.info.kind {
	case ast.BRIEKindBackup:
		err = handleBRIEError(brtask.RunBackup(taskCtx, glue, "Backup", e.backupCfg), ErrBRIEBackupFailed)
	case ast.BRIEKindRestore:
		err = handleBRIEError(brtask.RunRestore(taskCtx, glue, "Restore", e.restoreCfg), ErrBRIERestoreFailed)
	case ast.BRIEKindImport:
		l := lightning.New(e.importGlobalCfg)
		e.importTaskCfg.Checkpoint.Schema += strconv.FormatUint(taskID, 10)
		e.importTaskCfg.TikvImporter.SortedKVDir += strconv.FormatUint(taskID, 10)
		err2 := handleBRIEError(l.RunOnce(taskCtx, e.importTaskCfg, glue, logutil.Logger(ctx)), ErrBRIEImportFailed)
		if err2 != nil {
			e.info.protected.Lock()
			e.info.protected.message = err2.Error()
			e.info.protected.Unlock()
			item.progress.lock.Lock()
			item.progress.cmd = "Stop"
			item.progress.lock.Unlock()
		} else {
			item.progress.lock.Lock()
			item.progress.cmd = "Finish"
			item.progress.current = item.progress.total
			item.progress.lock.Unlock()
		}
	default:
		return errors.Errorf("unsupported BRIE statement kind: %s", e.info.kind)
	}
	e.info.protected.Lock()
	e.info.protected.finishTime = types.CurrentTime(mysql.TypeDatetime)
	e.info.protected.Unlock()
	glue.Flush(ctx, taskID)
	taskExecuted = true
	if err != nil {
		return err
	}

	switch e.info.kind {
	case ast.BRIEKindBackup, ast.BRIEKindRestore:
		req.AppendString(0, e.info.storage)
		req.AppendUint64(1, e.info.protected.archiveSize)
		req.AppendUint64(2, e.info.protected.backupTS)
		req.AppendTime(3, e.info.queueTime)
		req.AppendTime(4, e.info.protected.execTime)
	case ast.BRIEKindImport:
		req.AppendUint64(0, taskID)
		req.AppendString(1, item.progress.cmd)
		req.AppendString(2, e.info.storage)
		req.AppendUint64(3, e.info.protected.archiveSize)
		req.AppendTime(4, e.info.queueTime)
		req.AppendTime(5, e.info.protected.execTime)
		req.AppendTime(6, e.info.protected.finishTime)
		req.AppendString(7, e.info.protected.message)
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
	rs, err := e.ctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
	if err != nil {
		return err
	}
	r := rs[0]
	defer terror.Call(r.Close)
	req := r.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	for {
		err = r.Next(ctx, req)
		if err != nil {
			return err
		}
		if req.NumRows() == 0 {
			break
		}

		for row := it.Begin(); row != it.End(); row = it.Next() {
			e.result.AppendUint64(0, row.GetUint64(0))
			dataPath, err := base64.StdEncoding.DecodeString(row.GetString(1))
			if err != nil {
				logutil.Logger(ctx).Error("failed to decode base64", zap.Error(err))
				continue
			}
			e.result.AppendBytes(1, dataPath)
			e.result.AppendString(2, row.GetString(2))
			e.result.AppendFloat32(3, row.GetFloat32(3))
			e.result.AppendTime(4, row.GetTime(4))
			e.result.AppendTime(5, row.GetTime(5))
			e.result.AppendTime(6, row.GetTime(6))
			e.result.AppendUint64(7, row.GetUint64(7))
			msg, err := base64.StdEncoding.DecodeString(row.GetString(8))
			if err != nil {
				logutil.Logger(ctx).Error("failed to decode base64", zap.Error(err))
				continue
			}
			e.result.AppendBytes(8, msg)
		}
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

func (gs *tidbGlueSession) ExecuteWithLog(ctx context.Context, sql string, purpose string, logger importlog.Logger) error {
	se, err := gs.GetSession()
	if err != nil {
		return err
	}
	defer se.Close()
	return common.Retry(purpose, logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
}

func (gs *tidbGlueSession) ObtainStringWithLog(ctx context.Context, sql string, purpose string, logger importlog.Logger) (string, error) {
	se, err := gs.GetSession()
	if err != nil {
		return "", err
	}
	defer se.Close()
	var result string
	err = common.Retry(purpose, logger, func() error {
		rs, err := se.Execute(ctx, sql)
		if err != nil {
			return err
		}
		r := rs[0]
		defer terror.Call(r.Close)
		req := r.NewChunk()
		err = r.Next(ctx, req)
		if err != nil {
			return err
		}
		if req.NumRows() == 0 {
			return errors.Errorf("empty result while expecting a string, purpose: %s, sql: %s", purpose, sql)
		}
		row := req.GetRow(0)
		result = row.GetString(0)
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
	gs.progress.cmd = cmdName
	gs.progress.total = total
	atomic.StoreInt64(&gs.progress.current, 0)
	gs.progress.lock.Unlock()
	return gs.progress
}

// Record implements glue.Glue
func (gs *tidbGlueSession) Record(name string, value uint64) {
	gs.info.protected.Lock()
	defer gs.info.protected.Unlock()
	switch name {
	case "BackupTS":
		gs.info.protected.backupTS = value
	case "Size":
		gs.info.protected.archiveSize = value
	case importglue.RecordEstimatedChunk:
		gs.progress.lock.Lock()
		gs.progress.total = int64(value)
		gs.progress.lock.Unlock()
	case importglue.RecordFinishedChunk:
		gs.progress.lock.Lock()
		current := int64(value)
		if current < gs.progress.total {
			gs.progress.cmd = "Import"
		} else {
			current = gs.progress.total
			// TODO(lance6717): to be more exact, lightning could report status of restoring tables
			gs.progress.cmd = "Post-process"
		}
		gs.progress.current = current
		gs.progress.lock.Unlock()
	}
}

func (gs *tidbGlueSession) Flush(ctx context.Context, taskID uint64) {
	gs.info.protected.RLock()
	gs.progress.lock.Lock()
	sql := fmt.Sprintf(`UPDATE mysql.brie_tasks SET
					exec_time = '%s',
					finish_time = '%s',
					ts = %d,
					data_size = %d,
					status = '%s',
					progress = %f,
					message = '%s'
				WHERE id = %d`,
		gs.info.protected.execTime.String(),
		gs.info.protected.finishTime.String(),
		gs.info.protected.backupTS,
		gs.info.protected.archiveSize,
		gs.progress.cmd,
		gs.progress.GetFraction(),
		base64.StdEncoding.EncodeToString([]byte(gs.info.protected.message)),
		taskID)
	gs.progress.lock.Unlock()
	gs.info.protected.RUnlock()

	sqlExecutor := gs.se.(sqlexec.RestrictedSQLExecutor)
	_, _, err := sqlExecutor.ExecRestrictedSQLWithContext(ctx, sql)
	if err != nil {
		logutil.Logger(ctx).Error("failed to update BRIE task table",
			zap.Uint64("taskID", taskID),
			zap.Error(err))
	}
}

func (gs *tidbGlueSession) OwnsSQLExecutor() bool {
	return false
}

func (gs *tidbGlueSession) GetSQLExecutor() importglue.SQLExecutor {
	return gs
}

func (gs *tidbGlueSession) GetParser() *parser.Parser {
	p := parser.New()
	p.SetSQLMode(gs.se.GetSessionVars().SQLMode)
	return p
}

func (gs *tidbGlueSession) GetDB() (*sql.DB, error) {
	return nil, errors.New("tidbGlueSession can't perform GetDB")
}

func (gs *tidbGlueSession) GetTables(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	is := domain.GetDomain(gs.se).InfoSchema()
	tables := is.SchemaTables(model.NewCIStr(schemaName))
	tbsInfo := make([]*model.TableInfo, len(tables))
	for i := range tbsInfo {
		tbsInfo[i] = tables[i].Meta()
	}
	return tbsInfo, nil
}

func (gs *tidbGlueSession) GetSession() (checkpoints.Session, error) {
	s, err := CreateSessionForBRIEFunc(gs.se)
	if err != nil {
		return nil, errors.Annotate(err, "create new session in GlueCheckpointsDB")
	}
	return s, nil
}

func (gs *tidbGlueSession) OpenCheckpointsDB(ctx context.Context, c *importcfg.Config) (checkpoints.CheckpointsDB, error) {
	se, err := gs.GetSession()
	if err != nil {
		return nil, err
	}
	defer se.Close()
	return checkpoints.NewGlueCheckpointsDB(ctx, se, gs.GetSession, c.Checkpoint.Schema)
}
