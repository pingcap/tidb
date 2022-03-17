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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"context"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	pd "github.com/tikv/pd/client"

	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
)

const clearInterval = 10 * time.Minute

var outdatedDuration = types.Duration{
	Duration: 30 * time.Minute,
	Fsp:      types.DefaultFsp,
}

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

type brieTaskInfo struct {
	queueTime   types.Time
	execTime    types.Time
	finishTime  types.Time
	kind        ast.BRIEKind
	storage     string
	connID      uint64
	backupTS    uint64
	archiveSize uint64
	message     string
}

type brieQueueItem struct {
	info     *brieTaskInfo
	progress *brieTaskProgress
	cancel   func()
}

type brieQueue struct {
	nextID uint64
	tasks  sync.Map

	lastClearTime time.Time

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
) (context.Context, uint64) {
	taskCtx, taskCancel := context.WithCancel(ctx)
	item := &brieQueueItem{
		info:   info,
		cancel: taskCancel,
		progress: &brieTaskProgress{
			cmd:   "Wait",
			total: 1,
		},
	}

	taskID := atomic.AddUint64(&bq.nextID, 1)
	bq.tasks.Store(taskID, item)

	return taskCtx, taskID
}

// acquireTask prepares to execute a BRIE task. Only one BRIE task can be
// executed at a time, and this function blocks until the task is ready.
//
// Returns an object to track the task's progress.
func (bq *brieQueue) acquireTask(taskCtx context.Context, taskID uint64) (*brieTaskProgress, error) {
	// wait until we are at the front of the queue.
	select {
	case bq.workerCh <- struct{}{}:
		if item, ok := bq.tasks.Load(taskID); ok {
			return item.(*brieQueueItem).progress, nil
		}
		// cannot find task, perhaps it has been canceled. allow the next task to run.
		bq.releaseTask()
		return nil, errors.Errorf("backup/restore task %d is canceled", taskID)
	case <-taskCtx.Done():
		return nil, taskCtx.Err()
	}
}

func (bq *brieQueue) releaseTask() {
	<-bq.workerCh
}

func (bq *brieQueue) cancelTask(taskID uint64) {
	item, ok := bq.tasks.Load(taskID)
	if !ok {
		return
	}
	item.(*brieQueueItem).cancel()
}

func (bq *brieQueue) clearTask(sc *stmtctx.StatementContext) {
	if time.Since(bq.lastClearTime) < clearInterval {
		return
	}

	bq.lastClearTime = time.Now()
	currTime := types.CurrentTime(mysql.TypeDatetime)

	bq.tasks.Range(func(key, value interface{}) bool {
		item := value.(*brieQueueItem)
		if d := currTime.Sub(sc, &item.info.finishTime); d.Compare(outdatedDuration) > 0 {
			bq.tasks.Delete(key)
		}
		return true
	})
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
	return oracle.GoTimeToTS(t1), nil
}

func (b *executorBuilder) buildBRIE(s *ast.BRIEStmt, schema *expression.Schema) Executor {
	e := &BRIEExec{
		baseExecutor: newBaseExecutor(b.ctx, schema, 0),
		info: &brieTaskInfo{
			kind: s.Kind,
		},
	}

	tidbCfg := config.GetGlobalConfig()
	cfg := task.Config{
		TLS: task.TLSConfig{
			CA:   tidbCfg.Security.ClusterSSLCA,
			Cert: tidbCfg.Security.ClusterSSLCert,
			Key:  tidbCfg.Security.ClusterSSLKey,
		},
		PD:          strings.Split(tidbCfg.Path, ","),
		Concurrency: 4,
		Checksum:    true,
		SendCreds:   true,
		LogProgress: true,
		CipherInfo: backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
	}

	storageURL, err := url.Parse(s.Storage)
	if err != nil {
		b.err = errors.Annotate(err, "invalid destination URL")
		return nil
	}

	switch storageURL.Scheme {
	case "s3":
		storage.ExtractQueryParameters(storageURL, &cfg.S3)
	case "gs", "gcs":
		storage.ExtractQueryParameters(storageURL, &cfg.GCS)
	case "hdfs":
		if sem.IsEnabled() {
			// Storage is not permitted to be hdfs when SEM is enabled.
			b.err = ErrNotSupportedWithSem.GenWithStackByArgs("hdfs storage")
			return nil
		}
	case "local", "file", "":
		if sem.IsEnabled() {
			// Storage is not permitted to be local when SEM is enabled.
			b.err = ErrNotSupportedWithSem.GenWithStackByArgs("local storage")
			return nil
		}
	default:
		break
	}

	if tidbCfg.Store != "tikv" {
		b.err = errors.Errorf("%s requires tikv store, not %s", s.Kind, tidbCfg.Store)
		return nil
	}

	cfg.Storage = storageURL.String()
	e.info.storage = cfg.Storage

	for _, opt := range s.Options {
		switch opt.Tp {
		case ast.BRIEOptionRateLimit:
			cfg.RateLimit = opt.UintValue
		case ast.BRIEOptionConcurrency:
			cfg.Concurrency = uint32(opt.UintValue)
		case ast.BRIEOptionChecksum:
			cfg.Checksum = opt.UintValue != 0
		case ast.BRIEOptionSendCreds:
			cfg.SendCreds = opt.UintValue != 0
		}
	}

	switch {
	case len(s.Tables) != 0:
		tables := make([]filter.Table, 0, len(s.Tables))
		for _, tbl := range s.Tables {
			tables = append(tables, filter.Table{Name: tbl.Name.O, Schema: tbl.Schema.O})
		}
		cfg.TableFilter = filter.NewTablesFilter(tables...)
	case len(s.Schemas) != 0:
		cfg.TableFilter = filter.NewSchemasFilter(s.Schemas...)
	default:
		cfg.TableFilter = filter.All()
	}

	if tidbCfg.LowerCaseTableNames != 0 {
		cfg.TableFilter = filter.CaseInsensitive(cfg.TableFilter)
	}

	switch s.Kind {
	case ast.BRIEKindBackup:
		e.backupCfg = &task.BackupConfig{Config: cfg}

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
		e.restoreCfg = &task.RestoreConfig{Config: cfg}
		for _, opt := range s.Options {
			switch opt.Tp {
			case ast.BRIEOptionOnline:
				e.restoreCfg.Online = opt.UintValue != 0
			}
		}

	default:
		b.err = errors.Errorf("unsupported BRIE statement kind: %s", s.Kind)
		return nil
	}

	return e
}

// BRIEExec represents an executor for BRIE statements (BACKUP, RESTORE, etc)
type BRIEExec struct {
	baseExecutor

	backupCfg  *task.BackupConfig
	restoreCfg *task.RestoreConfig
	info       *brieTaskInfo
}

// Next implements the Executor Next interface.
func (e *BRIEExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.info == nil {
		return nil
	}

	bq := globalBRIEQueue
	bq.clearTask(e.ctx.GetSessionVars().StmtCtx)

	e.info.connID = e.ctx.GetSessionVars().ConnectionID
	e.info.queueTime = types.CurrentTime(mysql.TypeDatetime)
	taskCtx, taskID := bq.registerTask(ctx, e.info)
	defer bq.cancelTask(taskID)

	// manually monitor the Killed status...
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if atomic.LoadUint32(&e.ctx.GetSessionVars().Killed) == 1 {
					bq.cancelTask(taskID)
					return
				}
			case <-taskCtx.Done():
				return
			}
		}
	}()

	progress, err := bq.acquireTask(taskCtx, taskID)
	if err != nil {
		return err
	}
	defer bq.releaseTask()

	e.info.execTime = types.CurrentTime(mysql.TypeDatetime)
	glue := &tidbGlueSession{se: e.ctx, progress: progress, info: e.info}

	switch e.info.kind {
	case ast.BRIEKindBackup:
		err = handleBRIEError(task.RunBackup(taskCtx, glue, "Backup", e.backupCfg), ErrBRIEBackupFailed)
	case ast.BRIEKindRestore:
		err = handleBRIEError(task.RunRestore(taskCtx, glue, "Restore", e.restoreCfg), ErrBRIERestoreFailed)
	default:
		err = errors.Errorf("unsupported BRIE statement kind: %s", e.info.kind)
	}
	e.info.finishTime = types.CurrentTime(mysql.TypeDatetime)
	if err != nil {
		e.info.message = err.Error()
		return err
	}
	e.info.message = ""

	req.AppendString(0, e.info.storage)
	req.AppendUint64(1, e.info.archiveSize)
	req.AppendUint64(2, e.info.backupTS)
	req.AppendTime(3, e.info.queueTime)
	req.AppendTime(4, e.info.execTime)
	e.info = nil
	return nil
}

func handleBRIEError(err error, terror *terror.Error) error {
	if err == nil {
		return nil
	}
	return terror.GenWithStackByArgs(err)
}

func (e *ShowExec) fetchShowBRIE(kind ast.BRIEKind) error {
	globalBRIEQueue.tasks.Range(func(key, value interface{}) bool {
		item := value.(*brieQueueItem)
		if item.info.kind == kind {
			item.progress.lock.Lock()
			defer item.progress.lock.Unlock()
			current := atomic.LoadInt64(&item.progress.current)
			e.result.AppendString(0, item.info.storage)
			e.result.AppendString(1, item.progress.cmd)
			e.result.AppendFloat64(2, 100.0*float64(current)/float64(item.progress.total))
			e.result.AppendTime(3, item.info.queueTime)
			e.result.AppendTime(4, item.info.execTime)
			e.result.AppendTime(5, item.info.finishTime)
			e.result.AppendUint64(6, item.info.connID)
			if len(item.info.message) > 0 {
				e.result.AppendString(7, item.info.message)
			} else {
				e.result.AppendNull(7)
			}
		}
		return true
	})
	globalBRIEQueue.clearTask(e.ctx.GetSessionVars().StmtCtx)
	return nil
}

type tidbGlueSession struct {
	se       sessionctx.Context
	progress *brieTaskProgress
	info     *brieTaskInfo
}

// BootstrapSession implements glue.Glue
func (gs *tidbGlueSession) GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domain.GetDomain(gs.se), nil
}

// CreateSession implements glue.Glue
func (gs *tidbGlueSession) CreateSession(store kv.Storage) (glue.Session, error) {
	return gs, nil
}

// Execute implements glue.Session
// These queries execute without privilege checking, since the calling statements
// such as BACKUP and RESTORE have already been privilege checked.
func (gs *tidbGlueSession) Execute(ctx context.Context, sql string) error {
	_, _, err := gs.se.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, nil, sql)
	return err
}

func (gs *tidbGlueSession) ExecuteInternal(ctx context.Context, sql string, args ...interface{}) error {
	exec := gs.se.(sqlexec.SQLExecutor)
	_, err := exec.ExecuteInternal(ctx, sql, args...)
	return err
}

// CreateDatabase implements glue.Session
func (gs *tidbGlueSession) CreateDatabase(ctx context.Context, schema *model.DBInfo) error {
	d := domain.GetDomain(gs.se).DDL()
	// 512 is defaultCapOfCreateTable.
	result := bytes.NewBuffer(make([]byte, 0, 512))
	if err := ConstructResultOfShowCreateDatabase(gs.se, schema, true, result); err != nil {
		return err
	}
	gs.se.SetValue(sessionctx.QueryString, result.String())
	schema = schema.Clone()
	if len(schema.Charset) == 0 {
		schema.Charset = mysql.DefaultCharset
	}
	return d.CreateSchemaWithInfo(gs.se, schema, ddl.OnExistIgnore)
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

	return d.CreateTableWithInfo(gs.se, dbName, table, ddl.OnExistIgnore)
}

// CreatePlacementPolicy implements glue.Session
func (gs *tidbGlueSession) CreatePlacementPolicy(ctx context.Context, policy *model.PolicyInfo) error {
	d := domain.GetDomain(gs.se).DDL()
	// the default behaviour is ignoring duplicated policy during restore.
	return d.CreatePlacementPolicyWithInfo(gs.se, policy, ddl.OnExistIgnore)
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
	switch name {
	case "BackupTS":
		gs.info.backupTS = value
	case "Size":
		gs.info.archiveSize = value
	}
}

func (gs *tidbGlueSession) GetVersion() string {
	return "TiDB\n" + printer.GetTiDBInfo()
}
