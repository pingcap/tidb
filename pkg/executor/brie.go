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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/task/show"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/printer"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
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
	lock syncutil.Mutex
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

// IncBy implements glue.Progress
func (p *brieTaskProgress) IncBy(cnt int64) {
	atomic.AddInt64(&p.current, cnt)
}

// GetCurrent implements glue.Progress
func (p *brieTaskProgress) GetCurrent() int64 {
	return atomic.LoadInt64(&p.current)
}

// Close implements glue.Progress
func (p *brieTaskProgress) Close() {
	p.lock.Lock()
	current := atomic.LoadInt64(&p.current)
	if current < p.total {
		p.cmd = fmt.Sprintf("%s Canceled", p.cmd)
	}
	atomic.StoreInt64(&p.current, p.total)
	p.lock.Unlock()
}

type brieTaskInfo struct {
	id          uint64
	query       string
	queueTime   types.Time
	execTime    types.Time
	finishTime  types.Time
	kind        ast.BRIEKind
	storage     string
	connID      uint64
	backupTS    uint64
	restoreTS   uint64
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

// ResetGlobalBRIEQueueForTest resets the ID allocation for the global BRIE queue.
// In some of our test cases, we rely on the ID is allocated from 1.
// When batch executing test cases, the assumation may be broken and make the cases fail.
func ResetGlobalBRIEQueueForTest() {
	globalBRIEQueue = &brieQueue{
		workerCh: make(chan struct{}, 1),
	}
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
	info.id = taskID

	return taskCtx, taskID
}

// query task queries a task from the queue.
func (bq *brieQueue) queryTask(taskID uint64) (*brieTaskInfo, bool) {
	if item, ok := bq.tasks.Load(taskID); ok {
		return item.(*brieQueueItem).info, true
	}
	return nil, false
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

func (bq *brieQueue) cancelTask(taskID uint64) bool {
	item, ok := bq.tasks.Load(taskID)
	if !ok {
		return false
	}
	i := item.(*brieQueueItem)
	i.cancel()
	i.progress.Close()
	log.Info("BRIE job canceled.", zap.Uint64("ID", i.info.id))
	return true
}

func (bq *brieQueue) clearTask(sc *stmtctx.StatementContext) {
	if time.Since(bq.lastClearTime) < clearInterval {
		return
	}

	bq.lastClearTime = time.Now()
	currTime := types.CurrentTime(mysql.TypeDatetime)

	bq.tasks.Range(func(key, value any) bool {
		item := value.(*brieQueueItem)
		if d := currTime.Sub(sc.TypeCtx(), &item.info.finishTime); d.Compare(outdatedDuration) > 0 {
			bq.tasks.Delete(key)
		}
		return true
	})
}

func (b *executorBuilder) parseTSString(ts string) (uint64, error) {
	sc := stmtctx.NewStmtCtxWithTimeZone(b.ctx.GetSessionVars().Location())
	t, err := types.ParseTime(sc.TypeCtx(), ts, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return 0, err
	}
	t1, err := t.GoTime(sc.TimeZone())
	if err != nil {
		return 0, err
	}
	return oracle.GoTimeToTS(t1), nil
}

func (b *executorBuilder) buildBRIE(s *ast.BRIEStmt, schema *expression.Schema) exec.Executor {
	if s.Kind == ast.BRIEKindShowBackupMeta {
		return execOnce(&showMetaExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, schema, 0),
			showConfig:   buildShowMetadataConfigFrom(s),
		})
	}

	if s.Kind == ast.BRIEKindShowQuery {
		return execOnce(&showQueryExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, schema, 0),
			targetID:     uint64(s.JobID),
		})
	}

	if s.Kind == ast.BRIEKindCancelJob {
		return &cancelJobExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, schema, 0),
			targetID:     uint64(s.JobID),
		}
	}

	e := &BRIEExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, schema, 0),
		info: &brieTaskInfo{
			kind: s.Kind,
		},
	}

	tidbCfg := config.GetGlobalConfig()
	tlsCfg := task.TLSConfig{
		CA:   tidbCfg.Security.ClusterSSLCA,
		Cert: tidbCfg.Security.ClusterSSLCert,
		Key:  tidbCfg.Security.ClusterSSLKey,
	}
	pds := strings.Split(tidbCfg.Path, ",")
	cfg := task.DefaultConfig()
	cfg.PD = pds
	cfg.TLS = tlsCfg

	storageURL, err := storage.ParseRawURL(s.Storage)
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
			b.err = plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("hdfs storage")
			return nil
		}
	case "local", "file", "":
		if sem.IsEnabled() {
			// Storage is not permitted to be local when SEM is enabled.
			b.err = plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("local storage")
			return nil
		}
	default:
	}

	failpoint.Inject("modifyStore", func(v failpoint.Value) {
		tidbCfg.Store = v.(string)
	})
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
		case ast.BRIEOptionChecksumConcurrency:
			cfg.ChecksumConcurrency = uint(opt.UintValue)
		case ast.BRIEOptionEncryptionKeyFile:
			cfg.CipherInfo.CipherKey, err = task.GetCipherKeyContent("", opt.StrValue)
			if err != nil {
				b.err = err
				return nil
			}
		case ast.BRIEOptionEncryptionMethod:
			switch opt.StrValue {
			case "aes128-ctr":
				cfg.CipherInfo.CipherType = encryptionpb.EncryptionMethod_AES128_CTR
			case "aes192-ctr":
				cfg.CipherInfo.CipherType = encryptionpb.EncryptionMethod_AES192_CTR
			case "aes256-ctr":
				cfg.CipherInfo.CipherType = encryptionpb.EncryptionMethod_AES256_CTR
			case "plaintext":
				cfg.CipherInfo.CipherType = encryptionpb.EncryptionMethod_PLAINTEXT
			default:
				b.err = errors.Errorf("unsupported encryption method: %s", opt.StrValue)
				return nil
			}
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

	// table options are stored in original case, but comparison
	// is expected to be performed insensitive.
	cfg.TableFilter = filter.CaseInsensitive(cfg.TableFilter)

	// We cannot directly use the query string, or the secret may be print.
	// NOTE: the ownership of `s.Storage` is taken here.
	s.Storage = e.info.storage
	e.info.query = restoreQuery(s)

	switch s.Kind {
	case ast.BRIEKindBackup:
		bcfg := task.DefaultBackupConfig()
		bcfg.Config = cfg
		e.backupCfg = &bcfg

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
			case ast.BRIEOptionCompression:
				switch opt.StrValue {
				case "zstd":
					e.backupCfg.CompressionConfig.CompressionType = backuppb.CompressionType_ZSTD
				case "snappy":
					e.backupCfg.CompressionConfig.CompressionType = backuppb.CompressionType_SNAPPY
				case "lz4":
					e.backupCfg.CompressionConfig.CompressionType = backuppb.CompressionType_LZ4
				default:
					b.err = errors.Errorf("unsupported compression type: %s", opt.StrValue)
					return nil
				}
			case ast.BRIEOptionCompressionLevel:
				e.backupCfg.CompressionConfig.CompressionLevel = int32(opt.UintValue)
			case ast.BRIEOptionIgnoreStats:
				e.backupCfg.IgnoreStats = opt.UintValue != 0
			}
		}

	case ast.BRIEKindRestore:
		rcfg := task.DefaultRestoreConfig()
		rcfg.Config = cfg
		e.restoreCfg = &rcfg
		for _, opt := range s.Options {
			switch opt.Tp {
			case ast.BRIEOptionOnline:
				e.restoreCfg.Online = opt.UintValue != 0
			case ast.BRIEOptionWaitTiflashReady:
				e.restoreCfg.WaitTiflashReady = opt.UintValue != 0
			case ast.BRIEOptionWithSysTable:
				e.restoreCfg.WithSysTable = opt.UintValue != 0
			case ast.BRIEOptionLoadStats:
				e.restoreCfg.LoadStats = opt.UintValue != 0
			}
		}

	default:
		b.err = errors.Errorf("unsupported BRIE statement kind: %s", s.Kind)
		return nil
	}

	return e
}

// oneshotExecutor wraps a executor, making its `Next` would only be called once.
type oneshotExecutor struct {
	exec.Executor
	finished bool
}

func (o *oneshotExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if o.finished {
		req.Reset()
		return nil
	}

	if err := o.Executor.Next(ctx, req); err != nil {
		return err
	}
	o.finished = true
	return nil
}

func execOnce(ex exec.Executor) exec.Executor {
	return &oneshotExecutor{Executor: ex}
}

type showQueryExec struct {
	exec.BaseExecutor

	targetID uint64
}

func (s *showQueryExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()

	tsk, ok := globalBRIEQueue.queryTask(s.targetID)
	if !ok {
		return nil
	}

	req.AppendString(0, tsk.query)
	return nil
}

type cancelJobExec struct {
	exec.BaseExecutor

	targetID uint64
}

func (s cancelJobExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !globalBRIEQueue.cancelTask(s.targetID) {
		s.Ctx().GetSessionVars().StmtCtx.AppendWarning(exeerrors.ErrLoadDataJobNotFound.FastGenByArgs(s.targetID))
	}
	return nil
}

type showMetaExec struct {
	exec.BaseExecutor

	showConfig show.Config
}

// BRIEExec represents an executor for BRIE statements (BACKUP, RESTORE, etc)
type BRIEExec struct {
	exec.BaseExecutor

	backupCfg  *task.BackupConfig
	restoreCfg *task.RestoreConfig
	showConfig *show.Config
	info       *brieTaskInfo
}

func buildShowMetadataConfigFrom(s *ast.BRIEStmt) show.Config {
	if s.Kind != ast.BRIEKindShowBackupMeta {
		panic(fmt.Sprintf("precondition failed: `fillByShowMetadata` should always called by a ast.BRIEKindShowBackupMeta, but it is %s.", s.Kind))
	}

	store := s.Storage
	cfg := show.Config{
		Storage: store,
		Cipher: backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
	}
	return cfg
}

func (e *showMetaExec) Next(ctx context.Context, req *chunk.Chunk) error {
	exe, err := show.CreateExec(ctx, e.showConfig)
	if err != nil {
		return errors.Annotate(err, "failed to create show exec")
	}
	res, err := exe.Read(ctx)
	if err != nil {
		return errors.Annotate(err, "failed to read metadata from backupmeta")
	}

	startTime := oracle.GetTimeFromTS(uint64(res.StartVersion))
	endTime := oracle.GetTimeFromTS(uint64(res.EndVersion))

	for _, table := range res.Tables {
		req.AppendString(0, table.DBName)
		req.AppendString(1, table.TableName)
		req.AppendInt64(2, int64(table.KVCount))
		req.AppendInt64(3, int64(table.KVSize))
		if res.StartVersion > 0 {
			req.AppendTime(4, types.NewTime(types.FromGoTime(startTime.In(e.Ctx().GetSessionVars().Location())), mysql.TypeDatetime, 0))
		} else {
			req.AppendNull(4)
		}
		req.AppendTime(5, types.NewTime(types.FromGoTime(endTime.In(e.Ctx().GetSessionVars().Location())), mysql.TypeDatetime, 0))
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *BRIEExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.info == nil {
		return nil
	}

	bq := globalBRIEQueue
	bq.clearTask(e.Ctx().GetSessionVars().StmtCtx)

	e.info.connID = e.Ctx().GetSessionVars().ConnectionID
	e.info.queueTime = types.CurrentTime(mysql.TypeDatetime)
	taskCtx, taskID := bq.registerTask(ctx, e.info)
	defer bq.cancelTask(taskID)
	failpoint.Inject("block-on-brie", func() {
		log.Warn("You shall not pass, nya. :3")
		<-taskCtx.Done()
		if taskCtx.Err() != nil {
			failpoint.Return(taskCtx.Err())
		}
	})
	// manually monitor the Killed status...
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if e.Ctx().GetSessionVars().SQLKiller.HandleSignal() == exeerrors.ErrQueryInterrupted {
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
	glue := &tidbGlue{se: e.Ctx(), progress: progress, info: e.info}

	switch e.info.kind {
	case ast.BRIEKindBackup:
		err = handleBRIEError(task.RunBackup(taskCtx, glue, "Backup", e.backupCfg), exeerrors.ErrBRIEBackupFailed)
	case ast.BRIEKindRestore:
		err = handleBRIEError(task.RunRestore(taskCtx, glue, "Restore", e.restoreCfg), exeerrors.ErrBRIERestoreFailed)
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
	switch e.info.kind {
	case ast.BRIEKindBackup:
		req.AppendUint64(2, e.info.backupTS)
		req.AppendTime(3, e.info.queueTime)
		req.AppendTime(4, e.info.execTime)
	case ast.BRIEKindRestore:
		req.AppendUint64(2, e.info.backupTS)
		req.AppendUint64(3, e.info.restoreTS)
		req.AppendTime(4, e.info.queueTime)
		req.AppendTime(5, e.info.execTime)
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

func (e *ShowExec) fetchShowBRIE(kind ast.BRIEKind) error {
	globalBRIEQueue.tasks.Range(func(_, value any) bool {
		item := value.(*brieQueueItem)
		if item.info.kind == kind {
			item.progress.lock.Lock()
			defer item.progress.lock.Unlock()
			current := atomic.LoadInt64(&item.progress.current)
			e.result.AppendUint64(0, item.info.id)
			e.result.AppendString(1, item.info.storage)
			e.result.AppendString(2, item.progress.cmd)
			e.result.AppendFloat64(3, 100.0*float64(current)/float64(item.progress.total))
			e.result.AppendTime(4, item.info.queueTime)
			e.result.AppendTime(5, item.info.execTime)
			e.result.AppendTime(6, item.info.finishTime)
			e.result.AppendUint64(7, item.info.connID)
			if len(item.info.message) > 0 {
				e.result.AppendString(8, item.info.message)
			} else {
				e.result.AppendNull(8)
			}
		}
		return true
	})
	globalBRIEQueue.clearTask(e.Ctx().GetSessionVars().StmtCtx)
	return nil
}

type tidbGlue struct {
	// the session context of the brie task
	se       sessionctx.Context
	progress *brieTaskProgress
	info     *brieTaskInfo
}

// GetDomain implements glue.Glue
func (gs *tidbGlue) GetDomain(_ kv.Storage) (*domain.Domain, error) {
	return domain.GetDomain(gs.se), nil
}

// CreateSession implements glue.Glue
func (gs *tidbGlue) CreateSession(_ kv.Storage) (glue.Session, error) {
	newSCtx, err := CreateSession(gs.se)
	if err != nil {
		return nil, err
	}
	return &tidbGlueSession{se: newSCtx}, nil
}

// Open implements glue.Glue
func (gs *tidbGlue) Open(string, pd.SecurityOption) (kv.Storage, error) {
	return gs.se.GetStore(), nil
}

// OwnsStorage implements glue.Glue
func (*tidbGlue) OwnsStorage() bool {
	return false
}

// StartProgress implements glue.Glue
func (gs *tidbGlue) StartProgress(_ context.Context, cmdName string, total int64, _ bool) glue.Progress {
	gs.progress.lock.Lock()
	gs.progress.cmd = cmdName
	gs.progress.total = total
	atomic.StoreInt64(&gs.progress.current, 0)
	gs.progress.lock.Unlock()
	return gs.progress
}

// Record implements glue.Glue
func (gs *tidbGlue) Record(name string, value uint64) {
	switch name {
	case "BackupTS":
		gs.info.backupTS = value
	case "RestoreTS":
		gs.info.restoreTS = value
	case "Size":
		gs.info.archiveSize = value
	}
}

func (*tidbGlue) GetVersion() string {
	return "TiDB\n" + printer.GetTiDBInfo()
}

// UseOneShotSession implements glue.Glue
func (gs *tidbGlue) UseOneShotSession(_ kv.Storage, _ bool, fn func(se glue.Session) error) error {
	// In SQL backup, we don't need to close domain,
	// but need to create an new session.
	newSCtx, err := CreateSession(gs.se)
	if err != nil {
		return err
	}
	glueSession := &tidbGlueSession{se: newSCtx}
	defer func() {
		CloseSession(newSCtx)
		log.Info("one shot session from brie closed")
	}()
	return fn(glueSession)
}

type tidbGlueSession struct {
	// the session context of the brie task's subtask, such as `CREATE TABLE`.
	se sessionctx.Context
}

// Execute implements glue.Session
// These queries execute without privilege checking, since the calling statements
// such as BACKUP and RESTORE have already been privilege checked.
// NOTE: Maybe drain the restult too? See `gluetidb.tidbSession.ExecuteInternal` for more details.
func (gs *tidbGlueSession) Execute(ctx context.Context, sql string) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	_, _, err := gs.se.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql)
	return err
}

func (gs *tidbGlueSession) ExecuteInternal(ctx context.Context, sql string, args ...any) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	exec := gs.se.GetSQLExecutor()
	_, err := exec.ExecuteInternal(ctx, sql, args...)
	return err
}

// CreateDatabase implements glue.Session
func (gs *tidbGlueSession) CreateDatabase(_ context.Context, schema *model.DBInfo) error {
	return BRIECreateDatabase(gs.se, schema, "")
}

// CreateTable implements glue.Session
func (gs *tidbGlueSession) CreateTable(_ context.Context, dbName model.CIStr, table *model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	return BRIECreateTable(gs.se, dbName, table, "", cs...)
}

// CreateTables implements glue.BatchCreateTableSession.
func (gs *tidbGlueSession) CreateTables(_ context.Context,
	tables map[string][]*model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	return BRIECreateTables(gs.se, tables, "", cs...)
}

// CreatePlacementPolicy implements glue.Session
func (gs *tidbGlueSession) CreatePlacementPolicy(_ context.Context, policy *model.PolicyInfo) error {
	originQueryString := gs.se.Value(sessionctx.QueryString)
	defer gs.se.SetValue(sessionctx.QueryString, originQueryString)
	gs.se.SetValue(sessionctx.QueryString, ConstructResultOfShowCreatePlacementPolicy(policy))
	d := domain.GetDomain(gs.se).DDL()
	// the default behaviour is ignoring duplicated policy during restore.
	return d.CreatePlacementPolicyWithInfo(gs.se, policy, ddl.OnExistIgnore)
}

// Close implements glue.Session
func (gs *tidbGlueSession) Close() {
	CloseSession(gs.se)
}

// GetGlobalVariables implements glue.Session.
func (gs *tidbGlueSession) GetGlobalVariable(name string) (string, error) {
	return gs.se.GetSessionVars().GlobalVarsAccessor.GetTiDBTableValue(name)
}

// GetSessionCtx implements glue.Glue
func (gs *tidbGlueSession) GetSessionCtx() sessionctx.Context {
	return gs.se
}

func restoreQuery(stmt *ast.BRIEStmt) string {
	out := bytes.NewBuffer(nil)
	rc := format.NewRestoreCtx(format.RestoreNameBackQuotes|format.RestoreStringSingleQuotes, out)
	if err := stmt.Restore(rc); err != nil {
		return "N/A"
	}
	return out.String()
}
