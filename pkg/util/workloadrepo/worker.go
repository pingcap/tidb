// Copyright 2024 PingCAP, Inc.
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

package workloadrepo

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	snapshotTable int = iota
	samplingTable
	metadataTable
)

type repositoryTable struct {
	schema     string
	table      string
	tableType  int
	destTable  string
	where      string
	createStmt string
	insertStmt string
}

var (
	repositoryDest             = "tidb_workload_repository_dest"
	repositoryRetentionDays    = "tidb_workload_repository_retention_days"
	repositorySamplingInterval = "tidb_workload_repository_active_sampling_interval"
	repositorySnapshotInterval = "tidb_workload_repository_snapshot_interval"
)

var workloadTables = []repositoryTable{
	{
		"",
		"",
		metadataTable,
		histSnapshotsTable,
		"",
		sqlescape.MustEscapeSQL(`CREATE TABLE IF NOT EXISTS %n.%n (
			SNAP_ID int unsigned NOT NULL COMMENT 'Global unique identifier of the snapshot',
			BEGIN_TIME DATETIME NOT NULL COMMENT 'Datetime that TiDB begins taking this snapshot.',
			END_TIME DATETIME NULL COMMENT 'Datetime that TiDB finish taking this snapshot.',
			DB_VER JSON NULL COMMENT 'Versions of TiDB, TiKV, PD at the moment',
			WR_VER int unsigned NULL COMMENT 'Version to identify the compatibility of workload schema between releases.',
			SOURCE VARCHAR(20) NULL COMMENT 'The program that initializes the snaphost. ',
			ERROR TEXT DEFAULT NULL COMMENT 'extra messages are written if anything happens to block that snapshots.')`, mysql.WorkloadSchema, histSnapshotsTable),
		"",
	},
	{"INFORMATION_SCHEMA", "TIDB_INDEX_USAGE", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "TIDB_STATEMENTS_STATS", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "CLIENT_ERRORS_SUMMARY_BY_HOST", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "CLIENT_ERRORS_SUMMARY_BY_USER", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "CLIENT_ERRORS_SUMMARY_GLOBAL", snapshotTable, "", "", "", ""},

	{"INFORMATION_SCHEMA", "PROCESSLIST", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "DATA_LOCK_WAITS", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "TIDB_TRX", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "MEMORY_USAGE", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "DEADLOCKS", samplingTable, "", "", "", ""},

	// TODO: These tables are excluded for now, because reading from them adds unnecessary load to the PD.
	//{"INFORMATION_SCHEMA", "TIKV_REGION_STATUS", snapshotTable, "", "", "", ""},
	//{"INFORMATION_SCHEMA", "CLUSTER_LOAD", samplingTable, "", "", "", ""},
	//{"INFORMATION_SCHEMA", "TIDB_HOT_REGIONS", samplingTable, "", "", "", ""},
	//{"INFORMATION_SCHEMA", "TIDB_HOT_REGIONS_HISTORY", samplingTable, "", "", "", ""},
	//{"INFORMATION_SCHEMA", "TIKV_STORE_STATUS", samplingTable, "", "", "", ""},
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(resource pools.Resource)
}

// worker is the main struct for workload repository.
type worker struct {
	sync.Mutex
	etcdClient     *clientv3.Client
	sesspool       sessionPool
	cancel         context.CancelFunc
	newOwner       func(string, string) owner.Manager
	owner          owner.Manager
	wg             *util.WaitGroupEnhancedWrapper
	enabled        bool
	instanceID     string
	workloadTables []repositoryTable

	samplingInterval int32
	samplingTicker   *time.Ticker
	snapshotInterval int32
	snapshotTicker   *time.Ticker
	retentionDays    int32
}

var workerCtx = worker{}

func takeSnapshot(ctx context.Context) error {
	workerCtx.Lock()
	defer workerCtx.Unlock()

	if !workerCtx.enabled {
		return errWorkloadNotStarted.GenWithStackByArgs()
	}

	snapID, err := workerCtx.takeSnapshot(ctx)
	if err != nil {
		logutil.BgLogger().Info("workload repository manual snapshot failed", zap.String("owner", workerCtx.instanceID), zap.NamedError("err", err))
		return errCouldNotStartSnapshot.GenWithStackByArgs()
	}

	logutil.BgLogger().Info("workload repository ran manual snapshot", zap.String("owner", workerCtx.instanceID), zap.Uint64("snapID", snapID))
	return nil
}

func init() {
	executor.TakeSnapshot = takeSnapshot

	variable.RegisterSysVar(&variable.SysVar{
		Scope: vardef.ScopeGlobal,
		Name:  repositoryDest,
		Type:  vardef.TypeStr,
		Value: "",
		SetGlobal: func(ctx context.Context, _ *variable.SessionVars, val string) error {
			return workerCtx.setRepositoryDest(ctx, val)
		},
		Validation: func(_ *variable.SessionVars, norm, _ string, _ vardef.ScopeFlag) (string, error) {
			return validateDest(norm)
		},
	})
	variable.RegisterSysVar(&variable.SysVar{
		Scope:    vardef.ScopeGlobal,
		Name:     repositoryRetentionDays,
		Type:     vardef.TypeInt,
		Value:    strconv.Itoa(defRententionDays),
		MinValue: 0,
		MaxValue: 365,
		SetGlobal: func(ctx context.Context, _ *variable.SessionVars, val string) error {
			return workerCtx.setRetentionDays(ctx, val)
		},
	})
	variable.RegisterSysVar(&variable.SysVar{
		Scope:    vardef.ScopeGlobal,
		Name:     repositorySamplingInterval,
		Type:     vardef.TypeInt,
		Value:    strconv.Itoa(defSamplingInterval),
		MinValue: 0,
		MaxValue: 600,
		SetGlobal: func(ctx context.Context, _ *variable.SessionVars, val string) error {
			return workerCtx.changeSamplingInterval(ctx, val)
		},
	})
	variable.RegisterSysVar(&variable.SysVar{
		Scope:    vardef.ScopeGlobal,
		Name:     repositorySnapshotInterval,
		Type:     vardef.TypeInt,
		Value:    strconv.Itoa(defSnapshotInterval),
		MinValue: 900,
		MaxValue: 7200,
		SetGlobal: func(ctx context.Context, _ *variable.SessionVars, val string) error {
			return workerCtx.changeSnapshotInterval(ctx, val)
		},
	})
}

func initializeWorker(w *worker, etcdCli *clientv3.Client, newOwner func(string, string) owner.Manager, sesspool sessionPool, workloadTables []repositoryTable) {
	w.etcdClient = etcdCli
	w.sesspool = sesspool
	w.newOwner = newOwner
	w.workloadTables = workloadTables
	w.samplingInterval = defSamplingInterval
	w.snapshotInterval = defSnapshotInterval
	w.retentionDays = defRententionDays

	w.snapshotTicker = time.NewTicker(time.Second)
	w.snapshotTicker.Stop()
	w.samplingTicker = time.NewTicker(time.Second)
	w.samplingTicker.Stop()

	w.wg = util.NewWaitGroupEnhancedWrapper("workloadrepo", nil, false)
}

// SetupRepository finishes the initialization of the workload repository.
func SetupRepository(dom *domain.Domain) {
	workerCtx.Lock()
	defer workerCtx.Unlock()

	initializeWorker(&workerCtx, dom.GetEtcdClient(), dom.NewOwnerManager, dom.SysSessionPool(), workloadTables)

	if workerCtx.enabled {
		if err := workerCtx.start(); err != nil {
			logutil.BgLogger().Info("workload repository could not be started", zap.Any("err", err))
			workerCtx.enabled = false
		}
	}
}

// StopRepository stops any the go routines for the workload repository.
func StopRepository() {
	workerCtx.Lock()
	defer workerCtx.Unlock()

	workerCtx.stop()
	// prevent the workload repository from being restarted
	workerCtx.sesspool = nil
}

func runQuery(ctx context.Context, sctx sessionctx.Context, sql string, args ...any) (v []chunk.Row, e error) {
	defer func() {
		logutil.BgLogger().Debug("workload repository execute SQL", zap.String("sql", sql), zap.NamedError("err", e))
	}()
	exec := sctx.(sqlexec.SQLExecutor)
	res, err := exec.ExecuteInternal(ctx, sql, args...)
	if err == nil {
		if res != nil {
			rs, err := sqlexec.DrainRecordSet(ctx, res, 256)
			err = errors.Join(err, res.Close())
			return rs, err
		}
		return nil, nil
	}
	return nil, err
}

func execRetry(ctx context.Context, sctx sessionctx.Context, sql string, args ...any) ([]chunk.Row, error) {
	var errs [5]error
	for i := range errs {
		res, err := runQuery(ctx, sctx, sql, args...)
		if err == nil {
			return res, nil
		}
		errs[i] = err
	}
	return nil, errors.Join(errs[:]...)
}

func (w *worker) getSessionWithRetry() pools.Resource {
	for {
		_sessctx, err := w.sesspool.Get()
		if err != nil {
			logutil.BgLogger().Warn("workload repository cannot init session")
			time.Sleep(time.Second)
			continue
		}
		return _sessctx
	}
}

func (w *worker) readInstanceID() error {
	if w.instanceID != "" {
		return nil
	}

	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}

	w.instanceID = serverInfo.ID
	return nil
}

func (w *worker) fillInTableNames() {
	for rtIdx := range w.workloadTables {
		rt := &w.workloadTables[rtIdx]
		if rt.table != "" {
			if rt.destTable == "" {
				rt.destTable = "HIST_" + rt.table
			}
		}
	}
}

func (w *worker) startRepository(ctx context.Context) func() {
	// TODO: add another txn type
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	return func() {
		if err := w.owner.CampaignOwner(); err != nil {
			logutil.BgLogger().Error("repository could not campaign for owner", zap.NamedError("err", err))
		}
		ticker := time.NewTicker(time.Second)

		w.fillInTableNames()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if w.owner.IsOwner() {
					logutil.BgLogger().Info("repository has owner!")
					if err := w.createAllTables(ctx, time.Now()); err != nil {
						logutil.BgLogger().Error("workload repository cannot create tables", zap.NamedError("err", err))
					}
				}

				if !w.checkTablesExists(ctx, time.Now()) {
					continue
				}

				if err := w.readInstanceID(); err != nil {
					// if this fails try it again
					logutil.BgLogger().Info("workload repository could not get instance ID", zap.NamedError("err", err))
					continue
				}

				w.wg.RunWithRecover(w.startSample(ctx), func(err any) {
					logutil.BgLogger().Info("workload repository sample panic", zap.Any("err", err), zap.Stack("stack"))
				}, "sample")
				w.wg.RunWithRecover(w.startSnapshot(ctx), func(err any) {
					logutil.BgLogger().Info("workload repository snapshot panic", zap.Any("err", err), zap.Stack("stack"))
				}, "snapshot")
				w.wg.RunWithRecover(w.startHouseKeeper(ctx), func(err any) {
					logutil.BgLogger().Info("workload repository housekeeper panic", zap.Any("err", err), zap.Stack("stack"))
				}, "housekeeper")

				return
			}
		}
	}
}

func (w *worker) start() error {
	if w.cancel != nil {
		// prevent enable twice
		return nil
	}

	w.enabled = true
	if w.sesspool == nil {
		// setup isn't finished, just set enabled and return
		return nil
	}

	if w.etcdClient == nil {
		return errUnsupportedEtcdRequired.GenWithStackByArgs()
	}

	w.owner = w.newOwner(ownerKey, promptKey)
	_ = stmtsummary.StmtSummaryByDigestMap.SetHistoryEnabled(false)
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.RunWithRecover(w.startRepository(ctx), func(err any) {
		logutil.BgLogger().Info("workload repository prestart panic", zap.Any("err", err), zap.Stack("stack"))
	}, "prestart")
	return nil
}

// stop will stop the worker.
func (w *worker) stop() {
	w.enabled = false

	if w.cancel == nil {
		// Worker was not started, just clear enabled and return
		return
	}

	w.cancel()
	w.wg.Wait()
	_ = stmtsummary.StmtSummaryByDigestMap.SetHistoryEnabled(true)

	if w.owner != nil {
		w.owner.Close()
		w.owner = nil
	}

	w.cancel = nil
}

// setRepositoryDest will change the dest of workload snapshot.
func (w *worker) setRepositoryDest(_ context.Context, dst string) error {
	w.Lock()
	defer w.Unlock()

	switch dst {
	case "table":
		return w.start()
	default:
		w.stop()
		return nil
	}
}
