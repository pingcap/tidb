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

package repository

import (
	"context"
	"errors"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
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
			WR_VER int unsigned NULL COMMENT 'Version to identifiy the compatibility of workload schema between releases.',
			SOURCE VARCHAR(20) NULL COMMENT 'The program that initializes the snaphost. ',
			ERROR TEXT DEFAULT NULL COMMENT 'extra messages are written if anything happens to block that snapshots.')`, WorkloadSchema, histSnapshotsTable),
		"",
	},
	//{"INFORMATION_SCHEMA", "TIDB_INDEX_USAGE", snapshotTable, "", "", "", ""},
	//{"INFORMATION_SCHEMA", "TIDB_STATEMENTS_STATS", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "CLIENT_ERRORS_SUMMARY_BY_HOST", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "CLIENT_ERRORS_SUMMARY_BY_USER", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "CLIENT_ERRORS_SUMMARY_GLOBAL", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "TIKV_REGION_STATUS", snapshotTable, "", "", "", ""},

	{"INFORMATION_SCHEMA", "PROCESSLIST", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "DATA_LOCK_WAITS", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "TIDB_TRX", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "MEMORY_USAGE", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "DEADLOCKS", samplingTable, "", "", "", ""},

	{"INFORMATION_SCHEMA", "CLUSTER_LOAD", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "TIDB_HOT_REGIONS", samplingTable, "", "", "", ""},
	//{"INFORMATION_SCHEMA", "TIDB_HOT_REGIONS_HISTORY", samplingTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "TIKV_STORE_STATUS", samplingTable, "", "", "", ""},
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(resource pools.Resource)
}

// Worker is the main struct for repository.
type Worker struct {
	etcdClient   *clientv3.Client
	exit         chan struct{}
	sesspool     sessionPool
	cancel       context.CancelFunc
	getGlobalVar func(string) (string, error)
	newOwner     func(string, string) owner.Manager
	owner        owner.Manager
	wg           *util.WaitGroupEnhancedWrapper
	instanceID   string

	samplingTicker *time.Ticker
	snapshotTicker *time.Ticker
}

// NewWorker creates a new repository worker.
func NewWorker(
	etcdClient *clientv3.Client,
	getGlobalVar func(string) (string, error),
	newOwner func(string, string) owner.Manager,
	sesspool sessionPool,
	exit chan struct{},
) *Worker {
	w := &Worker{
		etcdClient:   etcdClient,
		exit:         exit,
		getGlobalVar: getGlobalVar,
		sesspool:     sesspool,
		newOwner:     newOwner,
		wg:           util.NewWaitGroupEnhancedWrapper("repository", exit, false),
	}

	w.samplingTicker = time.NewTicker(time.Second)
	w.samplingTicker.Stop()
	w.snapshotTicker = time.NewTicker(time.Second)
	w.snapshotTicker.Stop()

	return w
}

func (w *Worker) runQuery(ctx context.Context, sctx sessionctx.Context, sql string, args ...interface{}) (v []chunk.Row, e error) {
	defer func() {
		logutil.BgLogger().Debug("repository execute SQL", zap.String("sql", sql), zap.NamedError("err", e))
	}()
	exec := sctx.(sqlexec.SQLExecutor)
	res, err := exec.ExecuteInternal(ctx, sql, args...)
	if err == nil {
		if res != nil {
			defer res.Close()
			return sqlexec.DrainRecordSet(ctx, res, 256)
		}
		return nil, nil
	}
	return nil, err
}

func (w *Worker) execRetry(ctx context.Context, sctx sessionctx.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	var errs [5]error
	for i := 0; i < len(errs); i++ {
		res, err := w.runQuery(ctx, sctx, sql, args...)
		if err == nil {
			return res, nil
		}
		errs[i] = err
	}
	return nil, errors.Join(errs[:]...)
}

func (w *Worker) getSessionWithRetry() pools.Resource {
	for {
		_sessctx, err := w.sesspool.Get()
		if err != nil {
			logutil.BgLogger().Warn("repository cannot init session")
			time.Sleep(time.Second)
			continue
		}
		return _sessctx
	}
}

func (w *Worker) readInstanceID() error {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}

	w.instanceID = serverInfo.ID
	return nil
}

func (w *Worker) start(ctx context.Context) func() {
	// TODO: add another txn type
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	return func() {
		w.owner = w.newOwner(ownerKey, promptKey)
		ticker := time.NewTicker(time.Second)
		w.owner.CampaignOwner()
		defer w.owner.CampaignCancel()

		for rtIdx := range workloadTables {
			rt := &workloadTables[rtIdx]
			if rt.table != "" {
				if rt.destTable == "" {
					rt.destTable = "HIST_" + rt.table
				}
			}
		}

		for {
			select {
			case <-w.exit:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if w.owner.IsOwner() {
					logutil.BgLogger().Info("repository has owner!")
					if err := w.createAllTables(ctx); err != nil {
						logutil.BgLogger().Error("repository cannot create tables", zap.NamedError("err", err))
					}
				}

				if !w.checkTablesExists(ctx) {
					continue
				}

				if err := w.readInstanceID(); err != nil {
					// if this fails try it again
					logutil.BgLogger().Info("repository could not get instance ID", zap.NamedError("err", err))
					continue
				}

				w.wg.RunWithRecover(w.startSample(ctx), func(err interface{}) {
					logutil.BgLogger().Info("repository sample panic", zap.Any("err", err), zap.Stack("stack"))
				}, "sample")
				w.wg.RunWithRecover(w.startSnapshot(ctx), func(err interface{}) {
					logutil.BgLogger().Info("repository snapshot panic", zap.Any("err", err), zap.Stack("stack"))
				}, "snapshot")
				w.wg.RunWithRecover(w.startHouseKeeper(ctx), func(err interface{}) {
					logutil.BgLogger().Info("repository housekeeper panic", zap.Any("err", err), zap.Stack("stack"))
				}, "housekeeper")

				return
			}
		}
	}
}

// Start will start the worker.
func (w *Worker) Start(ctx context.Context) {
	if w.cancel != nil {
		return
	}

	nctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.RunWithRecover(w.start(nctx), func(err interface{}) {
		logutil.BgLogger().Info("repository prestart panic", zap.Any("err", err), zap.Stack("stack"))
	}, "prestart")
}

// Stop will stop the worker.
func (w *Worker) Stop() {
	if w.owner != nil {
		w.owner.Cancel()
		w.owner = nil
	}
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
	w.wg.Wait()
}
