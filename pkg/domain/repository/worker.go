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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util"
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
		"HIST_SNAPSHOTS",
		"",
		sqlescape.MustEscapeSQL(`CREATE TABLE IF NOT EXISTS %n.HIST_SNAPSHOTS (
			SNAP_ID int unsigned NOT NULL AUTO_INCREMENT COMMENT 'Global unique identifier of the snapshot',
			BEGIN_TIME DATETIME NOT NULL COMMENT 'Datetime that TiDB begins taking this snapshot.',
			END_TIME DATETIME NULL COMMENT 'Datetime that TiDB finish taking this snapshot.',
			DB_VER JSON NULL COMMENT 'Versions of TiDB, TiKV, PD at the moment',
			WR_VER int unsigned NULL COMMENT 'Version to identifiy the compatibility of workload schema between releases.',
			INSTANCE varchar(64) DEFAULT NULL COMMENT 'The instance that initializes the snapshots',
			SOURCE VARCHAR(20) NULL COMMENT 'The program that initializes the snaphost. ',
			ERROR TEXT DEFAULT NULL COMMENT 'extra messages are written if anything happens to block that snapshots.')`, WorkloadSchema),
		"",
	},
	//{"INFORMATION_SCHEMA", "TIDB_INDEX_USAGE", snapshotTable, "", "", "", ""},
	//{"INFORMATION_SCHEMA", "TIDB_STATEMENTS_STATS", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "CLIENT_ERRORS_SUMMARY_BY_HOST", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "CLIENT_ERRORS_SUMMARY_BY_USER", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "CLIENT_ERRORS_SUMMARY_GLOBAL", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "TIKV_REGION_STATUS", snapshotTable, "", "", "", ""},
	{"INFORMATION_SCHEMA", "TIKV_STORE_STATUS", snapshotTable, "", "", "", ""},

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
	return w
}

func (w *Worker) runQuery(ctx context.Context, sctx sessionctx.Context, sql string, args ...interface{}) error {
	exec := sctx.(sqlexec.SQLExecutor)
	_, err := exec.ExecuteInternal(ctx, sql, args...)
	return err
}

func (w *Worker) buildInsertQuery(ctx context.Context, sess sessionctx.Context, rt *repositoryTable) error {
	is := sessiontxn.GetTxnManager(sess).GetTxnInfoSchema()
	tbl, err := is.TableByName(ctx, model.NewCIStr(rt.schema), model.NewCIStr(rt.table))
	if err != nil {
		return err
	}

	sb := &strings.Builder{}
	sqlescape.MustFormatSQL(sb, "INSERT %n.%n (", WorkloadSchema, rt.destTable)

	if rt.tableType == snapshotTable {
		fmt.Fprint(sb, "`SNAP_ID`, ")
	}
	fmt.Fprint(sb, "`TS`, ")
	fmt.Fprint(sb, "`INSTANCE_ID`")

	for _, v := range tbl.Cols() {
		sqlescape.MustFormatSQL(sb, ", %n", v.Name.O)
	}
	fmt.Fprint(sb, ") SELECT ")

	if rt.tableType == snapshotTable {
		fmt.Fprint(sb, "%?, now(), %?")
	} else {
		fmt.Fprint(sb, "now(), %?")
	}

	for _, v := range tbl.Cols() {
		sqlescape.MustFormatSQL(sb, ", %n", v.Name.O)
	}
	sqlescape.MustFormatSQL(sb, " FROM %n.%n", rt.schema, rt.table)
	if rt.where != "" {
		fmt.Fprint(sb, "WHERE ", rt.where)
	}

	rt.insertStmt = sb.String()
	return nil
}

func (w *Worker) insertHistSnapshot(ctx context.Context, sctx sessionctx.Context) (uint64, error) {
	snapshotsInsert := sqlescape.MustEscapeSQL("INSERT INTO %n.`HIST_SNAPSHOTS` (`BEGIN_TIME`, `INSTANCE`) VALUES (now(), %%?)",
		WorkloadSchema)
	if err := w.runQuery(ctx, sctx, snapshotsInsert, w.instanceID); err != nil {
		return 0, err
	}

	return sctx.GetSessionVars().StmtCtx.LastInsertID, nil
}

func (w *Worker) updateHistSnapshot(ctx context.Context, sctx sessionctx.Context, snapID uint64, errs []error) error {
	var errs2 interface{} = nil
	if len(errs) > 0 {
		nerr := errors.Join(errs...)
		errs2 = nerr.Error()
	}

	snapshotsUpdate := sqlescape.MustEscapeSQL("UPDATE %n.`HIST_SNAPSHOTS` SET `END_TIME` = now(), `ERROR` = %%? WHERE `SNAP_ID` = %%?",
		WorkloadSchema)
	return w.runQuery(ctx, sctx, snapshotsUpdate, errs2, snapID)
}

func (w *Worker) snapshotTable(ctx context.Context, snapID uint64, rt *repositoryTable) error {
	_sessctx := w.getSessionWithRetry()
	defer w.sesspool.Put(_sessctx)
	sess := _sessctx.(sessionctx.Context)

	if rt.insertStmt == "" {
		if err := w.buildInsertQuery(ctx, sess, rt); err != nil {
			return fmt.Errorf("could not generate insert statement for `%s`: %v", rt.destTable, err)
		}
	}

	if err := w.runQuery(ctx, sess, rt.insertStmt, snapID, w.instanceID); err != nil {
		return fmt.Errorf("could not run insert statement for `%s`: %v", rt.destTable, err)
	}

	return nil
}

func (w *Worker) startSnapshot(ctx context.Context) func() {
	return func() {
		// final code will not use a timer
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		_sessctx := w.getSessionWithRetry()
		defer w.sesspool.Put(_sessctx)
		sess := _sessctx.(sessionctx.Context)

		for {
			select {
			case <-w.exit:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				errs := make([]error, len(workloadTables))

				snapID, err := w.insertHistSnapshot(ctx, sess)
				if err != nil {
					// we can not recover from this
					logutil.BgLogger().Info("Snapshot failed: could not insert into hist_snapshots",
						zap.Error(err))
					continue
				}

				var wg sync.WaitGroup
				cnt := 0
				for rtIdx := range workloadTables {
					rt := &workloadTables[rtIdx]
					if rt.tableType == snapshotTable {
						wg.Add(1)
						go func(i int) {
							defer wg.Done()
							errs[i] = w.snapshotTable(ctx, snapID, rt)
						}(cnt)
						cnt++
					}
				}
				wg.Wait()

				if err := w.updateHistSnapshot(ctx, sess, snapID, errs); err != nil {
					logutil.BgLogger().Info("Snapshot failed: could not update hist_snapshots",
						zap.Error(err))
					continue
				}
			}
		}
	}
}

func (w *Worker) samplingTable(ctx context.Context, rt *repositoryTable) {
	_sessctx := w.getSessionWithRetry()
	defer w.sesspool.Put(_sessctx)
	sess := _sessctx.(sessionctx.Context)

	if rt.insertStmt == "" {
		if err := w.buildInsertQuery(ctx, sess, rt); err != nil {
			m := fmt.Sprintf("Sampling failed: could not generate insert statement for `%s`", rt.destTable)
			logutil.BgLogger().Info(m, zap.Error(err))
			return
		}
	}

	if err := w.runQuery(ctx, sess, rt.insertStmt, w.instanceID); err != nil {
		m := fmt.Sprintf("Sampling failed: could not run insert statement for `%s`", rt.destTable)
		logutil.BgLogger().Info(m, zap.Error(err))
	}
}

func (w *Worker) startSample(ctx context.Context) func() {
	return func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-w.exit:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				var wg util.WaitGroupWrapper

				for rtIdx := range workloadTables {
					rt := &workloadTables[rtIdx]
					if rt.tableType == samplingTable {
						wg.Run(func() {
							w.samplingTable(ctx, rt)
						})
					}
				}

				wg.Wait()
			}
		}
	}
}

func (w *Worker) getSessionWithRetry() pools.Resource {
	for {
		_sessctx, err := w.sesspool.Get()
		if err != nil {
			logutil.BgLogger().Warn("can not init session for repository")
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
					if err := w.createAllTables(ctx); err != nil {
						logutil.BgLogger().Error("can't create workload repository tables", zap.Error(err), zap.Stack("stack"))
					}
				}

				// check if table exists
				if !w.checkTablesExists(ctx) {
					continue
				}

				if err := w.readInstanceID(); err != nil {
					// if this fails try it again
					logutil.BgLogger().Info("could not get instance ID", zap.Error(err))
					continue
				}

				w.wg.RunWithRecover(w.startSample(ctx), func(err interface{}) {
					logutil.BgLogger().Info("sample panic", zap.Any("err", err), zap.Stack("stack"))
				}, "sample")
				w.wg.RunWithRecover(w.startSnapshot(ctx), func(err interface{}) {
					logutil.BgLogger().Info("snapshot panic", zap.Any("err", err), zap.Stack("stack"))
				}, "snapshot")
				w.wg.RunWithRecover(w.startHouseKeeper(ctx), func(err interface{}) {
					logutil.BgLogger().Info("housekeeper panic", zap.Any("err", err), zap.Stack("stack"))
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
		logutil.BgLogger().Info("prestart panic", zap.Any("err", err), zap.Stack("stack"))
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
