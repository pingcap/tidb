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
	stderrors "errors"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func (w *worker) etcdCreate(ctx context.Context, key, val string) error {
	ctx, cancel := context.WithTimeout(ctx, etcdOpTimeout)
	defer cancel()
	res, err := w.etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, val)).
		Commit()
	if err != nil {
		return err
	}
	if !res.Succeeded {
		return errors.Errorf("failed to create etcd [%s:%s]", key, val)
	}
	return nil
}

func (w *worker) etcdGet(_ctx context.Context, key string) (string, error) {
	ctx, cancel := context.WithTimeout(_ctx, etcdOpTimeout)
	defer cancel()
	res, err := w.etcdClient.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(res.Kvs) == 0 {
		// key does not exist, just return an empty string
		return "", nil
	}
	return string(res.Kvs[len(res.Kvs)-1].Value), nil
}

func (w *worker) etcdCAS(ctx context.Context, key, oval, nval string) error {
	ctx, cancel := context.WithTimeout(ctx, etcdOpTimeout)
	defer cancel()
	res, err := w.etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", oval)).
		Then(clientv3.OpPut(key, nval)).
		Commit()
	if err != nil {
		return err
	}
	if !res.Succeeded {
		return errors.Errorf("failed to update etcd [%s:%s] to [%s:%s]", key, oval, key, nval)
	}
	return nil
}

func queryMaxSnapID(ctx context.Context, sctx sessionctx.Context) (uint64, error) {
	query := sqlescape.MustEscapeSQL("SELECT MAX(`SNAP_ID`) FROM %n.%n", mysql.WorkloadSchema, histSnapshotsTable)
	rs, err := runQuery(ctx, sctx, query)
	if err != nil {
		return 0, err
	}
	if len(rs) > 0 {
		if rs[0].IsNull(0) {
			return 0, nil
		}
		return rs[0].GetUint64(0), nil
	}
	return 0, errors.New("no rows returned when querying max snap id")
}

func (w *worker) getSnapID(ctx context.Context) (uint64, error) {
	snapIDStr, err := w.etcdGet(ctx, snapIDKey)
	if err != nil {
		return 0, err
	}
	if snapIDStr == "" {
		return 0, errKeyNotFound
	}
	return strconv.ParseUint(snapIDStr, 10, 64)
}

func upsertHistSnapshot(ctx context.Context, sctx sessionctx.Context, snapID uint64) error {
	// TODO: fill DB_VER, WR_VER
	snapshotsInsert := sqlescape.MustEscapeSQL("INSERT INTO %n.%n (`BEGIN_TIME`, `SNAP_ID`) VALUES (now(), %%?) ON DUPLICATE KEY UPDATE `BEGIN_TIME` = now()",
		mysql.WorkloadSchema, histSnapshotsTable)
	_, err := runQuery(ctx, sctx, snapshotsInsert, snapID)
	return err
}

func (w *worker) updateHistSnapshot(ctx context.Context, snapID uint64, errs []error) error {
	_sessctx := w.getSessionWithRetry()
	defer w.sesspool.Put(_sessctx)
	sctx := _sessctx.(sessionctx.Context)

	var nerr any
	if err := stderrors.Join(errs...); err != nil {
		nerr = err.Error()
	}

	snapshotsUpdate := sqlescape.MustEscapeSQL("UPDATE %n.%n SET `END_TIME` = now(), `ERROR` = COALESCE(CONCAT(ERROR, %%?), ERROR, %%?) WHERE `SNAP_ID` = %%?", mysql.WorkloadSchema, histSnapshotsTable)
	_, err := runQuery(ctx, sctx, snapshotsUpdate, nerr, nerr, snapID)
	return err
}

func (w *worker) snapshotTable(ctx context.Context, snapID uint64, rt *repositoryTable) error {
	_sessctx := w.getSessionWithRetry()
	defer w.sesspool.Put(_sessctx)
	sess := _sessctx.(sessionctx.Context)

	if rt.insertStmt == "" {
		if err := buildInsertQuery(ctx, sess, rt); err != nil {
			return fmt.Errorf("could not generate insert statement for `%s`: %v", rt.destTable, err)
		}
	}

	if _, err := runQuery(ctx, sess, rt.insertStmt, snapID, w.instanceID); err != nil {
		return fmt.Errorf("could not run insert statement for `%s`: %v", rt.destTable, err)
	}

	return nil
}

// takeSnapshot increments the value of snapIDKey, which triggers the tidb
// nodes to run the snapshot process.  See the code in startSnapshot().
func (w *worker) takeSnapshot(ctx context.Context) (uint64, error) {
	_sessctx := w.getSessionWithRetry()
	defer w.sesspool.Put(_sessctx)
	sess := _sessctx.(sessionctx.Context)

	var snapID uint64
	var err error
	for range snapshotRetries {
		isEmpty := false
		snapID, err = w.getSnapID(ctx)
		// Sometimes, a new etcd cluster without persisted snap_id may be used,
		// e.g. serverless TiDB or manually cleaned PD.
		// In such case, we can query the maximum snap_id in the table,
		//  and try to recover that snap_id directly by a etcd transaction.
		if stderrors.Is(err, errKeyNotFound) {
			snapID, err = queryMaxSnapID(ctx, sess)
			isEmpty = true
		}
		if err != nil {
			err = fmt.Errorf("cannot get current snapid: %w", err)
			continue
		}

		// Use UPSERT to ensure this SQL doesn't fail on duplicate snapID.
		//
		// NOTE: In a highly unlikely corner case, there could be two owners.
		// This might occur if upsertHistSnapshot succeeds but updateSnapID fails
		// due to another owner winning the etcd CAS loop.
		// While undesirable, this scenario is acceptable since both owners would
		// likely share similar datetime values and same cluster version.
		if err = upsertHistSnapshot(ctx, sess, snapID+1); err != nil {
			err = fmt.Errorf("could not insert into hist_snapshots: %w", err)
			continue
		}

		if isEmpty {
			err = w.etcdCreate(ctx, snapIDKey, strconv.FormatUint(snapID+1, 10))
		} else {
			err = w.etcdCAS(ctx, snapIDKey, strconv.FormatUint(snapID, 10), strconv.FormatUint(snapID+1, 10))
		}

		if err != nil {
			err = fmt.Errorf("cannot update current snapid to %d: %w", snapID, err)
			continue
		}

		break
	}

	// return the last error seen, if it ended on an error
	return snapID, err
}

func (w *worker) startSnapshot(_ctx context.Context) func() {
	return func() {
		w.resetSnapshotInterval(w.snapshotInterval)

		// this is for etcd watch
		// other wise wch won't be collected after the exit of this function
		ctx, cancel := context.WithCancel(_ctx)
		defer cancel()
		snapIDCh := w.etcdClient.Watch(ctx, snapIDKey)

		for {
			select {
			case <-ctx.Done():
				return
			case resp := <-snapIDCh:
				// This case is triggered by both by w.snapshotInterval and the SQL command, which calls w.takeSnapshot() directly.
				if len(resp.Events) < 1 {
					// since there is no event, we don't know the latest snapid either
					// really should not happen except creation
					// but let us just skip
					logutil.BgLogger().Debug("workload repository cannot get snap ID update")
					continue
				}

				// there probably will be multiple events
				// e.g. this node got stuck somehow
				// it eventually got notified by more than two snapid
				// if that's the case, let us just take a snap for the last one
				snapIDStr := string(resp.Events[len(resp.Events)-1].Kv.Value)
				snapID, err := strconv.ParseUint(snapIDStr, 10, 64)
				if err != nil {
					logutil.BgLogger().Info("workload repository snapshot failed: could not parse snapID", zap.String("snapID", snapIDStr), zap.NamedError("err", err))
					continue
				}

				errs := make([]error, len(w.workloadTables))
				var wg util.WaitGroupWrapper
				cnt := 0
				for rtIdx := range w.workloadTables {
					rt := &w.workloadTables[rtIdx]
					if rt.tableType != snapshotTable {
						continue
					}
					pcnt := cnt
					wg.Run(func() {
						errs[pcnt] = w.snapshotTable(ctx, snapID, rt)
					})
					cnt++
				}
				wg.Wait()

				if err := w.updateHistSnapshot(ctx, snapID, errs); err != nil {
					logutil.BgLogger().Info("workload repository snapshot failed: could not update hist_snapshots", zap.NamedError("err", err))
				}
			case <-w.snapshotTicker.C:
				if w.owner.IsOwner() {
					if snapID, err := w.takeSnapshot(ctx); err != nil {
						logutil.BgLogger().Info("workload repository snapshot failed", zap.NamedError("err", err))
					} else {
						logutil.BgLogger().Info("workload repository ran snapshot", zap.String("owner", w.instanceID), zap.Uint64("snapID", snapID))
					}
				}
			}
		}
	}
}

func (w *worker) resetSnapshotInterval(newRate int32) {
	w.snapshotTicker.Reset(time.Duration(newRate) * time.Second)
}

func (w *worker) changeSnapshotInterval(_ context.Context, d string) error {
	n, err := strconv.Atoi(d)

	if _, _err_ := failpoint.Eval(_curpkg_("FastRunawayGC")); _err_ == nil {
		err = errors.New("fake error")
	}

	if err != nil {
		return errWrongValueForVar.GenWithStackByArgs(repositorySnapshotInterval, d)
	}

	w.Lock()
	defer w.Unlock()

	if int32(n) != w.snapshotInterval {
		w.snapshotInterval = int32(n)
		if w.snapshotTicker != nil {
			w.resetSnapshotInterval(w.snapshotInterval)
		}
	}

	return nil
}
