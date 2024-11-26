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
		Then(clientv3.OpPut(snapIDKey, val)).
		Commit()
	if err != nil {
		return err
	}
	if !res.Succeeded {
		return errors.Errorf("failed to create etcd [%s:%s]", key, val)
	}
	return nil
}

func (w *worker) etcdGet(_ctx context.Context, key, defval string) (string, error) {
	ctx, cancel := context.WithTimeout(_ctx, etcdOpTimeout)
	defer cancel()
	res, err := w.etcdClient.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(res.Kvs) == 0 {
		// nonexistent, create it atomically
		// otherwise etcdCAS will fail
		return defval, w.etcdCreate(_ctx, key, defval)
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

func (w *worker) getSnapID(ctx context.Context) (uint64, error) {
	snapIDStr, err := w.etcdGet(ctx, snapIDKey, "0")
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(snapIDStr, 10, 64)
}

func (w *worker) updateSnapID(ctx context.Context, oid, nid uint64) error {
	return w.etcdCAS(ctx, snapIDKey,
		strconv.FormatUint(oid, 10),
		strconv.FormatUint(nid, 10))
}

func upsertHistSnapshot(ctx context.Context, sctx sessionctx.Context, snapID uint64) error {
	// TODO: fill DB_VER, WR_VER
	snapshotsInsert := sqlescape.MustEscapeSQL("INSERT INTO %n.%n (`BEGIN_TIME`, `SNAP_ID`) VALUES (now(), %%?) ON DUPLICATE KEY UPDATE `BEGIN_TIME` = now()",
		WorkloadSchema, histSnapshotsTable)
	_, err := runQuery(ctx, sctx, snapshotsInsert, snapID)
	return err
}

func updateHistSnapshot(ctx context.Context, sctx sessionctx.Context, snapID uint64, errs []error) error {
	var nerr any
	if err := stderrors.Join(errs...); err != nil {
		nerr = err.Error()
	}

	snapshotsUpdate := sqlescape.MustEscapeSQL("UPDATE %n.%n SET `END_TIME` = now(), `ERROR` = COALESCE(CONCAT(ERROR, %%?), ERROR, %%?) WHERE `SNAP_ID` = %%?", WorkloadSchema, histSnapshotsTable)
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

func (w *worker) startSnapshot(_ctx context.Context) func() {
	return func() {
		w.Lock()
		w.snapshotTicker = time.NewTicker(time.Duration(w.snapshotInterval) * time.Second)
		w.Unlock()

		_sessctx := w.getSessionWithRetry()
		defer w.sesspool.Put(_sessctx)
		sess := _sessctx.(sessionctx.Context)

		// this is for etcd watch
		// other wise wch won't be collected after the exit of this function
		ctx, cancel := context.WithCancel(_ctx)
		defer cancel()
		wch := w.etcdClient.Watch(ctx, snapIDKey)

		for {
			select {
			case <-ctx.Done():
				return
			case <-w.snapshotTicker.C:
				// coordination logic
				if !w.owner.IsOwner() {
					continue
				}

				for range 5 {
					snapID, err := w.getSnapID(ctx)
					if err != nil {
						logutil.BgLogger().Info("workload repository cannot get current snapid", zap.NamedError("err", err))
						continue
					}
					// use upsert such that this SQL does not fail on duplicated snapID
					//
					// NOTE: in an almost impossible corner case, there may be two owners.
					// maybe upsertHistSnapshot succeed and updateSnapID fail, because
					// another owner won the etcd CAS loop.
					// it is unwanted but acceptable. because two owners should share
					// similar datetime, same cluster versions.
					if err := upsertHistSnapshot(ctx, sess, snapID+1); err != nil {
						logutil.BgLogger().Info("workload repository could not insert into hist_snapshots", zap.NamedError("err", err))
						continue
					}
					err = w.updateSnapID(ctx, snapID, snapID+1)
					if err != nil {
						logutil.BgLogger().Info("workload repository cannot update current snapid", zap.Uint64("new_id", snapID), zap.NamedError("err", err))
						continue
					}
					logutil.BgLogger().Info("workload repository fired snapshot", zap.String("owner", w.instanceID), zap.Uint64("snapID", snapID+1))
					break
				}
			case resp := <-wch:
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

				errs := make([]error, len(workloadTables))
				var wg util.WaitGroupWrapper
				cnt := 0
				for rtIdx := range workloadTables {
					rt := &workloadTables[rtIdx]
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

				if err := updateHistSnapshot(ctx, sess, snapID, errs); err != nil {
					logutil.BgLogger().Info("workload repository snapshot failed: could not update hist_snapshots", zap.NamedError("err", err))
				}
			}
		}
	}
}

func (w *worker) resetSnapshotInterval(newRate int32) {
	if w.snapshotTicker != nil {
		w.snapshotTicker.Reset(time.Duration(newRate) * time.Second)
	}
}

func (w *worker) changeSnapshotInterval(_ context.Context, d string) error {
	n, err := strconv.Atoi(d)
	if err != nil {
		return err
	}

	w.Lock()
	defer w.Unlock()

	if int32(n) != w.snapshotInterval {
		w.snapshotInterval = int32(n)
		w.resetSnapshotInterval(w.snapshotInterval)
	}

	return nil
}
