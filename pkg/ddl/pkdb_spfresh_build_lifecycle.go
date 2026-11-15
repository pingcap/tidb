// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
)

func spfreshBuildGeneration(job *model.Job) (uint64, error) {
	if job.ReorgMeta == nil || job.RealStartTS == 0 {
		return 0, errors.New("SPFresh build admission requires job reorg metadata and base timestamp")
	}
	if job.ReorgMeta.SPFreshBuildGeneration == 0 {
		if job.SchemaState != model.StateNone {
			return 0, errors.New("SPFresh build generation is missing; cancel and rebuild")
		}
		job.ReorgMeta.SPFreshBuildGeneration = job.RealStartTS
	}
	if job.ReorgMeta.SPFreshBuildGeneration != job.RealStartTS {
		return 0, errors.New("SPFresh build generation differs from its admitted snapshot; cancel and rebuild")
	}
	return job.ReorgMeta.SPFreshBuildGeneration, nil
}

// Admission records only the immutable build identity. Storage establishes and
// maintains protection when Begin is acknowledged, before scanning snapshot B.
func ensureSPFreshBuildAdmission(job *model.Job, indexes []*model.IndexInfo) error {
	if !hasSPFreshIndex(indexes) {
		return nil
	}
	_, err := spfreshBuildGeneration(job)
	return err
}

func beginSPFreshBuild(w *worker, job *model.Job, tableID int64, indexes []*model.IndexInfo) error {
	if !hasSPFreshIndex(indexes) {
		return nil
	}
	index, err := singleSPFreshReadIndex(indexes)
	if err != nil {
		return err
	}
	generation, err := spfreshBuildGeneration(job)
	if err != nil {
		return err
	}
	targets, err := resolveSPFreshWorkerTargets(w.workCtx, w.store)
	if err != nil {
		return err
	}
	return requestSPFreshBuildLifecycle(w.workCtx, w.store, targets.workerAddrs, tableID, index.ID, generation, "begin")
}

func abortSPFreshBuild(w *worker, jobCtx *jobContext, job *model.Job) error {
	if job.ReorgMeta == nil || job.ReorgMeta.SPFreshBuildGeneration == 0 {
		return nil
	}
	info, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return err
	}
	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		return err
	}
	for _, arg := range args.IndexArgs {
		index := info.FindIndexByName(arg.IndexName.L)
		if index == nil || !index.IsSPFreshIndex() {
			continue
		}
		targets, err := resolveSPFreshWorkerTargets(w.workCtx, w.store)
		if err != nil {
			return err
		}
		return requestSPFreshBuildLifecycle(w.workCtx, w.store, targets.workerAddrs, info.ID, index.ID, job.ReorgMeta.SPFreshBuildGeneration, "abort")
	}
	return nil
}

// doSPFreshReorg owns SPFresh construction, including zero-input completion.
// SPFresh DML never writes local index or temporary-index KVs, so it does not
// participate in the ordinary index backfill/merge state machine.
func doSPFreshReorg(w *worker, jobCtx *jobContext, job *model.Job, tbl table.Table, indexes []*model.IndexInfo) (bool, int64, error) {
	if isSPFreshBulkReorg(job, indexes) {
		if err := runSPFreshBulkReorg(jobCtx.stepCtx, w, job, tbl); err != nil {
			ver, err := convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), indexes, err)
			return false, ver, err
		}
		return true, 0, nil
	}
	if !job.ReorgMeta.IsDistReorg {
		state := &w.jobContext(job.ID, job.ReorgMeta).spfresh
		if state.legacyRoute == nil {
			// Local remote writes cannot be replayed after losing the coordinator.
			if job.SnapshotVer != 0 {
				ver, err := convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), indexes,
					errors.New("SPFresh local build context was lost; cancel and rebuild"))
				return false, ver, err
			}
			plan, err := buildSPFreshBuildRoutePlan(w.store, job, tbl.Meta(), indexes)
			if err != nil {
				return false, 0, err
			}
			state.legacyRoute = plan
		}
	}
	if err := beginSPFreshBuild(w, job, tbl.Meta().ID, indexes); err != nil {
		return false, 0, err
	}
	return runReorgJobAndHandleErr(w, jobCtx, job, tbl, indexes, false)
}

var spfreshLifecycleHTTPClient = util.InternalHTTPClient

// A lost Begin or Abort response is retried with the same durable generation.
// Protection and its retirement are exclusively Storage responsibilities.
var requestSPFreshBuildLifecycle = func(ctx context.Context, store kv.Storage, endpoints []string, tableID, indexID int64, baseTS uint64, action string) error {
	withPD, ok := store.(kv.StorageWithPD)
	if !ok || withPD.GetPDClient() == nil {
		return errors.New("SPFresh build lifecycle requires PD")
	}
	if len(endpoints) == 0 {
		return errors.New("SPFresh build lifecycle requires a worker")
	}
	// Allow the previous Storage owner's sixty-second lease to expire before
	// a restarted worker acknowledges the same durable build.
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]any{
		"keyspace_id": uint32(store.GetCodec().GetKeyspaceID()), "table_id": tableID, "index_id": indexID,
		"base_ts": baseTS, "action": action,
	})
	if err != nil {
		return err
	}
	client := *spfreshLifecycleHTTPClient()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	clusterID := withPD.GetPDClient().GetClusterID(ctx)
	var lastErr error
	for {
		for _, endpoint := range endpoints {
			rpcCtx, rpcCancel := context.WithTimeout(ctx, 30*time.Second)
			req, err := http.NewRequestWithContext(rpcCtx, http.MethodPost, fmt.Sprintf("%s/spfresh/build-lifecycle?cluster_id=%d", endpoint, clusterID), bytes.NewReader(body))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				var resp *http.Response
				resp, err = client.Do(req)
				if err == nil {
					encoded, readErr := io.ReadAll(io.LimitReader(resp.Body, 4097))
					_ = resp.Body.Close()
					if readErr != nil {
						err = readErr
					} else if len(encoded) > 4096 {
						err = errors.New("oversized SPFresh lifecycle response")
					} else if resp.StatusCode == http.StatusNoContent {
						rpcCancel()
						return nil
					} else if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 408 && resp.StatusCode != 429 {
						rpcCancel()
						return errors.Errorf("SPFresh %s rejected: HTTP %d: %s", action, resp.StatusCode, encoded)
					} else {
						err = errors.Errorf("SPFresh %s pending: HTTP %d: %s", action, resp.StatusCode, encoded)
					}
				}
			}
			lastErr = err
			rpcCancel()
		}
		select {
		case <-ctx.Done():
			return errors.Annotatef(ctx.Err(), "SPFresh %s unconfirmed: %v", action, lastErr)
		case <-time.After(time.Second):
		}
	}
}
