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
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

func registerSPFreshBootstrap(ctx context.Context, store kv.Storage, job *model.Job, tbInfo *model.TableInfo, indexIDs []int64, baseTS uint64) (meta.SPFreshBootstrapTarget, error) {
	targets, err := resolveSPFreshWorkerTargets(ctx, store)
	if err != nil {
		return meta.SPFreshBootstrapTarget{}, err
	}
	owner, err := selectSPFreshBootstrapOwner(targets, tbInfo, indexIDs)
	if err != nil {
		return meta.SPFreshBootstrapTarget{}, err
	}
	target := meta.SPFreshBootstrapTarget{TableID: tbInfo.ID, IndexIDs: indexIDs, ClusterID: targets.clusterID, KeyspaceID: targets.keyspaceID, WorkerEndpoints: targets.workerAddrs, Owner: owner, BaseSnapshotTS: baseTS}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	err = kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		var err error
		target, err = meta.NewMutator(txn).RegisterSPFreshBootstrap(job.ID, target)
		return err
	})
	return target, err
}

func finishSPFreshBootstrap(jobCtx *jobContext, job *model.Job) error {
	var keepFence bool
	switch job.Type {
	case model.ActionCreateTable:
		args, err := model.GetCreateTableArgs(job)
		// Malformed canceled jobs cannot be allowed to bypass a possible prior intent.
		keepFence = err != nil || len(collectInlineSPFreshVectorIndexes(args.TableInfo)) != 0
	case model.ActionCreateTables:
		args, err := model.GetBatchCreateTableArgs(job)
		if err != nil {
			keepFence = true
		} else {
			for _, table := range args.Tables {
				keepFence = keepFence || len(collectInlineSPFreshVectorIndexes(table.TableInfo)) != 0
			}
		}
	case model.ActionTruncateTable:
		// A successful truncate carries the new table in its history. On
		// cancellation the old table still describes the potentially admitted
		// indexes. If metadata is unavailable, conservatively fence admission.
		var tblInfo *model.TableInfo
		if job.BinlogInfo != nil {
			tblInfo = job.BinlogInfo.TableInfo
		}
		if tblInfo == nil {
			var err error
			tblInfo, err = jobCtx.metaMut.GetTable(job.SchemaID, job.TableID)
			keepFence = err != nil || tblInfo == nil
		}
		keepFence = keepFence || len(collectInlineSPFreshVectorIndexes(tblInfo)) != 0
	default:
		return nil
	}
	return jobCtx.metaMut.FinishSPFreshBootstrap(job.ID, job.IsDone() || job.IsSynced(), keepFence)
}

func cleanupSPFreshBootstrap(ctx context.Context, record *meta.SPFreshBootstrapJob, teardown teardownSPFreshIndexRequest) error {
	if !record.Finished || record.Succeeded {
		return errors.New("cannot clean an active or successful SPFresh bootstrap")
	}
	for _, target := range record.Targets {
		targets := spfreshWorkerTargets{clusterID: target.ClusterID, keyspaceID: target.KeyspaceID, workerAddrs: target.WorkerEndpoints}
		if err := fanOutSPFreshLifecycle(targets, "bootstrap cleanup", func(endpoint string) error {
			for _, indexID := range target.IndexIDs {
				if err := teardown(ctx, util.InternalHTTPClient(), endpoint, target.ClusterID, target.KeyspaceID, target.TableID, indexID); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Failed creates and truncate replacements own fresh table IDs exclusively.
// Retire the whole range, including bootstrap metadata, using the ordinary GC
// queue. Reusing the DDL job identity makes retries idempotent and agrees with
// CTAS rollback's range.
func spfreshBootstrapCleanupGCJob(record *meta.SPFreshBootstrapJob) *model.Job {
	tableIDs := make([]int64, 0, len(record.Targets))
	for _, target := range record.Targets {
		tableIDs = append(tableIDs, target.TableID)
	}
	job := &model.Job{ID: record.JobID, Type: model.ActionDropSchema, Version: model.JobVersion2}
	job.FillFinishedArgs(&model.DropSchemaArgs{AllDroppedTableIDs: tableIDs})
	return job
}

// A replacement owner reads identities from metadata, never current endpoint
// configuration. Failed sweeps remain due; successful sweeps retain a fence and
// recur to cover remote requests whose timeout did not stop server-side work.
func (d *ddl) runSPFreshBootstrapCleanupLoop() {
	ticker := time.NewTicker(spfreshBulkCleanupInterval)
	defer ticker.Stop()
	var after int64
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
		}
		if !d.ownerManager.IsOwner() {
			continue
		}
		version, err := d.store.CurrentVersion(kv.GlobalTxnScope)
		if err != nil {
			logutil.DDLLogger().Warn("read SPFresh bootstrap cleanup version", zap.Error(err))
			continue
		}
		records, err := meta.ReadSPFreshBootstrapJobs(d.store.GetSnapshot(version), after, 16)
		if err != nil {
			logutil.DDLLogger().Warn("read SPFresh bootstrap cleanup queue", zap.Error(err))
			continue
		}
		if len(records) == 0 {
			after = 0
		}
		for _, record := range records {
			after = record.JobID
			if !record.Finished || record.Succeeded || len(record.Targets) == 0 || time.Now().UnixMilli() < record.NextSweepAtUnixMilli {
				continue
			}
			if !d.ownerManager.IsOwner() {
				break
			}
			ctx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
			err = cleanupSPFreshBootstrap(ctx, record, remote.TeardownSPFreshIndexWithShortRetry)
			if err == nil {
				err = d.delRangeMgr.addDelRangeJob(ctx, spfreshBootstrapCleanupGCJob(record))
			}
			if err == nil {
				ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
				err = kv.RunInNewTxn(ctx, d.store, true, func(_ context.Context, txn kv.Transaction) error {
					return meta.NewMutator(txn).ScheduleSPFreshBootstrapCleanup(record.JobID, time.Now().Add(spfreshBulkCleanupResweepInterval).UnixMilli())
				})
			}
			cancel()
			if err != nil {
				logutil.DDLLogger().Warn("SPFresh bootstrap cleanup remains pending", zap.Int64("job_id", record.JobID), zap.Error(err))
			}
		}
	}
}
