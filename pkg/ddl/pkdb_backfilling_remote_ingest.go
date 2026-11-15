// Copyright 2023 PingCAP, Inc.
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
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tikvapi"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const spfreshBuildCoordinatorConcurrency = 8

type remoteIngestExecutor struct {
	taskexecutor.BaseStepExecutor

	ptbl table.PhysicalTable

	store            kv.Storage
	httpClient       *http.Client
	spfreshRoutePlan *backend.SPFreshBuildRoutePlan
}

func newRemoteIngestExecutor(
	ptbl table.PhysicalTable,
	store kv.Storage,
	routePlan *backend.SPFreshBuildRoutePlan,
	httpClient *http.Client,
) (*remoteIngestExecutor, error) {
	if routePlan == nil {
		return nil, errors.New("SPFresh remote ingest requires a build route plan")
	}
	if err := routePlan.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("SPFresh remote ingest requires KV storage")
	}
	if httpClient == nil {
		return nil, errors.New("SPFresh remote ingest requires an HTTP client")
	}
	return &remoteIngestExecutor{
		ptbl:             ptbl,
		store:            store,
		httpClient:       httpClient,
		spfreshRoutePlan: routePlan.Clone(),
	}, nil
}

func (r *remoteIngestExecutor) Init(_ context.Context) error {
	logutil.BgLogger().Info("remote ingest executor init subtask exec env",
		zap.String("category", "ddl"),
		zap.Int64("jobID", r.spfreshRoutePlan.JobID))
	return nil
}

func (r *remoteIngestExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.BgLogger().Info("remote ingest executor run subtask",
		zap.String("category", "ddl"),
		zap.Int64("jobID", r.spfreshRoutePlan.JobID))

	if subtask == nil || len(subtask.Meta) == 0 {
		return errors.New("SPFresh remote ingest requires read-index build metadata")
	}
	sm, err := decodeBackfillSubTaskMeta(ctx, "", subtask.Meta)
	if err != nil {
		return err
	}
	if sm.SPFreshBuildHasData == nil {
		return errors.New("SPFresh remote ingest requires read-index build metadata")
	}
	return r.runSPFreshBuildCoordinator(ctx, *sm.SPFreshBuildHasData)
}

func (r *remoteIngestExecutor) runSPFreshBuildCoordinator(ctx context.Context, hasData bool) error {
	targets, err := resolveRoutedSPFreshWorkerTargets(ctx, r.store, r.spfreshRoutePlan)
	if err != nil {
		return err
	}
	if !hasData {
		return r.bootstrapEmptySPFreshBuild(ctx, targets)
	}
	failpoint.Inject("mockSPFreshBuildTransport", func() { failpoint.Return(nil) })
	shards, err := r.spfreshRoutePlan.AllBuildShardOwners()
	if err != nil {
		return err
	}

	if err := r.sealSPFreshBuildShards(ctx, targets.clusterID, shards); err != nil {
		return err
	}

	return r.commitSPFreshBuildReady(ctx, targets, remote.SPFreshBuildCommitRequest{
		BuildID:    r.spfreshRoutePlan.IndexBuildID(),
		KeyspaceID: r.spfreshRoutePlan.KeyspaceID,
		TableID:    r.spfreshRoutePlan.PhysicalTableID,
		IndexID:    r.spfreshRoutePlan.IndexID,
	})
}

func (r *remoteIngestExecutor) commitSPFreshBuildReady(
	ctx context.Context,
	targets spfreshWorkerTargets,
	req remote.SPFreshBuildCommitRequest,
) error {
	commitEndpoints, err := spfreshCommitEndpointOrder(req.BuildID, targets.workerAddrs)
	if err != nil {
		return err
	}
	var lastErr error
	for i, endpoint := range commitEndpoints {
		err := remote.CommitSPFreshBuildReady(ctx, r.httpClient, endpoint, targets.clusterID, req)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if !remote.IsRetryableSPFreshBuildCommitError(err) {
			return err
		}
		lastErr = err
		if i == len(commitEndpoints)-1 {
			break
		}
		logutil.BgLogger().Warn("retry routed SPFresh build commit on another worker",
			zap.String("buildID", req.BuildID),
			zap.String("failedWorker", endpoint),
			zap.Error(err))
	}
	return lastErr
}

func spfreshCommitEndpointOrder(commitTaskID string, endpoints []string) ([]string, error) {
	selected, err := tikvapi.SelectRendezvousEndpoint(commitTaskID, endpoints)
	if err != nil {
		return nil, err
	}
	ordered := make([]string, 0, len(endpoints))
	ordered = append(ordered, selected)
	for _, endpoint := range endpoints {
		if endpoint == selected {
			continue
		}
		ordered = append(ordered, endpoint)
	}
	return ordered, nil
}

func (r *remoteIngestExecutor) bootstrapEmptySPFreshBuild(ctx context.Context, targets spfreshWorkerTargets) error {
	if r.ptbl == nil {
		return errors.Errorf("SPFresh build %s has zero shards and cannot bootstrap Ready metadata without table metadata", r.spfreshRoutePlan.IndexBuildID())
	}
	if r.store == nil {
		return errors.Errorf("SPFresh build %s has zero shards and cannot bootstrap Ready metadata without KV storage", r.spfreshRoutePlan.IndexBuildID())
	}
	return bootstrapSPFreshEmptyIndex(ctx, r.httpClient, targets, r.ptbl.Meta(), r.spfreshRoutePlan.IndexID, r.spfreshRoutePlan.BaseSnapshotTS)
}

func (r *remoteIngestExecutor) sealSPFreshBuildShards(ctx context.Context, clusterID uint64, shards []backend.SPFreshBuildShardOwner) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(min(len(shards), spfreshBuildCoordinatorConcurrency))
	for _, shard := range shards {
		shard := shard
		g.Go(func() error {
			return remote.SealSPFreshBuildShard(gCtx, r.httpClient, shard.Owner, clusterID, shard.BuildShardID, remote.SPFreshBuildSealRequest{
				BuildID:        r.spfreshRoutePlan.IndexBuildID(),
				KeyspaceID:     r.spfreshRoutePlan.KeyspaceID,
				TableID:        r.spfreshRoutePlan.PhysicalTableID,
				IndexID:        r.spfreshRoutePlan.IndexID,
				BaseSnapshotTS: r.spfreshRoutePlan.BaseSnapshotTS,
			})
		})
	}
	return g.Wait()
}

func (r *remoteIngestExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("remote ingest executor cleanup subtask exec env",
		zap.String("category", "ddl"),
		zap.Int64("jobID", r.spfreshRoutePlan.JobID))
	return nil
}

// bootstrapSPFreshEmptyIndex publishes a zero-input build at its admitted base.
// It is called only after source scanning completes, never from a DDL skip probe.
func bootstrapSPFreshEmptyIndex(ctx context.Context, client *http.Client, targets spfreshWorkerTargets, tbl *model.TableInfo, indexID int64, baseTS uint64) (err error) {
	indexIDs := []int64{indexID}
	workerAddr, err := selectSPFreshBootstrapOwner(targets, tbl, indexIDs)
	if err != nil {
		return err
	}
	failpoint.InjectCall("mockBootstrapEmptySPFreshIndexes", workerAddr, indexIDs, baseTS, &err)
	if err != nil {
		return err
	}
	failpoint.Inject("MockBootstrapEmptySPFreshIndexes", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil)
		}
	})
	return bootstrapSPFreshReadyIndexesWithTargets(ctx, client, targets, workerAddr, tbl, indexIDs, baseTS)
}
