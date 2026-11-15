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
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	diststorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/tikvapi"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	spfreshLifecycleFanoutLimit = 8
)

type spfreshWorkerTargets struct {
	clusterID   uint64
	keyspaceID  uint32
	workerAddrs []string
}

func resolveSPFreshStoreIdentity(ctx context.Context, store kv.Storage) (uint64, uint32, error) {
	if store == nil {
		return 0, 0, errors.New("SPFresh requires KV storage")
	}
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return 0, 0, errors.New("SPFresh requires TiKV storage")
	}
	return tikvStore.GetRegionCache().PDClient().GetClusterID(ctx), uint32(store.GetCodec().GetKeyspaceID()), nil
}

var resolveSPFreshWorkerTargets = func(ctx context.Context, store kv.Storage) (spfreshWorkerTargets, error) {
	clusterID, keyspaceID, err := resolveSPFreshStoreIdentity(ctx, store)
	if err != nil {
		return spfreshWorkerTargets{}, err
	}
	cfg := config.GetGlobalConfig()
	endpoints, err := tikvapi.NormalizeEndpoints(cfg.TiKVAPIServiceAddrs, true)
	if err != nil {
		return spfreshWorkerTargets{}, err
	}
	return spfreshWorkerTargets{
		clusterID:   clusterID,
		keyspaceID:  keyspaceID,
		workerAddrs: endpoints,
	}, nil
}

func selectSPFreshBootstrapOwner(targets spfreshWorkerTargets, tbInfo *model.TableInfo, indexIDs []int64) (string, error) {
	if tbInfo == nil {
		return "", errors.New("SPFresh bootstrap requires table info")
	}
	taskID := spfreshBootstrapTaskID(targets.keyspaceID, tbInfo.ID, indexIDs)
	return tikvapi.SelectRendezvousEndpoint(taskID, targets.workerAddrs)
}

func selectSPFreshBootstrapOwnerForTable(store kv.Storage, tbInfo *model.TableInfo, indexIDs []int64) (string, error) {
	endpoints, err := tikvapi.NormalizeEndpoints(config.GetGlobalConfig().TiKVAPIServiceAddrs, true)
	if err != nil {
		return "", err
	}
	var keyspaceID uint32
	if store != nil {
		keyspaceID = uint32(store.GetCodec().GetKeyspaceID())
	}
	targets := spfreshWorkerTargets{
		keyspaceID:  keyspaceID,
		workerAddrs: endpoints,
	}
	return selectSPFreshBootstrapOwner(targets, tbInfo, indexIDs)
}

func spfreshBootstrapTaskID(keyspaceID uint32, tableID int64, indexIDs []int64) string {
	sortedIndexIDs := append([]int64(nil), indexIDs...)
	sort.Slice(sortedIndexIDs, func(i, j int) bool {
		return sortedIndexIDs[i] < sortedIndexIDs[j]
	})

	var b strings.Builder
	fmt.Fprintf(&b, "spfresh-bootstrap-%d-%d", keyspaceID, tableID)
	for _, indexID := range sortedIndexIDs {
		fmt.Fprintf(&b, "-%d", indexID)
	}
	return b.String()
}

type spfreshLifecycleEndpointError struct {
	operation  string
	endpoint   string
	statusCode int
	body       string
	err        error
}

type spfreshLifecycleErrors struct {
	operation string
	errs      []error
	succeeded int
}

func (e *spfreshLifecycleErrors) Error() string {
	messages := make([]string, 0, len(e.errs))
	for _, err := range e.errs {
		messages = append(messages, err.Error())
	}
	return fmt.Sprintf("spfresh %s failed on %d endpoint(s): %s", e.operation, len(e.errs), strings.Join(messages, "; "))
}

func (e *spfreshLifecycleErrors) Unwrap() []error {
	return e.errs
}

type spfreshRuntimeConfigUpdateError struct {
	err            error
	terminal       bool
	mayHaveApplied bool
}

func (e *spfreshRuntimeConfigUpdateError) Error() string {
	return e.err.Error()
}

func (e *spfreshRuntimeConfigUpdateError) Unwrap() error {
	return e.err
}

func (e spfreshLifecycleEndpointError) Error() string {
	msg := fmt.Sprintf("spfresh %s failed on %s", e.operation, e.endpoint)
	if e.statusCode != 0 {
		msg += fmt.Sprintf(": http %d", e.statusCode)
	}
	if e.body != "" {
		msg += fmt.Sprintf(": %s", e.body)
	}
	if e.err != nil && e.body == "" {
		msg += fmt.Sprintf(": %v", e.err)
	}
	return msg
}

func newSPFreshLifecycleEndpointError(operation, endpoint string, err error) error {
	endpointErr := spfreshLifecycleEndpointError{
		operation: operation,
		endpoint:  endpoint,
		err:       err,
	}
	var controlErr *remote.SPFreshControlRequestError
	if stderrors.As(err, &controlErr) {
		endpointErr.statusCode = controlErr.StatusCode
		endpointErr.body = controlErr.Body
	}
	return endpointErr
}

func isSPFreshRuntimeConfigTerminalError(err error) bool {
	var updateErr *spfreshRuntimeConfigUpdateError
	if stderrors.As(err, &updateErr) {
		return updateErr.terminal
	}
	var aggregateErr *spfreshLifecycleErrors
	if stderrors.As(err, &aggregateErr) {
		for _, endpointErr := range aggregateErr.errs {
			if isSPFreshRuntimeConfigTerminalError(endpointErr) {
				return true
			}
		}
		return false
	}
	var endpointErr spfreshLifecycleEndpointError
	if !stderrors.As(err, &endpointErr) {
		return false
	}
	return endpointErr.statusCode >= http.StatusBadRequest &&
		endpointErr.statusCode < http.StatusInternalServerError &&
		endpointErr.statusCode != http.StatusRequestTimeout &&
		endpointErr.statusCode != http.StatusTooManyRequests
}

func mayHaveAppliedSPFreshRuntimeConfig(err error) bool {
	var updateErr *spfreshRuntimeConfigUpdateError
	return stderrors.As(err, &updateErr) && updateErr.mayHaveApplied
}

func classifySPFreshRuntimeConfigUpdateError(err error) error {
	var aggregateErr *spfreshLifecycleErrors
	if !stderrors.As(err, &aggregateErr) {
		return err
	}
	terminal := false
	mayHaveApplied := aggregateErr.succeeded > 0
	for _, endpointErr := range aggregateErr.errs {
		if isSPFreshRuntimeConfigTerminalError(endpointErr) {
			terminal = true
		} else {
			mayHaveApplied = true
		}
	}
	return &spfreshRuntimeConfigUpdateError{
		err:            err,
		terminal:       terminal,
		mayHaveApplied: mayHaveApplied,
	}
}

func resolveRoutedSPFreshWorkerTargets(
	ctx context.Context,
	store kv.Storage,
	routePlan *backend.SPFreshBuildRoutePlan,
) (spfreshWorkerTargets, error) {
	if routePlan == nil {
		return spfreshWorkerTargets{}, errors.New("SPFresh build route plan is not set")
	}
	if err := routePlan.Validate(); err != nil {
		return spfreshWorkerTargets{}, err
	}
	clusterID, keyspaceID, err := resolveSPFreshStoreIdentity(ctx, store)
	if err != nil {
		return spfreshWorkerTargets{}, err
	}
	if routePlan.KeyspaceID != keyspaceID {
		return spfreshWorkerTargets{}, errors.Errorf(
			"SPFresh build route plan keyspace %d does not match store keyspace %d",
			routePlan.KeyspaceID,
			keyspaceID,
		)
	}
	return spfreshWorkerTargets{
		clusterID:   clusterID,
		keyspaceID:  keyspaceID,
		workerAddrs: routePlan.OwnerEndpoints,
	}, nil
}

func aggregateSPFreshLifecycleErrors(operation string, errs []error) error {
	nonNilErrs := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			nonNilErrs = append(nonNilErrs, err)
		}
	}
	if len(nonNilErrs) == 0 {
		return nil
	}
	return &spfreshLifecycleErrors{
		operation: operation,
		errs:      nonNilErrs,
		succeeded: len(errs) - len(nonNilErrs),
	}
}

func fanOutSPFreshLifecycle(
	targets spfreshWorkerTargets,
	operation string,
	fn func(endpoint string) error,
) error {
	if len(targets.workerAddrs) == 0 {
		return errors.Errorf("spfresh %s requires TiKV API service endpoints", operation)
	}
	limit := min(len(targets.workerAddrs), spfreshLifecycleFanoutLimit)
	errs := make([]error, len(targets.workerAddrs))
	var g errgroup.Group
	g.SetLimit(limit)
	for i, endpoint := range targets.workerAddrs {
		i, endpoint := i, endpoint
		g.Go(func() error {
			if err := fn(endpoint); err != nil {
				errs[i] = newSPFreshLifecycleEndpointError(operation, endpoint, err)
			}
			return nil
		})
	}
	_ = g.Wait()
	return aggregateSPFreshLifecycleErrors(operation, errs)
}

func updateSPFreshRuntimeConfig(
	ctx context.Context,
	store kv.Storage,
	tableID int64,
	indexID int64,
	expected model.SPFreshVectorRuntimeConfig,
	desired model.SPFreshVectorRuntimeConfig,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	targets, err := resolveSPFreshWorkerTargets(ctx, store)
	if err != nil {
		return err
	}
	httpClient := util.InternalHTTPClient()
	err = fanOutSPFreshLifecycle(targets, "runtime config update", func(workerAddr string) error {
		return remote.UpdateSPFreshRuntimeConfig(
			ctx,
			httpClient,
			workerAddr,
			targets.clusterID,
			targets.keyspaceID,
			tableID,
			indexID,
			expected,
			desired,
		)
	})
	if err == nil {
		return nil
	}

	return classifySPFreshRuntimeConfigUpdateError(err)
}

func resolveSPFreshClusterID(ctx context.Context, store kv.Storage) (uint64, error) {
	clusterID, _, err := resolveSPFreshStoreIdentity(ctx, store)
	return clusterID, err
}

func spfreshBackfillTaskKey(job *model.Job) string {
	taskKey := fmt.Sprintf("ddl/%s/%d", proto.Backfill, job.ID)
	if job.MultiSchemaInfo != nil {
		taskKey = fmt.Sprintf("%s/%d", taskKey, job.MultiSchemaInfo.Seq)
	}
	return taskKey
}

func loadSPFreshBuildRoutePlanForJob(ctx context.Context, job *model.Job) (*backend.SPFreshBuildRoutePlan, error) {
	if job == nil {
		return nil, nil
	}
	taskManager, err := diststorage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	taskKey := spfreshBackfillTaskKey(job)
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	if err != nil {
		if stderrors.Is(err, diststorage.ErrTaskNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if task == nil || len(task.Meta) == 0 {
		return nil, nil
	}
	var taskMeta BackfillTaskMeta
	if err := json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return nil, err
	}
	return taskMeta.SPFreshBuildRoutePlan, nil
}

func cleanupSPFreshBuildTasksByRoutePlan(
	ctx context.Context,
	httpClient *http.Client,
	clusterID uint64,
	routePlan *backend.SPFreshBuildRoutePlan,
	logger *zap.Logger,
) {
	if routePlan == nil {
		return
	}
	if err := routePlan.Validate(); err != nil {
		logger.Warn("skip SPFresh build task cleanup for invalid route plan",
			zap.Error(err))
		return
	}
	indexBuildID := routePlan.IndexBuildID()
	taskIDPrefix := indexBuildID + "-endpoint-"
	targets := spfreshWorkerTargets{
		clusterID:   clusterID,
		workerAddrs: routePlan.OwnerEndpoints,
	}
	if err := fanOutSPFreshLifecycle(targets, "build shard cleanup", func(workerAddr string) error {
		return remote.DeleteSPFreshBuildTasksByPrefix(ctx, httpClient, workerAddr, clusterID, taskIDPrefix)
	}); err != nil {
		logger.Warn("best-effort SPFresh build shard cleanup failed",
			zap.String("indexBuildID", indexBuildID),
			zap.String("taskIDPrefix", taskIDPrefix),
			zap.Error(err))
	}
}

func cleanupSPFreshBuildTasksForRollback(ctx context.Context, store kv.Storage, job *model.Job, logger *zap.Logger) {
	if job == nil {
		return
	}
	routePlan, err := loadSPFreshBuildRoutePlanForJob(ctx, job)
	if err != nil {
		logger.Warn("load SPFresh build route plan for rollback cleanup failed",
			zap.Int64("jobID", job.ID),
			zap.Error(err))
		return
	}
	if routePlan == nil {
		return
	}
	clusterID, err := resolveSPFreshClusterID(ctx, store)
	if err != nil {
		logger.Warn("resolve SPFresh cluster ID for rollback cleanup failed",
			zap.Int64("jobID", job.ID),
			zap.Error(err))
		return
	}
	cleanupSPFreshBuildTasksByRoutePlan(ctx, util.InternalHTTPClient(), clusterID, routePlan, logger)
}

func cleanupSPFreshBuildTasksForCompletedBackfill(ctx context.Context, store kv.Storage, taskMeta *BackfillTaskMeta, logger *zap.Logger) {
	if taskMeta == nil || taskMeta.SPFreshBuildRoutePlan == nil {
		return
	}
	clusterID, err := resolveSPFreshClusterID(ctx, store)
	if err != nil {
		logger.Warn("resolve SPFresh cluster ID for completed build cleanup failed",
			zap.Int64("jobID", taskMeta.Job.ID),
			zap.Error(err))
		return
	}
	cleanupSPFreshBuildTasksByRoutePlan(ctx, util.InternalHTTPClient(), clusterID, taskMeta.SPFreshBuildRoutePlan, logger)
}

func collectSPFreshIndexIDsFromIndexInfos(idxInfos []*model.IndexInfo) []int64 {
	indexIDs := make([]int64, 0, len(idxInfos))
	for _, idxInfo := range idxInfos {
		if idxInfo == nil || !idxInfo.IsSPFreshIndex() {
			continue
		}
		indexIDs = append(indexIDs, idxInfo.ID)
	}
	return indexIDs
}

func rejectRecoverSPFreshVectorIndexes(tbInfos ...*model.TableInfo) error {
	for _, tbInfo := range tbInfos {
		indexIDs := collectInlineSPFreshVectorIndexes(tbInfo)
		if len(indexIDs) > 0 {
			return dbterror.ErrUnsupportedIndexType.FastGen("recover/flashback table or schema with SPFresh vector index is not supported")
		}
	}
	return nil
}

func rejectRecoverSPFreshVectorIndexesFromRecoverInfos(recoverInfos []*model.RecoverTableInfo) error {
	for _, recoverInfo := range recoverInfos {
		if recoverInfo == nil {
			continue
		}
		if err := rejectRecoverSPFreshVectorIndexes(recoverInfo.TableInfo); err != nil {
			return err
		}
	}
	return nil
}

func teardownSPFreshIndexesByID(
	ctx context.Context,
	store kv.Storage,
	tableID int64,
	indexIDs []int64,
) error {
	return teardownSPFreshIndexesByIDWithRequest(
		ctx, store, tableID, indexIDs, remote.TeardownSPFreshIndex,
	)
}

func teardownSPFreshIndexesByIDWithShortRetry(
	ctx context.Context,
	store kv.Storage,
	tableID int64,
	indexIDs []int64,
) error {
	return teardownSPFreshIndexesByIDWithRequest(
		ctx, store, tableID, indexIDs, remote.TeardownSPFreshIndexWithShortRetry,
	)
}

type teardownSPFreshIndexRequest func(
	context.Context,
	*http.Client,
	string,
	uint64,
	uint32,
	int64,
	int64,
) error

func teardownSPFreshIndexesByIDWithRequest(
	ctx context.Context,
	store kv.Storage,
	tableID int64,
	indexIDs []int64,
	teardownIndex teardownSPFreshIndexRequest,
) error {
	if len(indexIDs) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	targets, err := resolveSPFreshWorkerTargets(ctx, store)
	if err != nil {
		return err
	}
	return fanOutSPFreshLifecycle(targets, "teardown", func(workerAddr string) error {
		httpClient := util.InternalHTTPClient()
		for _, indexID := range indexIDs {
			if err := teardownIndex(
				ctx,
				httpClient,
				workerAddr,
				targets.clusterID,
				targets.keyspaceID,
				tableID,
				indexID,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func teardownSPFreshIndexesForTable(
	ctx context.Context,
	store kv.Storage,
	tbInfo *model.TableInfo,
) error {
	if tbInfo == nil {
		return errors.New("SPFresh teardown requires table info")
	}
	indexIDs := collectInlineSPFreshVectorIndexes(tbInfo)
	return teardownSPFreshIndexesByID(ctx, store, tbInfo.ID, indexIDs)
}

func bootstrapSPFreshIndexesForTruncatedTable(
	ctx context.Context,
	store kv.Storage,
	tbInfo *model.TableInfo,
	job *model.Job,
) error {
	indexIDs := collectInlineSPFreshVectorIndexes(tbInfo)
	if len(indexIDs) == 0 {
		return nil
	}
	job = prepareSPFreshBootstrapJob(job)
	if job == nil {
		return errors.New("truncate table SPFresh bootstrap requires job metadata")
	}
	if !config.SPFreshEnabled() {
		return dbterror.ErrUnsupportedIndexType.FastGen("SPFresh vector index requires remote backend; set tikv-api-service-addrs")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// A failed truncate keeps the old SQL table, but remote admission of the
	// fresh replacement ID can already have succeeded. Persist that identity
	// before I/O so a replacement owner can retire only the abandoned new range.
	target, err := registerSPFreshBootstrap(ctx, store, job, tbInfo, indexIDs, job.RealStartTS)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(bootstrapSPFreshReadyIndexes(
		ctx,
		store,
		target.Owner,
		tbInfo,
		indexIDs,
		target.BaseSnapshotTS,
	))
}

func cleanupTruncatedTableOldSPFreshIndexes(
	ctx context.Context,
	store kv.Storage,
	oldTableID int64,
	newTblInfo *model.TableInfo,
) error {
	if newTblInfo == nil {
		return errors.New("truncate table SPFresh cleanup requires new table info")
	}
	indexIDs := collectInlineSPFreshVectorIndexes(newTblInfo)
	if len(indexIDs) == 0 {
		return nil
	}
	oldTblInfo := newTblInfo.Clone()
	oldTblInfo.ID = oldTableID
	return teardownSPFreshIndexesForTable(ctx, store, oldTblInfo)
}
