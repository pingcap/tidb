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
	"encoding/hex"
	"slices"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

const (
	affinityReconcileInterval        = 5 * time.Minute
	affinityReconcileTimeout         = 30 * time.Second
	affinityReconcileDeleteBatchSize = 100
	affinityReconcileGracePeriod     = 5 * time.Minute
	loadAllGCDeleteRangeQuery        = "SELECT HIGH_PRIORITY start_key, end_key FROM mysql.gc_delete_range"
)

type parsedPDGroup struct {
	id              string
	tableID         int64
	partitionID     int64
	isPartition     bool
	createTimestamp uint64
}

type expectedGroupOwner struct {
	schemaID int64
	tableID  int64
}

type inFlightAffinityProtection struct {
	skipRound         bool
	skipDeletion      bool
	protectedTableIDs map[int64]struct{}
	mutatingTableIDs  map[int64]struct{}
	mutatingSchemaIDs map[int64]struct{}
}

func newInFlightAffinityProtection() *inFlightAffinityProtection {
	return &inFlightAffinityProtection{
		protectedTableIDs: make(map[int64]struct{}),
		mutatingTableIDs:  make(map[int64]struct{}),
		mutatingSchemaIDs: make(map[int64]struct{}),
	}
}

func (p *inFlightAffinityProtection) protectID(id int64) {
	if id > 0 {
		p.protectedTableIDs[id] = struct{}{}
	}
}

func (p *inFlightAffinityProtection) protectIDs(ids []int64) {
	for _, id := range ids {
		p.protectID(id)
	}
}

func (p *inFlightAffinityProtection) protectTableInfo(tblInfo *model.TableInfo) {
	if tblInfo == nil {
		return
	}
	p.protectID(tblInfo.ID)
	if pi := tblInfo.Partition; pi != nil {
		for _, def := range pi.Definitions {
			p.protectID(def.ID)
		}
		for _, def := range pi.AddingDefinitions {
			p.protectID(def.ID)
		}
		for _, def := range pi.DroppingDefinitions {
			p.protectID(def.ID)
		}
		p.protectIDs(pi.NewPartitionIDs)
	}
}

// affinityReconciler periodically reconciles PD affinity groups against TiDB
// schema metadata on the DDL owner.
type affinityReconciler struct {
	store        kv.Storage
	infoCache    *infoschema.InfoCache
	schemaLoader SchemaLoader
	isOwner      func() bool

	interval    time.Duration
	timeout     time.Duration
	batchSize   int
	gracePeriod time.Duration
	now         func() time.Time
	pdGate      *affinityPDGate

	mu             sync.Mutex
	suspectOrphans map[string]struct{}
}

func newAffinityReconciler(d *ddl) *affinityReconciler {
	var isOwner func() bool
	if d.ownerManager != nil {
		isOwner = d.ownerManager.IsOwner
	}
	return &affinityReconciler{
		store:          d.store,
		infoCache:      d.infoCache,
		schemaLoader:   d.schemaLoader,
		isOwner:        isOwner,
		interval:       affinityReconcileInterval,
		timeout:        affinityReconcileTimeout,
		batchSize:      affinityReconcileDeleteBatchSize,
		gracePeriod:    affinityReconcileGracePeriod,
		now:            time.Now,
		pdGate:         newAffinityPDGate(),
		suspectOrphans: make(map[string]struct{}),
	}
}

func (r *affinityReconciler) checkIsOwner() bool {
	if r.isOwner != nil {
		return r.isOwner()
	}
	return true
}

func (r *affinityReconciler) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.suspectOrphans = make(map[string]struct{})
}

func (r *affinityReconciler) currentTime() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *affinityReconciler) roundTimeout() time.Duration {
	if r.timeout > 0 {
		return r.timeout
	}
	return affinityReconcileTimeout
}

func (r *affinityReconciler) deleteBatchSize() int {
	if r.batchSize > 0 {
		return r.batchSize
	}
	return affinityReconcileDeleteBatchSize
}

func (r *affinityReconciler) run(ctx context.Context, sessPool *sess.Pool) {
	defer r.clear()
	interval := r.interval
	if interval <= 0 {
		interval = affinityReconcileInterval
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		r.runOneTick(ctx, sessPool)
		timer.Reset(interval)
	}
}

func (r *affinityReconciler) runOneTick(ctx context.Context, sessPool *sess.Pool) {
	if !r.checkIsOwner() || ctx.Err() != nil {
		r.clear()
		return
	}
	if sessPool == nil {
		r.clear()
		logutil.DDLLogger().Warn("session pool is unavailable for affinity reconcile")
		return
	}
	sctx, err := sessPool.Get()
	if err != nil {
		r.clear()
		if ctx.Err() == nil {
			logutil.DDLLogger().Warn("get session for affinity reconcile failed", zap.Error(err))
		}
		return
	}
	roundCtx, cancel := context.WithTimeout(kv.WithInternalSourceType(ctx, kv.InternalTxnDDL), r.roundTimeout())
	err = r.reconcileOnce(roundCtx, sctx)
	cancel()
	sessPool.Put(sctx)
	if err != nil && ctx.Err() == nil {
		logutil.DDLLogger().Warn("affinity reconcile failed", zap.Error(err))
	}
}

// reconcileOnce runs one reconciliation pass using strict reverse-lifecycle
// sampling (T1 PD -> T2 tidb_ddl_job -> T3 InfoSchema -> T4 gc_delete_range)
// so that no group created by an in-flight DDL or retained for GC recovery
// can be misclassified as an orphan.
func (r *affinityReconciler) reconcileOnce(ctx context.Context, sctx sessionctx.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	codec := r.store.GetCodec()

	// Any round that cannot complete orphan classification (PD/DDL/InfoSchema/GC
	// read errors, unsupported PD, FLASHBACK CLUSTER, or deletion suppression)
	// must invalidate prior suspectOrphans so two-round confirmation restarts
	// from consecutive completed classifications.
	classified := false
	defer func() {
		if !classified {
			r.clear()
		}
	}()
	if !r.checkIsOwner() || ctx.Err() != nil {
		return nil
	}

	// T1: Read all affinity groups from PD and keep only those strictly belonging
	// to this TiDB instance's Classic or NextGen keyspace namespace.
	allPDGroups, err := affinity.GetAllGroups(ctx)
	if err != nil {
		if affinity.IsUnsupportedError(err) {
			logutil.DDLLogger().Debug("skip affinity reconcile because PD does not support affinity API", zap.Error(err))
			return nil
		}
		return errors.Trace(err)
	}
	pdGroups := make(map[string]parsedPDGroup, len(allPDGroups))
	for id, state := range allPDGroups {
		tableID, partitionID, isPartition, ok := ParseAffinityGroupID(codec, id)
		if !ok {
			continue
		}
		var createTS uint64
		if state != nil {
			createTS = state.CreateTimestamp
		}
		pdGroups[id] = parsedPDGroup{
			id:              id,
			tableID:         tableID,
			partitionID:     partitionID,
			isPartition:     isPartition,
			createTimestamp: createTS,
		}
	}
	failpoint.InjectCall("afterAffinityReconcileReadPD")

	// T2: Read in-flight DDL jobs BEFORE InfoSchema. Fail-closed on any read/decode error.
	inFlight, err := loadInFlightAffinityProtection(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}
	if inFlight.skipRound {
		logutil.DDLLogger().Info("skip affinity reconcile round due to in-flight cluster flashback")
		return nil
	}
	failpoint.InjectCall("afterAffinityReconcileReadDDLJobs")

	// T3: Reload and read latest InfoSchema. Fail-closed on nil loader or reload error.
	expectedGroups, expectedOwners, err := r.reloadAndCollectExpectedGroups(codec, inFlight)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("afterAffinityReconcileReadInfoSchema")

	// Repair missing groups for active affinity tables not currently being mutated/dropped.
	// Because T1 < T3, a table created/altered between T1 and T3 or dropped/altered after T3
	// could appear in expectedGroups \ pdGroups. Double-check against PD and a fresh T2->T3
	// snapshot under pdGate before creating so dropped or disabled groups are never resurrected.
	candidateMissing := collectMissingGroups(expectedGroups, expectedOwners, pdGroups, inFlight)
	var repairErr error
	if len(candidateMissing) > 0 {
		var abortRound, repairSkipDeletion bool
		abortRound, repairSkipDeletion, repairErr = r.repairMissingGroups(ctx, sctx, codec, candidateMissing)
		if abortRound {
			return nil
		}
		if repairSkipDeletion {
			inFlight.skipDeletion = true
		}
	}

	// If PD has no groups in this namespace, clear any stale suspectOrphans and
	// skip scanning mysql.gc_delete_range (T4).
	if len(pdGroups) == 0 {
		return repairErr
	}

	if inFlight.skipDeletion {
		return repairErr
	}

	// T4: Read all pending rows from mysql.gc_delete_range (unfiltered by ts,
	// excluding gc_delete_range_done and index-only ranges). Fail-closed on any query/decode error.
	pendingGCPhysicalIDs, err := loadPendingGCPhysicalIDs(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Identify candidate orphan groups in the current namespace.
	now := r.currentTime()
	currentOrphans := make(map[string]struct{})
	for groupID, info := range pdGroups {
		if _, expected := expectedGroups[groupID]; expected {
			continue
		}
		if _, protected := inFlight.protectedTableIDs[info.tableID]; protected {
			continue
		}
		if info.isPartition {
			if _, protected := inFlight.protectedTableIDs[info.partitionID]; protected {
				continue
			}
		}
		if _, pendingGC := pendingGCPhysicalIDs[info.tableID]; pendingGC {
			continue
		}
		if info.isPartition {
			if _, pendingGC := pendingGCPhysicalIDs[info.partitionID]; pendingGC {
				continue
			}
		}
		if info.createTimestamp > 0 && r.gracePeriod > 0 {
			createdAt := time.Unix(int64(info.createTimestamp), 0)
			if now.Sub(createdAt) < r.gracePeriod {
				continue
			}
		}
		currentOrphans[groupID] = struct{}{}
	}

	// Two-round confirmation: a group must be observed as an orphan in two
	// consecutive rounds before deletion.
	var toDelete []string
	r.mu.Lock()
	if r.suspectOrphans == nil {
		r.suspectOrphans = make(map[string]struct{})
	}
	for id := range r.suspectOrphans {
		if _, stillOrphan := currentOrphans[id]; !stillOrphan {
			delete(r.suspectOrphans, id)
		}
	}
	for id := range currentOrphans {
		if _, seenBefore := r.suspectOrphans[id]; seenBefore {
			toDelete = append(toDelete, id)
		} else {
			r.suspectOrphans[id] = struct{}{}
		}
	}
	r.mu.Unlock()
	classified = true

	if len(toDelete) == 0 {
		return repairErr
	}

	slices.Sort(toDelete)
	batchSize := r.deleteBatchSize()
	var deleteErr error
	for i := 0; i < len(toDelete); i += batchSize {
		end := min(i+batchSize, len(toDelete))
		batch := toDelete[i:end]

		failpoint.InjectCall("beforeAffinityReconcileDeleteGate")
		if err := r.pdGate.lock(ctx); err != nil {
			if deleteErr == nil {
				deleteErr = err
			}
			break
		}
		abort, skipDel, batchErr := r.deleteOrphanBatchUnderGate(ctx, sctx, codec, pdGroups, batch)
		r.pdGate.unlock()
		if abort {
			r.clear()
			return errors.Trace(batchErr)
		}
		if skipDel {
			r.clear()
			if batchErr != nil {
				return errors.Trace(batchErr)
			}
			return repairErr
		}
		if batchErr != nil && deleteErr == nil {
			deleteErr = batchErr
		}
	}

	if deleteErr != nil {
		return errors.Trace(deleteErr)
	}
	return repairErr
}

func (r *affinityReconciler) deleteOrphanBatchUnderGate(
	ctx context.Context,
	sctx sessionctx.Context,
	codec tikv.Codec,
	pdGroups map[string]parsedPDGroup,
	batch []string,
) (abortRound, skipDeletion bool, batchErr error) {
	// Re-verify under pdGate against fresh in-flight DDL, InfoSchema, and pending
	// gc_delete_range so a concurrent ALTER TABLE ... AFFINITY = 'table'/'partition'
	// or a DROP TABLE + RECOVER TABLE never loses its active or recoverable group.
	recheckInFlight, err := loadInFlightAffinityProtection(ctx, sctx)
	if err != nil {
		logutil.DDLLogger().Warn("reconcile failed to recheck in-flight DDL jobs before deleting orphans", zap.Error(err))
		return false, true, err
	}
	if recheckInFlight.skipRound {
		logutil.DDLLogger().Info("skip affinity reconcile delete due to in-flight cluster flashback")
		return true, false, nil
	}
	if recheckInFlight.skipDeletion {
		return false, true, nil
	}
	recheckExpected, _, err := r.reloadAndCollectExpectedGroups(codec, recheckInFlight)
	if err != nil {
		logutil.DDLLogger().Warn("reconcile failed to reload InfoSchema before deleting orphans", zap.Error(err))
		return false, true, err
	}
	recheckPendingGC, err := loadPendingGCPhysicalIDs(ctx, sctx)
	if err != nil {
		logutil.DDLLogger().Warn("reconcile failed to recheck pending gc_delete_range before deleting orphans", zap.Error(err))
		return false, true, err
	}

	filtered := make([]string, 0, len(batch))
	var skipped []string
	for _, id := range batch {
		if _, expected := recheckExpected[id]; expected {
			skipped = append(skipped, id)
			continue
		}
		info := pdGroups[id]
		if _, protected := recheckInFlight.protectedTableIDs[info.tableID]; protected {
			skipped = append(skipped, id)
			continue
		}
		if info.isPartition {
			if _, protected := recheckInFlight.protectedTableIDs[info.partitionID]; protected {
				skipped = append(skipped, id)
				continue
			}
		}
		if _, pendingGC := recheckPendingGC[info.tableID]; pendingGC {
			skipped = append(skipped, id)
			continue
		}
		if info.isPartition {
			if _, pendingGC := recheckPendingGC[info.partitionID]; pendingGC {
				skipped = append(skipped, id)
				continue
			}
		}
		filtered = append(filtered, id)
	}
	if len(skipped) > 0 {
		r.mu.Lock()
		for _, id := range skipped {
			delete(r.suspectOrphans, id)
		}
		r.mu.Unlock()
	}
	if len(filtered) == 0 {
		return false, false, nil
	}

	// Re-check DDL ownership right before issuing the PD write. This closes the
	// local race between owner retirement and pdGate acquisition; an HTTP request
	// already in flight when the etcd lease expires cannot be fenced by PD's
	// affinity API, and the next owner's reconcile pass heals any stale write.
	if !r.checkIsOwner() || ctx.Err() != nil {
		logutil.DDLLogger().Info("abort affinity reconcile delete because DDL ownership was lost")
		return true, false, nil
	}

	logutil.DDLLogger().Info("reconcile deleting confirmed orphan affinity groups from PD",
		zap.Strings("groupIDs", filtered))
	if err := affinity.DeleteGroups(ctx, filtered); err != nil {
		if affinity.IsUnsupportedError(err) {
			logutil.DDLLogger().Debug("skip affinity reconcile delete because PD does not support affinity API", zap.Error(err))
			return true, false, nil
		}
		logutil.DDLLogger().Warn("reconcile failed to delete orphan affinity groups from PD",
			zap.Strings("groupIDs", filtered), zap.Error(err))
		// Keep failed batch IDs in suspectOrphans so the next round retries them.
		return false, false, err
	}
	r.mu.Lock()
	for _, id := range filtered {
		delete(r.suspectOrphans, id)
	}
	r.mu.Unlock()
	return false, false, nil
}

func (r *affinityReconciler) reloadAndCollectExpectedGroups(
	codec tikv.Codec,
	inFlight *inFlightAffinityProtection,
) (map[string][]pdhttp.AffinityGroupKeyRange, map[string]expectedGroupOwner, error) {
	if r.schemaLoader == nil {
		return nil, nil, errors.New("schemaLoader is nil during affinity reconcile")
	}
	if err := r.schemaLoader.Reload(); err != nil {
		return nil, nil, errors.Trace(err)
	}
	if r.infoCache == nil {
		return nil, nil, errors.New("infoCache is nil during affinity reconcile")
	}
	is := r.infoCache.GetLatest()
	if is == nil {
		return nil, nil, errors.New("latest infoschema is nil during affinity reconcile")
	}

	expectedGroups := make(map[string][]pdhttp.AffinityGroupKeyRange)
	expectedOwners := make(map[string]expectedGroupOwner)
	affinityTables := is.ListTablesWithSpecialAttribute(infoschemacontext.AffinityAttribute)
	for _, res := range affinityTables {
		var schemaID int64
		if dbInfo, ok := is.SchemaByName(res.DBName); ok {
			schemaID = dbInfo.ID
		}
		for _, tblInfo := range res.TableInfos {
			if tblInfo == nil || tblInfo.Affinity == nil {
				continue
			}
			groups, buildErr := buildAffinityGroupDefinitions(codec, tblInfo, nil)
			if buildErr != nil {
				logutil.DDLLogger().Warn("failed to build expected affinity groups during reconcile; protecting table IDs",
					zap.Error(buildErr), zap.Int64("tableID", tblInfo.ID))
				if inFlight != nil {
					inFlight.protectTableInfo(tblInfo)
				}
				continue
			}
			for groupID, ranges := range groups {
				expectedGroups[groupID] = ranges
				expectedOwners[groupID] = expectedGroupOwner{schemaID: schemaID, tableID: tblInfo.ID}
			}
		}
	}
	return expectedGroups, expectedOwners, nil
}

func collectMissingGroups(
	expectedGroups map[string][]pdhttp.AffinityGroupKeyRange,
	expectedOwners map[string]expectedGroupOwner,
	pdGroups map[string]parsedPDGroup,
	inFlight *inFlightAffinityProtection,
) map[string][]pdhttp.AffinityGroupKeyRange {
	missing := make(map[string][]pdhttp.AffinityGroupKeyRange)
	for groupID, ranges := range expectedGroups {
		if _, exists := pdGroups[groupID]; exists {
			continue
		}
		owner := expectedOwners[groupID]
		if _, mutating := inFlight.mutatingTableIDs[owner.tableID]; mutating {
			continue
		}
		if owner.schemaID > 0 {
			if _, mutating := inFlight.mutatingSchemaIDs[owner.schemaID]; mutating {
				continue
			}
		}
		missing[groupID] = ranges
	}
	return missing
}

func (r *affinityReconciler) repairMissingGroups(
	ctx context.Context,
	sctx sessionctx.Context,
	codec tikv.Codec,
	candidateMissing map[string][]pdhttp.AffinityGroupKeyRange,
) (abortRound, skipDeletion bool, repairErr error) {
	candidateIDs := make([]string, 0, len(candidateMissing))
	for id := range candidateMissing {
		candidateIDs = append(candidateIDs, id)
	}
	slices.Sort(candidateIDs)

	// Step 1: Filter out groups that were already created in PD between T1 and T3.
	existing, err := affinity.GetGroups(ctx, candidateIDs)
	if err != nil {
		if affinity.IsUnsupportedError(err) {
			logutil.DDLLogger().Debug("skip affinity reconcile repair because PD does not support affinity API", zap.Error(err))
			return true, false, nil
		}
		logutil.DDLLogger().Warn("reconcile failed to recheck missing affinity groups in PD", zap.Error(err))
		return false, false, err
	}
	remainingIDs := make([]string, 0, len(candidateIDs))
	for _, id := range candidateIDs {
		if _, exists := existing[id]; !exists {
			remainingIDs = append(remainingIDs, id)
		}
	}
	if len(remainingIDs) == 0 {
		return false, false, nil
	}

	// Step 2: Re-verify against fresh in-flight DDL and InfoSchema under pdGate
	// before creating each batch, so groups removed by a concurrent
	// ALTER TABLE ... AFFINITY = '' or DROP TABLE are never resurrected.
	batchSize := r.deleteBatchSize()
	for i := 0; i < len(remainingIDs); i += batchSize {
		end := min(i+batchSize, len(remainingIDs))
		batchIDs := remainingIDs[i:end]

		failpoint.InjectCall("beforeAffinityReconcileRepairGate")
		if err := r.pdGate.lock(ctx); err != nil {
			if repairErr == nil {
				repairErr = err
			}
			break
		}
		abort, skipDel, batchErr := r.repairMissingBatchUnderGate(ctx, sctx, codec, batchIDs)
		r.pdGate.unlock()
		if abort {
			return true, false, nil
		}
		if skipDel {
			skipDeletion = true
		}
		if batchErr != nil && repairErr == nil {
			repairErr = batchErr
		}
	}
	return false, skipDeletion, repairErr
}

func (r *affinityReconciler) repairMissingBatchUnderGate(
	ctx context.Context,
	sctx sessionctx.Context,
	codec tikv.Codec,
	batchIDs []string,
) (abortRound, skipDeletion bool, batchErr error) {
	recheckInFlight, err := loadInFlightAffinityProtection(ctx, sctx)
	if err != nil {
		logutil.DDLLogger().Warn("reconcile failed to recheck in-flight DDL jobs before repair", zap.Error(err))
		return false, false, err
	}
	if recheckInFlight.skipRound {
		logutil.DDLLogger().Info("skip affinity reconcile round due to in-flight cluster flashback")
		return true, false, nil
	}
	recheckExpected, recheckOwners, err := r.reloadAndCollectExpectedGroups(codec, nil)
	if err != nil {
		logutil.DDLLogger().Warn("reconcile failed to reload InfoSchema before repair", zap.Error(err))
		return false, recheckInFlight.skipDeletion, err
	}

	batch := make(map[string][]pdhttp.AffinityGroupKeyRange, len(batchIDs))
	for _, id := range batchIDs {
		ranges, stillExpected := recheckExpected[id]
		if !stillExpected {
			continue
		}
		owner := recheckOwners[id]
		if _, mutating := recheckInFlight.mutatingTableIDs[owner.tableID]; mutating {
			continue
		}
		if owner.schemaID > 0 {
			if _, mutating := recheckInFlight.mutatingSchemaIDs[owner.schemaID]; mutating {
				continue
			}
		}
		batch[id] = ranges
	}
	if len(batch) == 0 {
		return false, recheckInFlight.skipDeletion, nil
	}

	// Re-check DDL ownership right before issuing the PD write. This closes the
	// local race between owner retirement and pdGate acquisition; an HTTP request
	// already in flight when the etcd lease expires cannot be fenced by PD's
	// affinity API, and the next owner's reconcile pass heals any stale write.
	if !r.checkIsOwner() || ctx.Err() != nil {
		logutil.DDLLogger().Info("abort affinity reconcile repair because DDL ownership was lost")
		return true, false, nil
	}

	logutil.DDLLogger().Info("reconcile creating missing affinity groups in PD",
		zap.Int("count", len(batch)))
	if err := affinity.CreateGroupsIfNotExists(ctx, batch); err != nil {
		if affinity.IsUnsupportedError(err) {
			logutil.DDLLogger().Debug("skip affinity reconcile repair because PD does not support affinity API", zap.Error(err))
			return true, false, nil
		}
		logutil.DDLLogger().Warn("reconcile failed to create missing affinity groups in PD", zap.Error(err))
		return false, recheckInFlight.skipDeletion, err
	}
	return false, recheckInFlight.skipDeletion, nil
}

func loadInFlightAffinityProtection(ctx context.Context, sctx sessionctx.Context) (*inFlightAffinityProtection, error) {
	var jobs []*model.Job
	var err error
	failpoint.Inject("mockAffinityReconcileGetDDLJobsErr", func() {
		err = errors.New("injected error reading tidb_ddl_job")
	})
	if err == nil {
		jobs, err = GetAllDDLJobs(ctx, sctx)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	prot := newInFlightAffinityProtection()
	for _, job := range jobs {
		if err := collectInFlightJobProtection(job, prot); err != nil {
			return nil, errors.Trace(err)
		}
		if prot.skipRound {
			return prot, nil
		}
	}
	return prot, nil
}

func collectInFlightJobProtection(job *model.Job, prot *inFlightAffinityProtection) error {
	if job == nil {
		return nil
	}
	if job.Type == model.ActionFlashbackCluster {
		prot.skipRound = true
		return nil
	}

	prot.protectID(job.TableID)
	if job.BinlogInfo != nil && job.BinlogInfo.TableInfo != nil {
		prot.protectTableInfo(job.BinlogInfo.TableInfo)
	}

	switch job.Type {
	case model.ActionCreateTable, model.ActionCreateMaterializedViewShadow:
		args, err := model.GetCreateTableArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		prot.protectTableInfo(args.TableInfo)
	case model.ActionCreateMaterializedView:
		args, err := model.GetCreateMaterializedViewArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		prot.protectTableInfo(args.TableInfo)
	case model.ActionCreateTables:
		args, err := model.GetBatchCreateTableArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		for _, tblArgs := range args.Tables {
			if tblArgs != nil {
				prot.protectTableInfo(tblArgs.TableInfo)
			}
		}
	case model.ActionTruncateTable, model.ActionTruncateTablePartition:
		if job.TableID > 0 {
			prot.mutatingTableIDs[job.TableID] = struct{}{}
		}
		var (
			args *model.TruncateTableArgs
			err  error
		)
		if job.IsDone() || job.IsSynced() {
			args, err = model.GetFinishedTruncateTableArgs(job)
		} else {
			args, err = model.GetTruncateTableArgs(job)
		}
		if err != nil {
			return errors.Trace(err)
		}
		prot.protectID(args.NewTableID)
		prot.protectIDs(args.NewPartitionIDs)
		prot.protectIDs(args.OldPartitionIDs)
		if args.NewTableID > 0 {
			prot.mutatingTableIDs[args.NewTableID] = struct{}{}
		}
	case model.ActionAlterTableAffinity:
		if job.TableID > 0 {
			prot.mutatingTableIDs[job.TableID] = struct{}{}
		}
	case model.ActionRecoverTable, model.ActionRecoverSchema:
		args, err := model.GetRecoverArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		if args.RecoverInfo != nil {
			if job.Type == model.ActionRecoverSchema && args.RecoverInfo.LoadTablesOnExecute && len(args.RecoverInfo.RecoverTableInfos) == 0 {
				prot.skipDeletion = true
			}
			for _, rt := range args.RecoverInfo.RecoverTableInfos {
				if rt != nil {
					prot.protectTableInfo(rt.TableInfo)
				}
			}
		}
	case model.ActionDropSchema:
		if job.SchemaID > 0 {
			prot.mutatingSchemaIDs[job.SchemaID] = struct{}{}
		}
		if job.IsDone() || job.IsSynced() {
			args, err := model.GetFinishedDropSchemaArgs(job)
			if err != nil {
				return errors.Trace(err)
			}
			prot.protectIDs(args.AllDroppedTableIDs)
		} else {
			// Before DROP DATABASE finishes StateDeleteOnly, AllDroppedTableIDs is
			// not populated yet and tables could vanish from InfoSchema before
			// finishDDLJob inserts their gc_delete_range rows.
			prot.skipDeletion = true
		}
	case model.ActionDropTable, model.ActionDropMaterializedView,
		model.ActionDropMaterializedViewLog, model.ActionDropMaterializedViewShadow:
		if job.TableID > 0 {
			prot.mutatingTableIDs[job.TableID] = struct{}{}
		}
		if job.IsDone() || job.IsSynced() {
			args, err := model.GetFinishedDropTableArgs(job)
			if err != nil {
				return errors.Trace(err)
			}
			prot.protectIDs(args.OldPartitionIDs)
		}
	case model.ActionMViewRefreshOutOfPlaceCutover:
		args, err := model.GetRefreshMaterializedViewCompleteOutOfPlaceCutoverArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		prot.protectID(args.OldMViewID)
		prot.protectID(args.ShadowTableID)
		prot.protectID(args.OldMLogID)
		prot.protectID(args.NewMLogID)
		if args.OldMViewID > 0 {
			prot.mutatingTableIDs[args.OldMViewID] = struct{}{}
		}
		if args.ShadowTableID > 0 {
			prot.mutatingTableIDs[args.ShadowTableID] = struct{}{}
		}
	case model.ActionExchangeTablePartition:
		args, err := model.GetExchangeTablePartitionArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		prot.protectID(args.PartitionID)
		prot.protectID(args.PTTableID)
	case model.ActionMultiSchemaChange:
		if job.MultiSchemaInfo != nil {
			for i, sub := range job.MultiSchemaInfo.SubJobs {
				proxyJob := sub.ToProxyJob(job, i)
				if err := collectInFlightJobProtection(&proxyJob, prot); err != nil {
					return errors.Trace(err)
				}
				if prot.skipRound {
					return nil
				}
			}
		}
	}
	return nil
}

func stripKeyspacePrefixForTableKey(key []byte) ([]byte, bool) {
	if len(key) > 0 && key[0] == 't' {
		return key, true
	}
	_, decoded, err := tikv.DecodeKey(key, kvrpcpb.APIVersion_V2)
	if err != nil || len(decoded) == 0 || decoded[0] != 't' {
		return nil, false
	}
	return decoded, true
}

// isIndexDeleteRange reports whether [startKey, endKey) is strictly an index
// range of physicalID (such as DROP INDEX / DROP COLUMN / rolled-back ADD INDEX),
// rather than a retired table/partition range [t<id>, t<id+1>) or a partially
// resumed table range whose end_key is still t<id+1>.
func isIndexDeleteRange(startKey, endKey []byte, physicalID int64) bool {
	sKey, ok := stripKeyspacePrefixForTableKey(startKey)
	if !ok || !tablecodec.IsIndexKey(sKey) {
		return false
	}
	eKey, ok := stripKeyspacePrefixForTableKey(endKey)
	if !ok || !tablecodec.IsIndexKey(eKey) {
		return false
	}
	return tablecodec.DecodeTableID(eKey) == physicalID
}

func loadPendingGCPhysicalIDs(ctx context.Context, sctx sessionctx.Context) (map[int64]struct{}, error) {
	failpoint.Inject("mockAffinityReconcileLoadGCRangesErr", func() {
		failpoint.Return(nil, errors.New("injected error reading gc_delete_range"))
	})
	rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, loadAllGCDeleteRangeQuery)
	if rs != nil {
		defer terror.Call(rs.Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	pendingIDs := make(map[int64]struct{})
	req := rs.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	for {
		if err := rs.Next(ctx, req); err != nil {
			return nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			startKey, err := hex.DecodeString(row.GetString(0))
			if err != nil {
				return nil, errors.Trace(err)
			}
			endKey, err := hex.DecodeString(row.GetString(1))
			if err != nil {
				return nil, errors.Trace(err)
			}
			physicalID := tablecodec.DecodeTableID(startKey)
			if physicalID <= 0 {
				return nil, errors.Errorf("failed to decode physical table ID from gc_delete_range start_key %q", row.GetString(0))
			}
			if isIndexDeleteRange(startKey, endKey, physicalID) {
				continue
			}
			pendingIDs[physicalID] = struct{}{}
		}
	}
	return pendingIDs, nil
}
