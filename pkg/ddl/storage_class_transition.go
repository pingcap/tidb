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
	"cmp"
	"context"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/tikv/client-go/v2/oracle"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

const (
	storageClassDirectionToIA            = "TO_IA"
	storageClassDirectionToStandard      = "TO_STANDARD"
	storageClassTransitionPollInterval   = 2 * time.Second
	storageClassTransitionRequestTimeout = 30 * time.Second
)

// StorageClassTransition is one SQL-visible explicit storage-class operation.
// Multiple physical partitions can belong to one operation. In that case the
// partition fields are empty and the replica counters are aggregated.
type StorageClassTransition struct {
	TableSchema       string
	TableName         string
	TableID           int64
	PartitionName     string
	PartitionID       int64
	Direction         string
	State             string
	TotalReplicas     uint64
	CompletedReplicas uint64
	Progress          float64
	ProgressValid     bool
	StartTime         time.Time
	FinishTime        time.Time
	Duration          time.Duration
	LastUpdateTime    time.Time
	StatusValid       bool
	PhysicalTableIDs  []int64
	startTS           uint64
}

type storageClassTransitionKey struct {
	tableID int64
	target  string
	startTS uint64
}

type storageClassTransitionTarget struct {
	physicalID    int64
	partitionID   int64
	partitionName string
	target        string
}

type storageClassTransitionOperation struct {
	StorageClassTransition
	schemaID          int64
	currentSchemaName string
	currentTableName  string
	targets           []storageClassTransitionTarget
}

type storageClassTransitionManager struct {
	ddl *ddl
	mu  struct {
		sync.RWMutex
		active   map[storageClassTransitionKey]StorageClassTransition
		observed map[storageClassTransitionKey]StorageClassTransition
	}
}

func newStorageClassTransitionManager(d *ddl) *storageClassTransitionManager {
	m := &storageClassTransitionManager{ddl: d}
	m.mu.active = make(map[storageClassTransitionKey]StorageClassTransition)
	m.mu.observed = make(map[storageClassTransitionKey]StorageClassTransition)
	return m
}

func (m *storageClassTransitionManager) snapshot() []StorageClassTransition {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]StorageClassTransition, 0, len(m.mu.active))
	for _, transition := range m.mu.active {
		transition.PhysicalTableIDs = slices.Clone(transition.PhysicalTableIDs)
		transition.Duration = time.Since(transition.StartTime)
		if transition.Duration < 0 {
			transition.Duration = 0
		}
		result = append(result, transition)
	}
	slices.SortFunc(result, func(a, b StorageClassTransition) int {
		if a.StartTime.Before(b.StartTime) {
			return -1
		}
		if a.StartTime.After(b.StartTime) {
			return 1
		}
		if a.TableID != b.TableID {
			return cmp.Compare(a.TableID, b.TableID)
		}
		if a.Direction < b.Direction {
			return -1
		}
		if a.Direction > b.Direction {
			return 1
		}
		return 0
	})
	return result
}

// StorageClassTransitions returns the owner-maintained active snapshot.
func (d *ddl) StorageClassTransitions() []StorageClassTransition {
	return d.storageClassTransitionManager.snapshot()
}

func (m *storageClassTransitionManager) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.mu.active)
	clear(m.mu.observed)
}

func (m *storageClassTransitionManager) discover() (
	map[storageClassTransitionKey]*storageClassTransitionOperation,
	map[storageClassTransitionKey]*storageClassTransitionOperation,
	error,
) {
	latest := m.ddl.infoCache.GetLatest()
	if latest == nil {
		return nil, nil, errors.New("information schema is not initialized")
	}
	active := make(map[storageClassTransitionKey]*storageClassTransitionOperation)
	pending := make(map[storageClassTransitionKey]*storageClassTransitionOperation)
	for _, db := range latest.ListTablesWithSpecialAttribute(infoschemacontext.StorageClassAttribute) {
		for _, tblInfo := range db.TableInfos {
			if tblInfo.StorageClassTransitionStartTS != 0 {
				schemaName := firstNonEmpty(tblInfo.StorageClassTransitionSchemaName, db.DBName.O)
				tableName := firstNonEmpty(tblInfo.StorageClassTransitionTableName, tblInfo.Name.O)
				key, err := addStorageClassTransitionTarget(active, schemaName, tableName, tblInfo, "", 0, tblInfo.ID,
					tblInfo.StorageClassTransitionTarget, tblInfo.StorageClassTransitionStartTS)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				setStorageClassTransitionCurrentNames(active[key], db.DBName.O, tblInfo.Name.O)
			}
			if tblInfo.Partition != nil {
				for _, partition := range tblInfo.Partition.Definitions {
					if partition.StorageClassTransitionStartTS == 0 {
						continue
					}
					schemaName := firstNonEmpty(partition.StorageClassTransitionSchemaName, db.DBName.O)
					tableName := firstNonEmpty(partition.StorageClassTransitionTableName, tblInfo.Name.O)
					partitionName := firstNonEmpty(partition.StorageClassTransitionPartitionName, partition.Name.O)
					key, err := addStorageClassTransitionTarget(active, schemaName, tableName, tblInfo, partitionName, partition.ID, partition.ID,
						partition.StorageClassTransitionTarget, partition.StorageClassTransitionStartTS)
					if err != nil {
						return nil, nil, errors.Trace(err)
					}
					setStorageClassTransitionCurrentNames(active[key], db.DBName.O, tblInfo.Name.O)
				}
			}
			for _, history := range tblInfo.StorageClassTransitionPendingHistory {
				if len(history.Targets) == 0 || history.FinishTS == 0 {
					return nil, nil, errors.Errorf("invalid pending storage class transition history for table %d at start TS %d", tblInfo.ID, history.StartTS)
				}
				if history.State != model.StorageClassTransitionStateCompleted && history.State != model.StorageClassTransitionStateSuperseded {
					return nil, nil, errors.Errorf("invalid pending storage class transition state %q for table %d", history.State, tblInfo.ID)
				}
				if history.StatusValid && history.CompletedReplicas > history.TotalReplicas {
					return nil, nil, errors.Errorf(
						"invalid pending storage class transition replica counts %d/%d for table %d",
						history.CompletedReplicas,
						history.TotalReplicas,
						tblInfo.ID,
					)
				}
				if history.State == model.StorageClassTransitionStateCompleted &&
					(!history.StatusValid || history.TotalReplicas == 0 || history.CompletedReplicas != history.TotalReplicas) {
					return nil, nil, errors.Errorf(
						"invalid completed storage class transition replica counts %d/%d for table %d",
						history.CompletedReplicas,
						history.TotalReplicas,
						tblInfo.ID,
					)
				}
				for _, target := range history.Targets {
					key, err := addStorageClassTransitionTarget(
						pending,
						firstNonEmpty(history.SchemaName, db.DBName.O),
						firstNonEmpty(history.TableName, tblInfo.Name.O),
						tblInfo,
						target.PartitionName,
						target.PartitionID,
						target.PhysicalID,
						history.Target,
						history.StartTS,
					)
					if err != nil {
						return nil, nil, errors.Trace(err)
					}
					operation := pending[key]
					operation.State = history.State
					operation.FinishTime = model.TSConvert2Time(history.FinishTS)
					operation.StatusValid = history.StatusValid
					operation.TotalReplicas = history.TotalReplicas
					operation.CompletedReplicas = history.CompletedReplicas
					if history.StatusValid && history.TotalReplicas > 0 {
						operation.Progress = float64(history.CompletedReplicas) / float64(history.TotalReplicas)
						operation.ProgressValid = true
					}
					setStorageClassTransitionCurrentNames(operation, db.DBName.O, tblInfo.Name.O)
				}
			}
		}
	}
	for _, operations := range []map[storageClassTransitionKey]*storageClassTransitionOperation{active, pending} {
		for _, operation := range operations {
			slices.SortFunc(operation.targets, func(a, b storageClassTransitionTarget) int {
				if a.physicalID < b.physicalID {
					return -1
				}
				if a.physicalID > b.physicalID {
					return 1
				}
				return 0
			})
			operation.PhysicalTableIDs = make([]int64, len(operation.targets))
			for i, target := range operation.targets {
				operation.PhysicalTableIDs[i] = target.physicalID
			}
		}
	}
	return active, pending, nil
}

func firstNonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func setStorageClassTransitionCurrentNames(operation *storageClassTransitionOperation, schemaName, tableName string) {
	operation.currentSchemaName = schemaName
	operation.currentTableName = tableName
}

func addStorageClassTransitionTarget(
	operations map[storageClassTransitionKey]*storageClassTransitionOperation,
	schemaName string,
	tableName string,
	tblInfo *model.TableInfo,
	partitionName string,
	partitionID, physicalID int64,
	target string,
	startTS uint64,
) (storageClassTransitionKey, error) {
	if startTS == 0 {
		return storageClassTransitionKey{}, errors.Errorf("invalid storage class transition metadata for physical table %d", physicalID)
	}
	direction := storageClassDirectionToStandard
	switch target {
	case model.StorageClassTierIA:
		direction = storageClassDirectionToIA
	case model.StorageClassTierStandard:
	default:
		return storageClassTransitionKey{}, errors.Errorf("invalid storage class transition target %q for physical table %d", target, physicalID)
	}

	key := storageClassTransitionKey{tableID: tblInfo.ID, target: target, startTS: startTS}
	operation := operations[key]
	if operation == nil {
		operation = &storageClassTransitionOperation{
			StorageClassTransition: StorageClassTransition{
				TableSchema: schemaName,
				TableName:   tableName,
				TableID:     tblInfo.ID,
				Direction:   direction,
				StartTime:   model.TSConvert2Time(startTS),
				startTS:     startTS,
			},
			schemaID: tblInfo.DBID,
		}
		operations[key] = operation
	} else if operation.TableID != tblInfo.ID || operation.TableSchema != schemaName || operation.TableName != tableName ||
		operation.Direction != direction ||
		!operation.StartTime.Equal(model.TSConvert2Time(startTS)) {
		return storageClassTransitionKey{}, errors.Errorf("conflicting storage class transition metadata for table %d at start TS %d", tblInfo.ID, startTS)
	}
	for _, existing := range operation.targets {
		if existing.physicalID == physicalID {
			return storageClassTransitionKey{}, errors.Errorf("duplicate physical table %d in storage class transition at start TS %d", physicalID, startTS)
		}
	}
	operation.targets = append(operation.targets, storageClassTransitionTarget{
		physicalID: physicalID, partitionID: partitionID, partitionName: partitionName, target: target,
	})
	if len(operation.targets) == 1 && partitionID != 0 {
		operation.PartitionID = partitionID
		operation.PartitionName = partitionName
	} else if len(operation.targets) > 1 {
		operation.PartitionID = 0
		operation.PartitionName = ""
	}
	return key, nil
}

func sameStorageClassTransition(a, b StorageClassTransition) bool {
	return a.TableID == b.TableID && a.startTS == b.startTS &&
		a.PartitionID == b.PartitionID && a.Direction == b.Direction && a.StartTime.Equal(b.StartTime) &&
		slices.Equal(a.PhysicalTableIDs, b.PhysicalTableIDs)
}

func (m *storageClassTransitionManager) setActive(
	activeOperations map[storageClassTransitionKey]*storageClassTransitionOperation,
	pendingOperations map[storageClassTransitionKey]*storageClassTransitionOperation,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	active := make(map[storageClassTransitionKey]StorageClassTransition, len(activeOperations))
	for key, operation := range activeOperations {
		transition := operation.StorageClassTransition
		if previous, ok := m.mu.observed[key]; ok && previous.StatusValid && !transition.StatusValid &&
			sameStorageClassTransition(previous, transition) {
			transition.TotalReplicas = previous.TotalReplicas
			transition.CompletedReplicas = previous.CompletedReplicas
			transition.Progress = previous.Progress
			transition.ProgressValid = previous.ProgressValid
			transition.LastUpdateTime = previous.LastUpdateTime
			transition.StatusValid = true
		}
		active[key] = transition
	}
	m.mu.active = active
	for key := range m.mu.observed {
		if _, ok := activeOperations[key]; ok {
			continue
		}
		if _, ok := pendingOperations[key]; !ok {
			delete(m.mu.observed, key)
		}
	}
}

func (m *storageClassTransitionManager) observe(
	ctx context.Context,
	operation *storageClassTransitionOperation,
	tikvStores map[int64]pdhttp.StoreInfo,
) (bool, error) {
	var ready, total uint64
	for _, target := range operation.targets {
		statuses, err := infosync.CollectStorageClassStatus(ctx, target.physicalID, target.target, tikvStores)
		if err != nil {
			return false, errors.Trace(err)
		}
		for _, status := range statuses {
			if status.Ready > status.Total {
				return false, errors.Errorf("TiKV store %d returned ready %d greater than total %d for physical table %d",
					status.StoreID, status.Ready, status.Total, target.physicalID)
			}
			ready += status.Ready
			total += status.Total
		}
	}
	complete := updateStorageClassTransitionProgress(operation, ready, total)
	operation.LastUpdateTime = time.Now()
	operation.StatusValid = true
	m.mu.Lock()
	m.mu.observed[operation.key()] = operation.StorageClassTransition
	m.mu.Unlock()
	return complete, nil
}

func updateStorageClassTransitionProgress(operation *storageClassTransitionOperation, ready, total uint64) bool {
	operation.CompletedReplicas = ready
	operation.TotalReplicas = total
	operation.Progress = 0
	operation.ProgressValid = false
	if total > 0 {
		operation.Progress = float64(ready) / float64(total)
		operation.ProgressValid = true
	}
	return total > 0 && ready == total
}

func (operation *storageClassTransitionOperation) key() storageClassTransitionKey {
	return storageClassTransitionKey{
		tableID: operation.TableID,
		target:  operation.targets[0].target,
		startTS: operation.startTS,
	}
}

func (m *storageClassTransitionManager) mergeObserved(operation *storageClassTransitionOperation) {
	if operation.StatusValid {
		return
	}
	m.mu.RLock()
	observed, ok := m.mu.observed[operation.key()]
	m.mu.RUnlock()
	if !ok || !observed.StatusValid || !sameStorageClassTransition(observed, operation.StorageClassTransition) {
		return
	}
	operation.TotalReplicas = observed.TotalReplicas
	operation.CompletedReplicas = observed.CompletedReplicas
	operation.Progress = observed.Progress
	operation.ProgressValid = observed.ProgressValid
	operation.LastUpdateTime = observed.LastUpdateTime
	operation.StatusValid = true
}

func (m *storageClassTransitionManager) recordHistory(ctx context.Context, sctx sessionctx.Context, operation *storageClassTransitionOperation) error {
	duration := operation.FinishTime.Sub(operation.StartTime)
	if duration < 0 {
		duration = 0
	}
	var partitionName any
	var partitionID any
	if operation.PartitionID != 0 {
		partitionName = operation.PartitionName
		partitionID = operation.PartitionID
	}
	var totalReplicas any
	var completedReplicas any
	var progress any
	if operation.StatusValid {
		totalReplicas = operation.TotalReplicas
		completedReplicas = operation.CompletedReplicas
		if operation.ProgressValid {
			progress = operation.Progress
		}
	}
	_, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil,
		`INSERT IGNORE INTO mysql.tidb_storage_class_transition_history
		(table_schema, table_name, table_id, partition_name, partition_id, direction, state,
		 total_replicas, completed_replicas, progress, start_time, finish_time, duration)
		VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`,
		operation.TableSchema, operation.TableName, operation.TableID,
		partitionName, partitionID, operation.Direction, operation.State,
		totalReplicas, completedReplicas, progress,
		operation.StartTime, operation.FinishTime, uint64(duration/time.Second))
	return errors.Trace(err)
}

func (m *storageClassTransitionManager) pruneHistory(ctx context.Context, sctx sessionctx.Context) error {
	value, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBStorageClassTransitionHistorySize)
	if err != nil {
		return errors.Trace(err)
	}
	limit, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return errors.Trace(err)
	}
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
		ctx, nil, "SELECT COUNT(*) FROM mysql.tidb_storage_class_transition_history")
	if err != nil {
		return errors.Trace(err)
	}
	excess := rows[0].GetInt64(0) - limit
	if excess <= 0 {
		return nil
	}
	_, _, err = sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil,
		"DELETE FROM mysql.tidb_storage_class_transition_history ORDER BY finish_time, table_id, start_time, direction LIMIT %?", excess)
	return errors.Trace(err)
}

func (m *storageClassTransitionManager) finalize(sctx sessionctx.Context, operation *storageClassTransitionOperation) error {
	schemaName := firstNonEmpty(operation.currentSchemaName, operation.TableSchema)
	tableName := firstNonEmpty(operation.currentTableName, operation.TableName)
	return m.ddl.executor.UpdateStorageClassTransition(
		sctx,
		operation.schemaID,
		operation.TableID,
		schemaName,
		tableName,
		&model.FinishStorageClassTransitionArgs{
			Action:            model.StorageClassTransitionActionFinalize,
			Target:            operation.targets[0].target,
			StartTS:           operation.startTS,
			FinishTS:          oracle.GoTimeToTS(time.Now()),
			TotalReplicas:     operation.TotalReplicas,
			CompletedReplicas: operation.CompletedReplicas,
		},
	)
}

func (m *storageClassTransitionManager) cleanupHistory(sctx sessionctx.Context, operation *storageClassTransitionOperation) error {
	schemaName := firstNonEmpty(operation.currentSchemaName, operation.TableSchema)
	tableName := firstNonEmpty(operation.currentTableName, operation.TableName)
	return m.ddl.executor.UpdateStorageClassTransition(
		sctx,
		operation.schemaID,
		operation.TableID,
		schemaName,
		tableName,
		&model.FinishStorageClassTransitionArgs{
			Action:  model.StorageClassTransitionActionCleanupHistory,
			Target:  operation.targets[0].target,
			StartTS: operation.startTS,
		},
	)
}

func (m *storageClassTransitionManager) poll(ctx context.Context, sctx sessionctx.Context) error {
	active, pending, err := m.discover()
	if err != nil {
		return errors.Trace(err)
	}
	cleanupFailed := false
	for key, operation := range pending {
		m.mergeObserved(operation)
		if err := m.recordHistory(ctx, sctx, operation); err != nil {
			logutil.DDLLogger().Warn("record storage class transition history failed",
				zap.Int64("tableID", key.tableID), zap.Uint64("startTS", key.startTS), zap.String("target", key.target), zap.Error(err))
			continue
		}
		if err := m.cleanupHistory(sctx, operation); err != nil {
			cleanupFailed = true
			logutil.DDLLogger().Warn("clean storage class transition pending history failed",
				zap.Int64("tableID", key.tableID), zap.Uint64("startTS", key.startTS), zap.String("target", key.target), zap.Error(err))
			continue
		}
		delete(pending, key)
	}

	if len(active) > 0 {
		requestCtx, cancel := context.WithTimeout(ctx, storageClassTransitionRequestTimeout)
		_, tikvStores, err := infosync.GetTiFlashProgressStores(requestCtx)
		cancel()
		if err != nil {
			m.setActive(active, pending)
			return errors.Trace(err)
		}
		for key, operation := range active {
			if !m.ddl.ownerManager.IsOwner() {
				break
			}
			requestCtx, cancel := context.WithTimeout(ctx, storageClassTransitionRequestTimeout)
			complete, err := m.observe(requestCtx, operation, tikvStores)
			cancel()
			if err != nil {
				logutil.DDLLogger().Warn("storage class transition status poll failed",
					zap.Int64("tableID", key.tableID), zap.Uint64("startTS", key.startTS), zap.String("target", key.target), zap.Error(err))
				continue
			}
			if !complete || !m.ddl.ownerManager.IsOwner() {
				continue
			}
			if err := m.finalize(sctx, operation); err != nil {
				logutil.DDLLogger().Warn("finalize storage class transition failed",
					zap.Int64("tableID", key.tableID), zap.Uint64("startTS", key.startTS), zap.String("target", key.target), zap.Error(err))
				continue
			}
			delete(active, key)
		}
	}
	m.setActive(active, pending)
	// Never prune a history row while its durable table marker may still exist.
	// Otherwise a failed cleanup followed by pruning can make the same operation
	// appear active and complete a second time.
	if cleanupFailed {
		return nil
	}
	return m.pruneHistory(ctx, sctx)
}

func (d *ddl) PollStorageClassTransitionRoutine() {
	ticker := time.NewTicker(storageClassTransitionPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
		}
		if !d.ownerManager.IsOwner() {
			d.storageClassTransitionManager.clear()
			continue
		}
		if d.sessPool == nil {
			logutil.DDLLogger().Warn("session pool is unavailable for storage class transition poll")
			continue
		}
		sctx, err := d.sessPool.Get()
		if err != nil {
			logutil.DDLLogger().Warn("get session for storage class transition poll failed", zap.Error(err))
			continue
		}
		ctx := kv.WithInternalSourceType(d.ctx, kv.InternalTxnDDL)
		err = d.storageClassTransitionManager.poll(ctx, sctx)
		d.sessPool.Put(sctx)
		if err != nil {
			logutil.DDLLogger().Warn("storage class transition poll failed", zap.Error(err))
		}
	}
}

func onFinishStorageClassTransition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetFinishStorageClassTransitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if args.StartTS == 0 || (args.Target != model.StorageClassTierIA && args.Target != model.StorageClassTierStandard) {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("invalid storage class transition key target=%q startTS=%d", args.Target, args.StartTS)
	}
	if args.Action == model.StorageClassTransitionActionFinalize &&
		(args.FinishTS == 0 || args.TotalReplicas == 0 || args.CompletedReplicas != args.TotalReplicas) {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf(
			"invalid completed storage class transition finishTS=%d replicas=%d/%d",
			args.FinishTS,
			args.CompletedReplicas,
			args.TotalReplicas,
		)
	}

	key := storageClassTransitionKey{tableID: tblInfo.ID, target: args.Target, startTS: args.StartTS}
	changed := false
	switch args.Action {
	case model.StorageClassTransitionActionFinalize:
		changed = finalizeStorageClassTransition(tblInfo, key, args)
	case model.StorageClassTransitionActionCleanupHistory:
		changed = cleanupPendingStorageClassTransitionHistory(tblInfo, key)
	default:
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("invalid storage class transition action %q", args.Action)
	}
	if changed {
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func finalizeStorageClassTransition(
	tblInfo *model.TableInfo,
	key storageClassTransitionKey,
	args *model.FinishStorageClassTransitionArgs,
) bool {
	state := snapshotStorageClassTransitionState(tblInfo)
	members := make([]physicalStorageClassTransitionState, 0)
	for _, physical := range state.physical {
		physicalKey, active := physical.operationKey(tblInfo.ID)
		if active && physicalKey == key {
			members = append(members, physical)
		}
	}
	if len(members) == 0 {
		return false
	}
	appendPendingStorageClassTransitionHistory(
		tblInfo,
		key,
		members,
		model.StorageClassTransitionStateCompleted,
		args.FinishTS,
		args.TotalReplicas,
		args.CompletedReplicas,
		true,
	)
	for _, member := range members {
		clearStorageClassTransitionMarker(tblInfo, member.physicalID)
	}
	return true
}

func cleanupPendingStorageClassTransitionHistory(tblInfo *model.TableInfo, key storageClassTransitionKey) bool {
	changed := false
	histories := tblInfo.StorageClassTransitionPendingHistory[:0]
	for _, history := range tblInfo.StorageClassTransitionPendingHistory {
		if history.StartTS == key.startTS && normalizedStorageClassTransitionTarget(history.Target) == key.target {
			changed = true
			continue
		}
		histories = append(histories, history)
	}
	tblInfo.StorageClassTransitionPendingHistory = histories
	return changed
}

func clearTableStorageClassTransition(tblInfo *model.TableInfo) {
	tblInfo.StorageClassTransitionTarget = ""
	tblInfo.StorageClassTransitionStartTS = 0
	tblInfo.StorageClassTransitionSchemaName = ""
	tblInfo.StorageClassTransitionTableName = ""
}

func clearPartitionStorageClassTransition(partition *model.PartitionDefinition) {
	partition.StorageClassTransitionTarget = ""
	partition.StorageClassTransitionStartTS = 0
	partition.StorageClassTransitionSchemaName = ""
	partition.StorageClassTransitionTableName = ""
	partition.StorageClassTransitionPartitionName = ""
}

func clearStorageClassTransitionMarker(tblInfo *model.TableInfo, physicalID int64) {
	if physicalID == tblInfo.ID {
		clearTableStorageClassTransition(tblInfo)
		return
	}
	if tblInfo.Partition == nil {
		return
	}
	for i := range tblInfo.Partition.Definitions {
		if tblInfo.Partition.Definitions[i].ID == physicalID {
			clearPartitionStorageClassTransition(&tblInfo.Partition.Definitions[i])
			return
		}
	}
}

// UpdateStorageClassTransition submits an internal DDL to finalize an active
// operation or remove a history record after it is durably copied.
func (e *executor) UpdateStorageClassTransition(
	ctx sessionctx.Context,
	schemaID, tableID int64,
	schemaName, tableName string,
	args *model.FinishStorageClassTransitionArgs,
) error {
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   schemaID,
		TableID:    tableID,
		SchemaName: schemaName,
		TableName:  tableName,
		Type:       model.ActionFinishStorageClassTransition,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: schemaName,
			Table:    tableName,
		}},
	}
	return errors.Trace(e.doDDLJob2(ctx, job, args))
}
