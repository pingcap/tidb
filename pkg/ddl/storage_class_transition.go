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
	"encoding/json"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/chunk"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

const (
	storageClassDirectionToIA             = "TO_IA"
	storageClassDirectionToStandard       = "TO_STANDARD"
	storageClassTransitionStateRunning    = "RUNNING"
	storageClassTransitionStateCompleted  = "COMPLETED"
	storageClassTransitionStateSuperseded = "SUPERSEDED"

	storageClassTransitionPollInterval   = 2 * time.Second
	storageClassTransitionPruneInterval  = time.Minute
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
	TotalReplicas     uint64
	CompletedReplicas uint64
	Progress          float64
	ProgressValid     bool
	StartTime         time.Time
	Duration          time.Duration
	LastUpdateTime    time.Time
	StatusValid       bool
	PhysicalTableIDs  []int64
	startTS           uint64
}

type storageClassTransitionKey struct {
	tableID   int64
	direction string
	startTS   uint64
}

// storageClassTransitionTarget is serialized in the internal bookkeeping
// column physical_targets and is not exposed through SHOW or InfoSchema.
type storageClassTransitionTarget struct {
	PhysicalID    int64  `json:"physical_id"`
	PartitionID   int64  `json:"partition_id,omitempty"`
	PartitionName string `json:"partition_name,omitempty"`
}

type physicalStorageClass struct {
	storageClassTransitionTarget
	tier string
}

type storageClassTransitionOperation struct {
	StorageClassTransition
	target  string
	targets []storageClassTransitionTarget
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

func normalizedStorageClassTransitionTarget(tier string) string {
	if tier == "" {
		return model.StorageClassTierDefault
	}
	return tier
}

func storageClassTransitionDirection(target string) (string, error) {
	switch target {
	case model.StorageClassTierIA:
		return storageClassDirectionToIA, nil
	case model.StorageClassTierStandard:
		return storageClassDirectionToStandard, nil
	default:
		return "", errors.Errorf("invalid storage class transition target %q", target)
	}
}

func storageClassTransitionTargetForDirection(direction string) (string, error) {
	switch direction {
	case storageClassDirectionToIA:
		return model.StorageClassTierIA, nil
	case storageClassDirectionToStandard:
		return model.StorageClassTierStandard, nil
	default:
		return "", errors.Errorf("invalid storage class transition direction %q", direction)
	}
}

func snapshotPhysicalStorageClasses(tblInfo *model.TableInfo) map[int64]physicalStorageClass {
	physical := map[int64]physicalStorageClass{
		tblInfo.ID: {
			storageClassTransitionTarget: storageClassTransitionTarget{PhysicalID: tblInfo.ID},
			tier:                         tblInfo.StorageClassTier,
		},
	}
	if tblInfo.Partition != nil {
		for _, partition := range tblInfo.Partition.Definitions {
			physical[partition.ID] = physicalStorageClass{
				storageClassTransitionTarget: storageClassTransitionTarget{
					PhysicalID:    partition.ID,
					PartitionID:   partition.ID,
					PartitionName: partition.Name.O,
				},
				tier: partition.StorageClassTier,
			}
		}
	}
	return physical
}

func changedStorageClassPhysicalIDs(
	old, current map[int64]physicalStorageClass,
) map[int64]struct{} {
	changed := make(map[int64]struct{})
	for physicalID, currentState := range current {
		oldState, ok := old[physicalID]
		if !ok || normalizedStorageClassTransitionTarget(oldState.tier) == normalizedStorageClassTransitionTarget(currentState.tier) {
			continue
		}
		changed[physicalID] = struct{}{}
	}
	return changed
}

func buildStorageClassTransitionOperations(
	tblInfo *model.TableInfo,
	physicalIDs map[int64]struct{},
	startTS uint64,
	schemaName, tableName string,
) ([]*storageClassTransitionOperation, error) {
	if startTS == 0 {
		return nil, errors.New("storage class transition start TSO is unavailable")
	}
	physical := snapshotPhysicalStorageClasses(tblInfo)
	ids := make([]int64, 0, len(physicalIDs))
	for physicalID := range physicalIDs {
		ids = append(ids, physicalID)
	}
	slices.Sort(ids)

	byTarget := make(map[string]*storageClassTransitionOperation)
	for _, physicalID := range ids {
		state, ok := physical[physicalID]
		if !ok {
			return nil, errors.Errorf("physical table %d is missing from table %d", physicalID, tblInfo.ID)
		}
		target := normalizedStorageClassTransitionTarget(state.tier)
		direction, err := storageClassTransitionDirection(target)
		if err != nil {
			return nil, errors.Annotatef(err, "physical table %d", physicalID)
		}
		operation := byTarget[target]
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
				target: target,
			}
			byTarget[target] = operation
		}
		operation.targets = append(operation.targets, state.storageClassTransitionTarget)
	}

	operations := make([]*storageClassTransitionOperation, 0, len(byTarget))
	for _, operation := range byTarget {
		setStorageClassTransitionTargets(operation)
		operations = append(operations, operation)
	}
	slices.SortFunc(operations, func(a, b *storageClassTransitionOperation) int {
		return cmp.Compare(a.Direction, b.Direction)
	})
	return operations, nil
}

func setStorageClassTransitionTargets(operation *storageClassTransitionOperation) {
	slices.SortFunc(operation.targets, func(a, b storageClassTransitionTarget) int {
		return cmp.Compare(a.PhysicalID, b.PhysicalID)
	})
	operation.PhysicalTableIDs = make([]int64, len(operation.targets))
	for i, target := range operation.targets {
		operation.PhysicalTableIDs[i] = target.PhysicalID
	}
	if len(operation.targets) == 1 && operation.targets[0].PartitionID != 0 {
		operation.PartitionID = operation.targets[0].PartitionID
		operation.PartitionName = operation.targets[0].PartitionName
	}
}

func stageStorageClassTransitions(
	ctx context.Context,
	se *sess.Session,
	tblInfo *model.TableInfo,
	old map[int64]physicalStorageClass,
	startTS uint64,
	schemaName, tableName string,
) error {
	current := snapshotPhysicalStorageClasses(tblInfo)
	changed := changedStorageClassPhysicalIDs(old, current)
	if len(changed) == 0 {
		return nil
	}

	running, err := loadRunningStorageClassTransitionsForTable(ctx, se, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	finishTime := time.Now()
	for _, operation := range running {
		if !storageClassTransitionTouches(operation, changed) {
			continue
		}
		superseded, err := supersedeStorageClassTransition(ctx, se, operation, finishTime)
		if err != nil {
			return errors.Trace(err)
		}
		if !superseded {
			continue
		}
		// Restart every member of a superseded logical operation under the
		// latest start TSO. This prevents one operation from being partly
		// RUNNING and partly SUPERSEDED.
		for _, target := range operation.targets {
			changed[target.PhysicalID] = struct{}{}
		}
	}

	operations, err := buildStorageClassTransitionOperations(tblInfo, changed, startTS, schemaName, tableName)
	if err != nil {
		return errors.Trace(err)
	}
	for _, operation := range operations {
		if err := insertRunningStorageClassTransition(ctx, se, operation); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func storageClassTransitionTouches(
	operation *storageClassTransitionOperation,
	physicalIDs map[int64]struct{},
) bool {
	for _, target := range operation.targets {
		if _, ok := physicalIDs[target.PhysicalID]; ok {
			return true
		}
	}
	return false
}

func supersedeStorageClassTransition(
	ctx context.Context,
	se *sess.Session,
	operation *storageClassTransitionOperation,
	finishTime time.Time,
) (bool, error) {
	duration := finishTime.Sub(operation.StartTime)
	if duration < 0 {
		duration = 0
	}
	_, err := se.Execute(ctx,
		`UPDATE mysql.tidb_storage_class_transition_history
		 SET state = %?, finish_time = %?, duration = %?
		 WHERE table_id = %? AND start_ts = %? AND direction = %? AND state = %?`,
		"supersede-storage-class-transition",
		storageClassTransitionStateSuperseded,
		finishTime,
		uint64(duration/time.Second),
		operation.TableID,
		operation.startTS,
		operation.Direction,
		storageClassTransitionStateRunning,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	return se.GetSessionVars().StmtCtx.AffectedRows() > 0, nil
}

func insertRunningStorageClassTransition(
	ctx context.Context,
	se *sess.Session,
	operation *storageClassTransitionOperation,
) error {
	targets, err := json.Marshal(operation.targets)
	if err != nil {
		return errors.Trace(err)
	}
	var partitionName any
	var partitionID any
	if operation.PartitionID != 0 {
		partitionName = operation.PartitionName
		partitionID = operation.PartitionID
	}
	_, err = se.Execute(ctx,
		`INSERT INTO mysql.tidb_storage_class_transition_history
		 (table_schema, table_name, table_id, partition_name, partition_id, direction, state,
		  start_ts, start_time, physical_targets)
		 VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`,
		"insert-storage-class-transition",
		operation.TableSchema,
		operation.TableName,
		operation.TableID,
		partitionName,
		partitionID,
		operation.Direction,
		storageClassTransitionStateRunning,
		operation.startTS,
		operation.StartTime,
		targets,
	)
	return errors.Trace(err)
}

func loadRunningStorageClassTransitionsForTable(
	ctx context.Context,
	se *sess.Session,
	tableID int64,
) ([]*storageClassTransitionOperation, error) {
	rows, err := se.Execute(ctx,
		`SELECT table_schema, table_name, table_id, direction, start_ts, physical_targets
		 FROM mysql.tidb_storage_class_transition_history
		 WHERE state = %? AND table_id = %?
		 ORDER BY start_ts, direction`,
		"load-table-storage-class-transitions",
		storageClassTransitionStateRunning,
		tableID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return decodeRunningStorageClassTransitions(rows)
}

func loadRunningStorageClassTransitions(
	ctx context.Context,
	se *sess.Session,
) ([]*storageClassTransitionOperation, error) {
	rows, err := se.Execute(ctx,
		`SELECT table_schema, table_name, table_id, direction, start_ts, physical_targets
		 FROM mysql.tidb_storage_class_transition_history
		 WHERE state = %?
		 ORDER BY table_id, start_ts, direction`,
		"load-storage-class-transitions",
		storageClassTransitionStateRunning,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return decodeRunningStorageClassTransitions(rows)
}

func decodeRunningStorageClassTransitions(rows []chunk.Row) ([]*storageClassTransitionOperation, error) {
	operations := make([]*storageClassTransitionOperation, 0, len(rows))
	for _, row := range rows {
		direction := row.GetString(3)
		target, err := storageClassTransitionTargetForDirection(direction)
		if err != nil {
			return nil, errors.Trace(err)
		}
		startTS := row.GetUint64(4)
		if startTS == 0 {
			return nil, errors.Errorf("invalid storage class transition start TSO for table %d", row.GetInt64(2))
		}
		var targets []storageClassTransitionTarget
		if err := json.Unmarshal(row.GetBytes(5), &targets); err != nil {
			return nil, errors.Annotatef(err, "decode storage class transition targets for table %d", row.GetInt64(2))
		}
		if err := validateStorageClassTransitionTargets(targets); err != nil {
			return nil, errors.Annotatef(err, "table %d at start TSO %d", row.GetInt64(2), startTS)
		}

		operation := &storageClassTransitionOperation{
			StorageClassTransition: StorageClassTransition{
				TableSchema: row.GetString(0),
				TableName:   row.GetString(1),
				TableID:     row.GetInt64(2),
				Direction:   direction,
				StartTime:   model.TSConvert2Time(startTS),
				startTS:     startTS,
			},
			target:  target,
			targets: targets,
		}
		setStorageClassTransitionTargets(operation)
		operations = append(operations, operation)
	}
	return operations, nil
}

func validateStorageClassTransitionTargets(targets []storageClassTransitionTarget) error {
	if len(targets) == 0 {
		return errors.New("storage class transition has no physical targets")
	}
	seen := make(map[int64]struct{}, len(targets))
	for _, target := range targets {
		if target.PhysicalID == 0 {
			return errors.New("storage class transition has a zero physical table ID")
		}
		if _, ok := seen[target.PhysicalID]; ok {
			return errors.Errorf("duplicate physical table %d in storage class transition", target.PhysicalID)
		}
		seen[target.PhysicalID] = struct{}{}
	}
	return nil
}

func storageClassTransitionTargetsExist(
	tblInfo *model.TableInfo,
	operation *storageClassTransitionOperation,
) bool {
	physical := snapshotPhysicalStorageClasses(tblInfo)
	for _, target := range operation.targets {
		if _, ok := physical[target.PhysicalID]; !ok {
			return false
		}
	}
	return true
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
		return cmp.Compare(a.Direction, b.Direction)
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

func (m *storageClassTransitionManager) discover(
	ctx context.Context,
	se *sess.Session,
) (map[storageClassTransitionKey]*storageClassTransitionOperation, error) {
	operations, err := loadRunningStorageClassTransitions(ctx, se)
	if err != nil {
		return nil, errors.Trace(err)
	}
	active := make(map[storageClassTransitionKey]*storageClassTransitionOperation, len(operations))
	for _, operation := range operations {
		key := operation.key()
		if _, ok := active[key]; ok {
			return nil, errors.Errorf(
				"duplicate storage class transition for table %d at start TSO %d in direction %s",
				key.tableID,
				key.startTS,
				key.direction,
			)
		}
		active[key] = operation
	}
	return active, nil
}

func sameStorageClassTransition(a, b StorageClassTransition) bool {
	return a.TableID == b.TableID && a.startTS == b.startTS &&
		a.PartitionID == b.PartitionID && a.Direction == b.Direction && a.StartTime.Equal(b.StartTime) &&
		slices.Equal(a.PhysicalTableIDs, b.PhysicalTableIDs)
}

func (m *storageClassTransitionManager) setActive(
	activeOperations map[storageClassTransitionKey]*storageClassTransitionOperation,
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
		if _, ok := activeOperations[key]; !ok {
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
		statuses, err := infosync.CollectStorageClassStatus(ctx, target.PhysicalID, operation.target, tikvStores)
		if err != nil {
			return false, errors.Trace(err)
		}
		for _, status := range statuses {
			if status.Ready > status.Total {
				return false, errors.Errorf("TiKV store %d returned ready %d greater than total %d for physical table %d",
					status.StoreID, status.Ready, status.Total, target.PhysicalID)
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
		tableID:   operation.TableID,
		direction: operation.Direction,
		startTS:   operation.startTS,
	}
}

func completeStorageClassTransition(
	ctx context.Context,
	se *sess.Session,
	operation *storageClassTransitionOperation,
) (bool, error) {
	finishTime := time.Now()
	duration := finishTime.Sub(operation.StartTime)
	if duration < 0 {
		duration = 0
	}
	_, err := se.Execute(ctx,
		`UPDATE mysql.tidb_storage_class_transition_history
		 SET state = %?, total_replicas = %?, completed_replicas = %?, finish_time = %?, duration = %?
		 WHERE table_id = %? AND start_ts = %? AND direction = %? AND state = %?`,
		"complete-storage-class-transition",
		storageClassTransitionStateCompleted,
		operation.TotalReplicas,
		operation.CompletedReplicas,
		finishTime,
		uint64(duration/time.Second),
		operation.TableID,
		operation.startTS,
		operation.Direction,
		storageClassTransitionStateRunning,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	return se.GetSessionVars().StmtCtx.AffectedRows() > 0, nil
}

func pruneStorageClassTransitionHistory(ctx context.Context, se *sess.Session) error {
	//nolint:forbidigo
	value, err := se.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBStorageClassTransitionHistorySize)
	if err != nil {
		return errors.Trace(err)
	}
	limit, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return errors.Trace(err)
	}
	rows, err := se.Execute(ctx,
		`SELECT COUNT(*) FROM mysql.tidb_storage_class_transition_history
		 WHERE state IN (%?, %?)`,
		"count-storage-class-transition-history",
		storageClassTransitionStateCompleted,
		storageClassTransitionStateSuperseded,
	)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) != 1 {
		return errors.Errorf("unexpected storage class transition history count rows: %d", len(rows))
	}
	excess := rows[0].GetInt64(0) - limit
	if excess <= 0 {
		return nil
	}
	_, err = se.Execute(ctx,
		`DELETE FROM mysql.tidb_storage_class_transition_history
		 WHERE state IN (%?, %?)
		 ORDER BY finish_time, table_id, start_ts, direction LIMIT %?`,
		"prune-storage-class-transition-history",
		storageClassTransitionStateCompleted,
		storageClassTransitionStateSuperseded,
		excess,
	)
	return errors.Trace(err)
}

func (m *storageClassTransitionManager) poll(
	ctx context.Context,
	se *sess.Session,
	pruneHistory bool,
) (bool, error) {
	active, err := m.discover(ctx, se)
	if err != nil {
		return false, errors.Trace(err)
	}
	m.setActive(active)

	historyPruneAttempted := false
	if pruneHistory && m.ddl.ownerManager.IsOwner() {
		historyPruneAttempted = true
		if err := pruneStorageClassTransitionHistory(ctx, se); err != nil {
			logutil.DDLLogger().Warn("prune storage class transition history failed", zap.Error(err))
		}
	}
	if len(active) == 0 {
		return historyPruneAttempted, nil
	}

	for key, operation := range active {
		if !m.ddl.ownerManager.IsOwner() {
			break
		}
		tbl, exists := m.ddl.infoCache.GetLatest().TableByID(ctx, operation.TableID)
		if !exists || !storageClassTransitionTargetsExist(tbl.Meta(), operation) {
			if _, err := supersedeStorageClassTransition(ctx, se, operation, time.Now()); err != nil {
				logutil.DDLLogger().Warn("supersede orphaned storage class transition failed",
					zap.Int64("tableID", key.tableID), zap.Uint64("startTS", key.startTS), zap.String("direction", key.direction), zap.Error(err))
				continue
			}
			delete(active, key)
		}
	}
	m.setActive(active)
	if len(active) == 0 || !m.ddl.ownerManager.IsOwner() {
		return historyPruneAttempted, nil
	}

	requestCtx, cancel := context.WithTimeout(ctx, storageClassTransitionRequestTimeout)
	_, tikvStores, err := infosync.GetTiFlashProgressStores(requestCtx)
	cancel()
	if err != nil {
		return historyPruneAttempted, errors.Trace(err)
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
				zap.Int64("tableID", key.tableID), zap.Uint64("startTS", key.startTS), zap.String("direction", key.direction), zap.Error(err))
			continue
		}
		if !complete || !m.ddl.ownerManager.IsOwner() {
			continue
		}
		if _, err := completeStorageClassTransition(ctx, se, operation); err != nil {
			logutil.DDLLogger().Warn("complete storage class transition failed",
				zap.Int64("tableID", key.tableID), zap.Uint64("startTS", key.startTS), zap.String("direction", key.direction), zap.Error(err))
			continue
		}
		// A zero-row update means a newer DDL already superseded this row. In
		// both cases this observation is no longer active.
		delete(active, key)
	}
	m.setActive(active)
	return historyPruneAttempted, nil
}

func (d *ddl) PollStorageClassTransitionRoutine() {
	ticker := time.NewTicker(storageClassTransitionPollInterval)
	defer ticker.Stop()
	nextHistoryPrune := time.Now().Add(storageClassTransitionPruneInterval)
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
		pruneHistory := !time.Now().Before(nextHistoryPrune)
		historyPruneAttempted, err := d.storageClassTransitionManager.poll(ctx, sess.NewSession(sctx), pruneHistory)
		d.sessPool.Put(sctx)
		if historyPruneAttempted {
			nextHistoryPrune = time.Now().Add(storageClassTransitionPruneInterval)
		}
		if err != nil {
			logutil.DDLLogger().Warn("storage class transition poll failed", zap.Error(err))
		}
	}
}
