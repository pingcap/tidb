// Copyright 2016 PingCAP, Inc.
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

package domain

import (
	"slices"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

type checkResult int

const (
	// ResultSucc means schemaValidator's check is passing.
	ResultSucc checkResult = iota
	// ResultFail means schemaValidator's check is fail.
	ResultFail
	// ResultUnknown means schemaValidator doesn't know the check would be success or fail.
	ResultUnknown
)

// SchemaValidator is the interface for checking the validity of schema version.
type SchemaValidator interface {
	// Update the schema validator, add a new item, delete the expired deltaSchemaInfos.
	// The latest schemaVer is valid within leaseGrantTime plus lease duration.
	// Add the changed table IDs to the new schema information,
	// which is produced when the oldSchemaVer is updated to the newSchemaVer.
	Update(leaseGrantTime uint64, oldSchemaVer, newSchemaVer int64, change *transaction.RelatedSchemaChange)
	// Check is it valid for a transaction to use schemaVer and related tables, at timestamp txnTS.
	Check(txnTS uint64, schemaVer int64, relatedPhysicalTableIDs []int64, needCheckSchema bool) (*transaction.RelatedSchemaChange, checkResult)
	// Stop stops checking the valid of transaction.
	Stop()
	// Restart restarts the schema validator after it is stopped.
	Restart()
	// Reset resets SchemaValidator to initial state.
	Reset()
	// IsStarted indicates whether SchemaValidator is started.
	IsStarted() bool
}

type deltaSchemaInfo struct {
	schemaVersion  int64
	relatedIDs     []int64
	relatedActions []uint64
}

type schemaValidator struct {
	isStarted          bool
	mux                sync.RWMutex
	lease              time.Duration
	latestSchemaVer    int64
	restartSchemaVer   int64
	latestInfoSchema   infoschema.InfoSchema
	do                 *Domain
	latestSchemaExpire time.Time
	// deltaSchemaInfos is a queue that maintain the history of changes.
	deltaSchemaInfos []deltaSchemaInfo
}

// NewSchemaValidator returns a SchemaValidator structure.
func NewSchemaValidator(lease time.Duration, do *Domain) SchemaValidator {
	return &schemaValidator{
		isStarted:        true,
		lease:            lease,
		deltaSchemaInfos: make([]deltaSchemaInfo, 0, variable.DefTiDBMaxDeltaSchemaCount),
		do:               do,
	}
}

func (s *schemaValidator) IsStarted() bool {
	s.mux.RLock()
	isStarted := s.isStarted
	s.mux.RUnlock()
	return isStarted
}

func (s *schemaValidator) Stop() {
	logutil.BgLogger().Info("the schema validator stops")
	metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorStop).Inc()
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = false
	s.latestSchemaVer = 0
	s.deltaSchemaInfos = s.deltaSchemaInfos[:0]
}

func (s *schemaValidator) Restart() {
	metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorRestart).Inc()
	logutil.BgLogger().Info("the schema validator restarts")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = true
	if s.do != nil {
		// When this instance reconnects PD, we should record the latest schema version after mustReload(),
		// to prevent write txns using a stale schema version by aborting them before commit.
		// However, the problem still exists for read-only txns.
		s.restartSchemaVer = s.do.InfoSchema().SchemaMetaVersion()
	}
}

func (s *schemaValidator) Reset() {
	metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorReset).Inc()
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = true
	s.latestSchemaVer = 0
	s.deltaSchemaInfos = s.deltaSchemaInfos[:0]
	s.restartSchemaVer = 0
}

func (s *schemaValidator) Update(leaseGrantTS uint64, oldVer, currVer int64, change *transaction.RelatedSchemaChange) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if !s.isStarted {
		logutil.BgLogger().Info("the schema validator stopped before updating")
		return
	}

	// Renew the lease.
	s.latestSchemaVer = currVer
	if s.do != nil {
		s.latestInfoSchema = s.do.InfoSchema()
	}
	leaseGrantTime := oracle.GetTimeFromTS(leaseGrantTS)
	leaseExpire := leaseGrantTime.Add(s.lease - time.Millisecond)
	s.latestSchemaExpire = leaseExpire
	metrics.LeaseExpireTime.Set(float64(leaseExpire.Unix()))

	// Update the schema deltaItem information.
	if currVer != oldVer {
		s.enqueue(currVer, change)
		var tblIDs []int64
		var actionTypes []uint64
		if change != nil {
			tblIDs = change.PhyTblIDS
			actionTypes = change.ActionTypes
		}
		for idx, ac := range actionTypes {
			if ac == uint64(model.ActionUnlockTable) {
				s.do.Store().GetMemCache().Delete(tblIDs[idx])
			}
			if ac == uint64(model.ActionFlashbackCluster) {
				if s.do != nil && s.do.InfoSyncer() != nil && s.do.InfoSyncer().GetSessionManager() != nil {
					s.do.InfoSyncer().GetSessionManager().KillNonFlashbackClusterConn()
				}
			}
		}
		logutil.BgLogger().Debug("update schema validator", zap.Int64("oldVer", oldVer),
			zap.Int64("currVer", currVer), zap.Int64s("changedTableIDs", tblIDs), zap.Uint64s("changedActionTypes", actionTypes))
	}
}

// isRelatedTablesChanged returns the result whether relatedTableIDs is changed
// from usedVer to the latest schema version.
// NOTE, this function should be called under lock!
func (s *schemaValidator) isRelatedTablesChanged(currVer int64, tableIDs []int64) bool {
	if len(s.deltaSchemaInfos) == 0 {
		metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorCacheEmpty).Inc()
		logutil.BgLogger().Info("schema change history is empty", zap.Int64("currVer", currVer))
		return true
	}
	newerDeltas := s.findNewerDeltas(currVer)
	if len(newerDeltas) == len(s.deltaSchemaInfos) {
		metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorCacheMiss).Inc()
		logutil.BgLogger().Info("the schema version is much older than the latest version", zap.Int64("currVer", currVer),
			zap.Int64("latestSchemaVer", s.latestSchemaVer), zap.Reflect("deltas", newerDeltas))
		return true
	}

	changedTblMap := make(map[int64]uint64)
	changedSchemaVers := make([]int64, 0)
	for _, item := range newerDeltas {
		affected := false
		for i, tblID := range item.relatedIDs {
			for _, relatedTblID := range tableIDs {
				if tblID == relatedTblID || relatedTblID == -1 {
					// if actionType >= 64, the value of left shift equals 0, and it will not impact amend txn
					changedTblMap[tblID] |= 1 << item.relatedActions[i]
					affected = true
				}
			}
		}
		if affected {
			changedSchemaVers = append(changedSchemaVers, item.schemaVersion)
		}
	}
	if len(changedTblMap) > 0 {
		tblIDs := make([]int64, 0, len(changedTblMap))
		for id := range changedTblMap {
			tblIDs = append(tblIDs, id)
		}
		slices.Sort(tblIDs)
		logutil.BgLogger().Info("schema of tables in the transaction are changed", zap.Int64s("conflicted table IDs", tblIDs),
			zap.Int64("transaction schema", currVer), zap.Int64s("schema versions that changed the tables", changedSchemaVers))
		return true
	}
	return false
}

func (s *schemaValidator) findNewerDeltas(currVer int64) []deltaSchemaInfo {
	q := s.deltaSchemaInfos
	pos := len(q)
	for i := len(q) - 1; i >= 0 && q[i].schemaVersion > currVer; i-- {
		pos = i
	}
	return q[pos:]
}

// Check checks schema validity, returns true if use schemaVer and related tables at txnTS is legal.
func (s *schemaValidator) Check(txnTS uint64, schemaVer int64, relatedPhysicalTableIDs []int64, needCheckSchema bool) (*transaction.RelatedSchemaChange, checkResult) {
	s.mux.RLock()
	defer s.mux.RUnlock()
	if !s.isStarted {
		logutil.BgLogger().Info("the schema validator stopped before checking")
		return nil, ResultUnknown
	}

	if schemaVer < s.restartSchemaVer {
		logutil.BgLogger().Info("the schema version is too old, TiDB and PD maybe unhealthy after the transaction started",
			zap.Int64("schemaVer", schemaVer))
		return nil, ResultFail
	}
	if s.lease == 0 {
		return nil, ResultSucc
	}

	// Schema changed, result decided by whether related tables change.
	if schemaVer < s.latestSchemaVer {
		// When a transaction executes a DDL and got an error, it should manually call this method to check if it is caused by schema change.
		// And then it will pass a nil for relatedPhysicalTableIDs to indicate just check schema version.
		// When a transaction only contains DML on temporary tables, relatedPhysicalTableIDs is [].
		if relatedPhysicalTableIDs == nil {
			logutil.BgLogger().Info("the related physical table ID is empty", zap.Int64("schemaVer", schemaVer),
				zap.Int64("latestSchemaVer", s.latestSchemaVer))
			return nil, ResultFail
		}

		// When disabling MDL -> enabling MDL, the old transaction's needCheckSchema is true, we need to check it.
		// When enabling MDL -> disabling MDL, the old transaction's needCheckSchema is false, so still need to check it, and variable EnableMDL is false now.
		if needCheckSchema || !variable.EnableMDL.Load() {
			changed := s.isRelatedTablesChanged(schemaVer, relatedPhysicalTableIDs)
			if changed {
				return nil, ResultFail
			}
		}
		return nil, ResultSucc
	}

	// Schema unchanged, maybe success or the schema validator is unavailable.
	t := oracle.GetTimeFromTS(txnTS)
	if t.After(s.latestSchemaExpire) {
		return nil, ResultUnknown
	}
	return nil, ResultSucc
}

func (s *schemaValidator) enqueue(schemaVersion int64, change *transaction.RelatedSchemaChange) {
	maxCnt := int(variable.GetMaxDeltaSchemaCount())
	if maxCnt <= 0 {
		logutil.BgLogger().Info("the schema validator enqueue", zap.Int("delta max count", maxCnt))
		return
	}

	delta := deltaSchemaInfo{schemaVersion, []int64{}, []uint64{}}
	if change != nil {
		delta.relatedIDs = change.PhyTblIDS
		delta.relatedActions = change.ActionTypes
	}
	if len(s.deltaSchemaInfos) == 0 {
		s.deltaSchemaInfos = append(s.deltaSchemaInfos, delta)
		return
	}

	lastOffset := len(s.deltaSchemaInfos) - 1
	// The first item we needn't to merge, because we hope to cover more versions.
	if lastOffset != 0 && containIn(s.deltaSchemaInfos[lastOffset], delta) {
		s.deltaSchemaInfos[lastOffset] = delta
	} else {
		s.deltaSchemaInfos = append(s.deltaSchemaInfos, delta)
	}

	if len(s.deltaSchemaInfos) > maxCnt {
		logutil.BgLogger().Info("the schema validator enqueue, queue is too long",
			zap.Int("delta max count", maxCnt), zap.Int64("remove schema version", s.deltaSchemaInfos[0].schemaVersion))
		s.deltaSchemaInfos = s.deltaSchemaInfos[1:]
	}
}

// containIn is checks if lasteDelta is included in curDelta considering table id and action type.
func containIn(lastDelta, curDelta deltaSchemaInfo) bool {
	if len(lastDelta.relatedIDs) > len(curDelta.relatedIDs) {
		return false
	}

	var isEqual bool
	for i, lastTblID := range lastDelta.relatedIDs {
		isEqual = false
		for j, curTblID := range curDelta.relatedIDs {
			if lastTblID == curTblID && lastDelta.relatedActions[i] == curDelta.relatedActions[j] {
				isEqual = true
				break
			}
		}
		if !isEqual {
			return false
		}
	}

	return true
}
