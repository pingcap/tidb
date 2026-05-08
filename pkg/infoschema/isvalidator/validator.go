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

package isvalidator

import (
	"slices"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

type deltaSchemaInfo struct {
	schemaVersion  int64
	relatedIDs     []int64
	relatedActions []uint64
}

type validator struct {
	isStarted          bool
	mux                sync.RWMutex
	lease              time.Duration
	latestSchemaVer    int64
	restartSchemaVer   int64
	latestSchemaExpire time.Time
	// deltaSchemaInfos is a queue that maintain the history of changes.
	// it's ordered by schema version in ascending order.
	deltaSchemaInfos []deltaSchemaInfo
}

// New returns a Validator structure.
func New(lease time.Duration) validatorapi.Validator {
	intest.Assert(lease > 0, "lease should be greater than 0")
	return &validator{
		isStarted:        true,
		lease:            lease,
		deltaSchemaInfos: make([]deltaSchemaInfo, 0, vardef.DefTiDBMaxDeltaSchemaCount),
	}
}

func (v *validator) IsStarted() bool {
	v.mux.RLock()
	isStarted := v.isStarted
	v.mux.RUnlock()
	return isStarted
}

func (v *validator) Stop() {
	logutil.BgLogger().Info("the schema validator stops")
	metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorStop).Inc()
	v.mux.Lock()
	defer v.mux.Unlock()
	v.isStarted = false
	v.latestSchemaVer = 0
	v.deltaSchemaInfos = v.deltaSchemaInfos[:0]
}

func (v *validator) Restart(currSchemaVer int64) {
	metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorRestart).Inc()
	logutil.BgLogger().Info("the schema validator restarts")
	v.mux.Lock()
	defer v.mux.Unlock()
	v.isStarted = true
	// When this instance reconnects PD, we should record the latest schema version after mustReload(),
	// to prevent write txns using a stale schema version by aborting them before commit.
	// However, the problem still exists for read-only txns.
	v.restartSchemaVer = currSchemaVer
}

func (v *validator) Reset() {
	metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorReset).Inc()
	v.mux.Lock()
	defer v.mux.Unlock()
	v.isStarted = true
	v.latestSchemaVer = 0
	v.deltaSchemaInfos = v.deltaSchemaInfos[:0]
	v.restartSchemaVer = 0
}

func (v *validator) Update(leaseGrantTS uint64, oldVer, currVer int64, change *transaction.RelatedSchemaChange) {
	v.mux.Lock()
	defer v.mux.Unlock()

	if !v.isStarted {
		logutil.BgLogger().Info("the schema validator stopped before updating")
		return
	}

	// Renew the lease.
	v.latestSchemaVer = currVer
	leaseGrantTime := oracle.GetTimeFromTS(leaseGrantTS)
	leaseExpire := leaseGrantTime.Add(v.lease - time.Millisecond)
	v.latestSchemaExpire = leaseExpire
	metrics.LeaseExpireTime.Set(float64(leaseExpire.Unix()))

	// Update the schema deltaItem information.
	if currVer != oldVer {
		v.enqueue(currVer, change)
		var tblIDs []int64
		var actionTypes []uint64
		if change != nil {
			tblIDs = change.PhyTblIDS
			actionTypes = change.ActionTypes
		}
		logutil.BgLogger().Debug("update schema validator", zap.Int64("oldVer", oldVer),
			zap.Int64("currVer", currVer), zap.Int64s("changedTableIDs", tblIDs), zap.Uint64s("changedActionTypes", actionTypes))
	}
}

func (v *validator) IsLeaseExpired() bool {
	v.mux.Lock()
	defer v.mux.Unlock()
	return time.Now().After(v.latestSchemaExpire)
}

// isRelatedTablesChanged returns the result whether relatedTableIDs is changed
// from usedVer to the latest schema version.
// NOTE, this function should be called under lock!
func (v *validator) isRelatedTablesChanged(currVer int64, tableIDs []int64) bool {
	if len(v.deltaSchemaInfos) == 0 {
		metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorCacheEmpty).Inc()
		logutil.BgLogger().Info("schema change history is empty", zap.Int64("currVer", currVer))
		return true
	}
	newerDeltas := v.findNewerDeltas(currVer)
	if len(newerDeltas) == len(v.deltaSchemaInfos) {
		// we only save the latest N schema changes, so if we enter this branch,
		// we cannot determine whether the table is changed or not, as the delta
		// might be evicted.
		metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorCacheMiss).Inc()
		logutil.BgLogger().Info("the schema version is much older than the latest version", zap.Int64("currVer", currVer),
			zap.Int64("latestSchemaVer", v.latestSchemaVer), zap.Reflect("deltas", newerDeltas))
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

func (v *validator) findNewerDeltas(currVer int64) []deltaSchemaInfo {
	q := v.deltaSchemaInfos
	pos := len(q)
	for i := len(q) - 1; i >= 0 && q[i].schemaVersion > currVer; i-- {
		pos = i
	}
	return q[pos:]
}

// Check checks schema validity, returns true if it's safe to commit using the
// schemaVer and related tables at txnTS.
func (v *validator) Check(txnTS uint64, schemaVer int64, relatedPhysicalTableIDs []int64, needCheckSchemaByDelta bool) (*transaction.RelatedSchemaChange, validatorapi.Result) {
	v.mux.RLock()
	defer v.mux.RUnlock()
	if !v.isStarted {
		logutil.BgLogger().Info("the schema validator stopped before checking")
		return nil, validatorapi.ResultUnknown
	}

	if schemaVer < v.restartSchemaVer {
		logutil.BgLogger().Info("the schema version is too old, TiDB and PD maybe unhealthy after the transaction started",
			zap.Int64("schemaVer", schemaVer))
		return nil, validatorapi.ResultFail
	}

	// Schema changed, result decided by whether related tables change.
	if schemaVer < v.latestSchemaVer {
		// When a transaction executes a DDL and got an error, it should manually
		// call this method to check if it is caused by schema change.
		// And then it will pass a nil for relatedPhysicalTableIDs to indicate
		// just check schema version.
		// When a transaction only contains DML on temporary tables,
		// relatedPhysicalTableIDs is also [].
		if relatedPhysicalTableIDs == nil {
			logutil.BgLogger().Info("the related physical table ID is empty", zap.Int64("schemaVer", schemaVer),
				zap.Int64("latestSchemaVer", v.latestSchemaVer))
			return nil, validatorapi.ResultFail
		}

		// needCheckSchemaByDelta is always false when MDL enabled, and vice
		// versa.
		// when MDL is enabled, if there are DDL running for the related tables,
		// DDL will wait the txn to finishes before move to next step, so there
		// is no need to check through schema delta.
		//
		// below EnableMDL check is for the case that MDL is switching to on/off
		// during the txn.
		// When switch MDL from off to on, needCheckSchemaByDelta of the old txn
		// is true, we need to check by schema delta.
		// When switch MDL from on to off, the needCheckSchemaByDelta is false,
		// and EnableMDL is also false, so we still need to check by schema delta.
		if needCheckSchemaByDelta || !vardef.IsMDLEnabled() {
			changed := v.isRelatedTablesChanged(schemaVer, relatedPhysicalTableIDs)
			if changed {
				return nil, validatorapi.ResultFail
			}
		}
		return nil, validatorapi.ResultSucc
	}

	// Schema unchanged, maybe success or the schema validator is unavailable.
	t := oracle.GetTimeFromTS(txnTS)
	if t.After(v.latestSchemaExpire) {
		return nil, validatorapi.ResultUnknown
	}
	return nil, validatorapi.ResultSucc
}

func (v *validator) enqueue(schemaVersion int64, change *transaction.RelatedSchemaChange) {
	maxCnt := int(vardef.GetMaxDeltaSchemaCount())
	if maxCnt <= 0 {
		logutil.BgLogger().Info("the schema validator enqueue", zap.Int("delta max count", maxCnt))
		return
	}

	delta := deltaSchemaInfo{schemaVersion, []int64{}, []uint64{}}
	if change != nil {
		delta.relatedIDs = change.PhyTblIDS
		delta.relatedActions = change.ActionTypes
	}
	if len(v.deltaSchemaInfos) == 0 {
		v.deltaSchemaInfos = append(v.deltaSchemaInfos, delta)
		return
	}

	lastOffset := len(v.deltaSchemaInfos) - 1
	// The first item we needn't to merge, because we hope to cover more versions.
	if lastOffset != 0 && containIn(v.deltaSchemaInfos[lastOffset], delta) {
		v.deltaSchemaInfos[lastOffset] = delta
	} else {
		v.deltaSchemaInfos = append(v.deltaSchemaInfos, delta)
	}

	if len(v.deltaSchemaInfos) > maxCnt {
		logutil.BgLogger().Info("the schema validator enqueue, queue is too long",
			zap.Int("delta max count", maxCnt), zap.Int64("remove schema version", v.deltaSchemaInfos[0].schemaVersion))
		v.deltaSchemaInfos = v.deltaSchemaInfos[1:]
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
