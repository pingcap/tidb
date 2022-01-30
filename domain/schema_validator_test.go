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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestSchemaValidator(t *testing.T) {
	t.Run("general", subTestSchemaValidatorGeneral)
	t.Run("enqueue", subTestEnqueue)
	t.Run("enqueueActionType", subTestEnqueueActionType)
}

// subTestSchemaValidatorGeneral is batched in TestSchemaValidator
func subTestSchemaValidatorGeneral(t *testing.T) {
	lease := 10 * time.Millisecond
	leaseGrantCh := make(chan leaseGrantItem)
	oracleCh := make(chan uint64)
	exit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go serverFunc(lease, leaseGrantCh, oracleCh, exit, &wg)

	validator := NewSchemaValidator(lease, nil).(*schemaValidator)
	require.True(t, validator.IsStarted())

	for i := 0; i < 10; i++ {
		delay := time.Duration(100+rand.Intn(900)) * time.Microsecond
		time.Sleep(delay)
		// Reload can run arbitrarily, at any time.
		item := <-leaseGrantCh
		validator.Update(item.leaseGrantTS, item.oldVer, item.schemaVer, nil)
	}

	// Take a lease, check it's valid.
	item := <-leaseGrantCh
	validator.Update(
		item.leaseGrantTS,
		item.oldVer,
		item.schemaVer,
		&transaction.RelatedSchemaChange{PhyTblIDS: []int64{10}, ActionTypes: []uint64{10}})
	_, valid := validator.Check(item.leaseGrantTS, item.schemaVer, []int64{10})
	require.Equal(t, ResultSucc, valid)

	// Stop the validator, validator's items value is nil.
	validator.Stop()
	require.False(t, validator.IsStarted())
	_, isTablesChanged := validator.isRelatedTablesChanged(item.schemaVer, []int64{10})
	require.True(t, isTablesChanged)
	_, valid = validator.Check(item.leaseGrantTS, item.schemaVer, []int64{10})
	require.Equal(t, ResultUnknown, valid)
	validator.Restart()

	// Increase the current time by 2 leases, check schema is invalid.
	ts := uint64(time.Now().Add(2 * lease).UnixNano()) // Make sure that ts has timed out a lease.
	_, valid = validator.Check(ts, item.schemaVer, []int64{10})
	require.Equalf(t, ResultUnknown, valid, "validator latest schema ver %v, time %v, item schema ver %v, ts %v", validator.latestSchemaVer, validator.latestSchemaExpire, 0, oracle.GetTimeFromTS(ts))

	// Make sure newItem's version is greater than item.schema.
	newItem := getGreaterVersionItem(t, lease, leaseGrantCh, item.schemaVer)
	currVer := newItem.schemaVer
	validator.Update(newItem.leaseGrantTS, newItem.oldVer, currVer, nil)
	_, valid = validator.Check(ts, item.schemaVer, nil)
	require.Equalf(t, ResultFail, valid, "currVer %d, newItem %v", currVer, item)
	_, valid = validator.Check(ts, item.schemaVer, []int64{0})
	require.Equalf(t, ResultFail, valid, "currVer %d, newItem %v", currVer, item)

	// Check the latest schema version must changed.
	require.Less(t, item.schemaVer, validator.latestSchemaVer)

	// Make sure newItem's version is greater than currVer.
	newItem = getGreaterVersionItem(t, lease, leaseGrantCh, currVer)
	// Update current schema version to newItem's version and the delta table IDs is 1, 2, 3.
	validator.Update(ts, currVer, newItem.schemaVer, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{1, 2, 3}, ActionTypes: []uint64{1, 2, 3}})
	// Make sure the updated table IDs don't be covered with the same schema version.
	validator.Update(ts, newItem.schemaVer, newItem.schemaVer, nil)
	_, isTablesChanged = validator.isRelatedTablesChanged(currVer, nil)
	require.False(t, isTablesChanged)
	_, isTablesChanged = validator.isRelatedTablesChanged(currVer, []int64{2})
	require.Truef(t, isTablesChanged, "currVer %d, newItem %v", currVer, newItem)
	// The current schema version is older than the oldest schema version.
	_, isTablesChanged = validator.isRelatedTablesChanged(-1, nil)
	require.Truef(t, isTablesChanged, "currVer %d, newItem %v", currVer, newItem)

	// All schema versions is expired.
	ts = uint64(time.Now().Add(2 * lease).UnixNano())
	_, valid = validator.Check(ts, newItem.schemaVer, nil)
	require.Equal(t, ResultUnknown, valid)

	close(exit)
	wg.Wait()
}

// subTestEnqueue is batched in TestSchemaValidator
func subTestEnqueue(t *testing.T) {
	lease := 10 * time.Millisecond
	originalCnt := variable.GetMaxDeltaSchemaCount()
	defer variable.SetMaxDeltaSchemaCount(originalCnt)

	validator := NewSchemaValidator(lease, nil).(*schemaValidator)
	require.True(t, validator.IsStarted())

	// maxCnt is 0.
	variable.SetMaxDeltaSchemaCount(0)
	validator.enqueue(1, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{11}, ActionTypes: []uint64{11}})
	require.Len(t, validator.deltaSchemaInfos, 0)

	// maxCnt is 10.
	variable.SetMaxDeltaSchemaCount(10)
	ds := []deltaSchemaInfo{
		{0, []int64{1}, []uint64{1}},
		{1, []int64{1}, []uint64{1}},
		{2, []int64{1}, []uint64{1}},
		{3, []int64{2, 2}, []uint64{2, 2}},
		{4, []int64{2}, []uint64{2}},
		{5, []int64{1, 4}, []uint64{1, 4}},
		{6, []int64{1, 4}, []uint64{1, 4}},
		{7, []int64{3, 1, 3}, []uint64{3, 1, 3}},
		{8, []int64{1, 2, 3}, []uint64{1, 2, 3}},
		{9, []int64{1, 2, 3}, []uint64{1, 2, 3}},
	}
	for _, d := range ds {
		validator.enqueue(d.schemaVersion, &transaction.RelatedSchemaChange{PhyTblIDS: d.relatedIDs, ActionTypes: d.relatedActions})
	}
	validator.enqueue(10, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{1}, ActionTypes: []uint64{1}})
	ret := []deltaSchemaInfo{
		{0, []int64{1}, []uint64{1}},
		{2, []int64{1}, []uint64{1}},
		{3, []int64{2, 2}, []uint64{2, 2}},
		{4, []int64{2}, []uint64{2}},
		{6, []int64{1, 4}, []uint64{1, 4}},
		{9, []int64{1, 2, 3}, []uint64{1, 2, 3}},
		{10, []int64{1}, []uint64{1}},
	}
	require.Equal(t, ret, validator.deltaSchemaInfos)
	// The Items' relatedTableIDs have different order.
	validator.enqueue(11, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{1, 2, 3, 4}, ActionTypes: []uint64{1, 2, 3, 4}})
	validator.enqueue(12, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{4, 1, 2, 3, 1}, ActionTypes: []uint64{4, 1, 2, 3, 1}})
	validator.enqueue(13, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{4, 1, 3, 2, 5}, ActionTypes: []uint64{4, 1, 3, 2, 5}})
	ret[len(ret)-1] = deltaSchemaInfo{13, []int64{4, 1, 3, 2, 5}, []uint64{4, 1, 3, 2, 5}}
	require.Equal(t, ret, validator.deltaSchemaInfos)
	// The length of deltaSchemaInfos is greater then maxCnt.
	validator.enqueue(14, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{1}, ActionTypes: []uint64{1}})
	validator.enqueue(15, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{2}, ActionTypes: []uint64{2}})
	validator.enqueue(16, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{3}, ActionTypes: []uint64{3}})
	validator.enqueue(17, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{4}, ActionTypes: []uint64{4}})
	ret = append(ret, deltaSchemaInfo{14, []int64{1}, []uint64{1}})
	ret = append(ret, deltaSchemaInfo{15, []int64{2}, []uint64{2}})
	ret = append(ret, deltaSchemaInfo{16, []int64{3}, []uint64{3}})
	ret = append(ret, deltaSchemaInfo{17, []int64{4}, []uint64{4}})
	require.Equal(t, ret[1:], validator.deltaSchemaInfos)
}

// subTestEnqueueActionType is batched in TestSchemaValidator
func subTestEnqueueActionType(t *testing.T) {
	lease := 10 * time.Millisecond
	originalCnt := variable.GetMaxDeltaSchemaCount()
	defer variable.SetMaxDeltaSchemaCount(originalCnt)

	validator := NewSchemaValidator(lease, nil).(*schemaValidator)
	require.True(t, validator.IsStarted())

	// maxCnt is 0.
	variable.SetMaxDeltaSchemaCount(0)
	validator.enqueue(1, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{11}, ActionTypes: []uint64{11}})
	require.Len(t, validator.deltaSchemaInfos, 0)

	// maxCnt is 10.
	variable.SetMaxDeltaSchemaCount(10)
	ds := []deltaSchemaInfo{
		{0, []int64{1}, []uint64{1}},
		{1, []int64{1}, []uint64{1}},
		{2, []int64{1}, []uint64{1}},
		{3, []int64{2, 2}, []uint64{2, 2}},
		{4, []int64{2}, []uint64{2}},
		{5, []int64{1, 4}, []uint64{1, 4}},
		{6, []int64{1, 4}, []uint64{1, 4}},
		{7, []int64{3, 1, 3}, []uint64{3, 1, 3}},
		{8, []int64{1, 2, 3}, []uint64{1, 2, 3}},
		{9, []int64{1, 2, 3}, []uint64{1, 2, 4}},
	}
	for _, d := range ds {
		validator.enqueue(d.schemaVersion, &transaction.RelatedSchemaChange{PhyTblIDS: d.relatedIDs, ActionTypes: d.relatedActions})
	}
	validator.enqueue(10, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{1}, ActionTypes: []uint64{15}})
	ret := []deltaSchemaInfo{
		{0, []int64{1}, []uint64{1}},
		{2, []int64{1}, []uint64{1}},
		{3, []int64{2, 2}, []uint64{2, 2}},
		{4, []int64{2}, []uint64{2}},
		{6, []int64{1, 4}, []uint64{1, 4}},
		{8, []int64{1, 2, 3}, []uint64{1, 2, 3}},
		{9, []int64{1, 2, 3}, []uint64{1, 2, 4}},
		{10, []int64{1}, []uint64{15}},
	}
	require.Equal(t, ret, validator.deltaSchemaInfos)

	// Check the flag set by schema diff, note tableID = 3 has been set flag 0x3 in schema version 9, and flag 0x4
	// in schema version 10, so the resActions for tableID = 3 should be 0x3 & 0x4 = 0x7.
	relatedChanges, isTablesChanged := validator.isRelatedTablesChanged(5, []int64{1, 2, 3, 4})
	require.True(t, isTablesChanged)
	require.Equal(t, []int64{1, 2, 3, 4}, relatedChanges.PhyTblIDS)
	require.Equal(t, []uint64{15, 2, 7, 4}, relatedChanges.ActionTypes)
}

type leaseGrantItem struct {
	leaseGrantTS uint64
	oldVer       int64
	schemaVer    int64
}

func getGreaterVersionItem(t *testing.T, lease time.Duration, leaseGrantCh chan leaseGrantItem, currVer int64) leaseGrantItem {
	var newItem leaseGrantItem
	for i := 0; i < 10; i++ {
		time.Sleep(lease / 2)
		newItem = <-leaseGrantCh
		if newItem.schemaVer > currVer {
			break
		}
	}
	require.Greaterf(t, newItem.schemaVer, currVer, "currVer %d, newItem %v", currVer, newItem)
	return newItem
}

// serverFunc plays the role as a remote server, runs in a separate goroutine.
// It can grant lease and provide timestamp oracle.
// Caller should communicate with it through channel to mock network.
func serverFunc(lease time.Duration, requireLease chan leaseGrantItem, oracleCh chan uint64, exit chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var version int64
	leaseTS := uint64(time.Now().UnixNano())
	ticker := time.NewTicker(lease)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			version++
			leaseTS = uint64(now.UnixNano())
		case requireLease <- leaseGrantItem{
			leaseGrantTS: leaseTS,
			oldVer:       version - 1,
			schemaVer:    version,
		}:
		case oracleCh <- uint64(time.Now().UnixNano()):
		case <-exit:
			return
		}
	}
}
