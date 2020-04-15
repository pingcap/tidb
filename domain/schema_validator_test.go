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
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"math/rand"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/sessionctx/variable"
	"github.com/pingcap/tidb/v4/store/tikv/oracle"
	"github.com/pingcap/tidb/v4/util/testleak"
)

type leaseGrantItem struct {
	leaseGrantTS uint64
	oldVer       int64
	schemaVer    int64
}

func (*testSuite) TestSchemaValidator(c *C) {
	defer testleak.AfterTest(c)()

	lease := 10 * time.Millisecond
	leaseGrantCh := make(chan leaseGrantItem)
	oracleCh := make(chan uint64)
	exit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go serverFunc(lease, leaseGrantCh, oracleCh, exit, &wg)

	validator := NewSchemaValidator(lease).(*schemaValidator)
	c.Assert(validator.IsStarted(), IsTrue)

	for i := 0; i < 10; i++ {
		delay := time.Duration(100+rand.Intn(900)) * time.Microsecond
		time.Sleep(delay)
		// Reload can run arbitrarily, at any time.
		item := <-leaseGrantCh
		validator.Update(item.leaseGrantTS, item.oldVer, item.schemaVer, nil)
	}

	// Take a lease, check it's valid.
	item := <-leaseGrantCh
	validator.Update(item.leaseGrantTS, item.oldVer, item.schemaVer, []int64{10})
	valid := validator.Check(item.leaseGrantTS, item.schemaVer, []int64{10})
	c.Assert(valid, Equals, ResultSucc)

	// Stop the validator, validator's items value is nil.
	validator.Stop()
	c.Assert(validator.IsStarted(), IsFalse)
	isTablesChanged := validator.isRelatedTablesChanged(item.schemaVer, []int64{10})
	c.Assert(isTablesChanged, IsTrue)
	valid = validator.Check(item.leaseGrantTS, item.schemaVer, []int64{10})
	c.Assert(valid, Equals, ResultUnknown)
	validator.Restart()

	// Increase the current time by 2 leases, check schema is invalid.
	ts := uint64(time.Now().Add(2 * lease).UnixNano()) // Make sure that ts has timed out a lease.
	valid = validator.Check(ts, item.schemaVer, []int64{10})
	c.Assert(valid, Equals, ResultUnknown, Commentf("validator latest schema ver %v, time %v, item schema ver %v, ts %v",
		validator.latestSchemaVer, validator.latestSchemaExpire, 0, oracle.GetTimeFromTS(ts)))
	// Make sure newItem's version is greater than item.schema.
	newItem := getGreaterVersionItem(c, lease, leaseGrantCh, item.schemaVer)
	currVer := newItem.schemaVer
	validator.Update(newItem.leaseGrantTS, newItem.oldVer, currVer, nil)
	valid = validator.Check(ts, item.schemaVer, nil)
	c.Assert(valid, Equals, ResultFail, Commentf("currVer %d, newItem %v", currVer, item))
	valid = validator.Check(ts, item.schemaVer, []int64{0})
	c.Assert(valid, Equals, ResultFail, Commentf("currVer %d, newItem %v", currVer, item))
	// Check the latest schema version must changed.
	c.Assert(item.schemaVer, Less, validator.latestSchemaVer)

	// Make sure newItem's version is greater than currVer.
	newItem = getGreaterVersionItem(c, lease, leaseGrantCh, currVer)
	// Update current schema version to newItem's version and the delta table IDs is 1, 2, 3.
	validator.Update(ts, currVer, newItem.schemaVer, []int64{1, 2, 3})
	// Make sure the updated table IDs don't be covered with the same schema version.
	validator.Update(ts, newItem.schemaVer, newItem.schemaVer, nil)
	isTablesChanged = validator.isRelatedTablesChanged(currVer, nil)
	c.Assert(isTablesChanged, IsFalse)
	isTablesChanged = validator.isRelatedTablesChanged(currVer, []int64{2})
	c.Assert(isTablesChanged, IsTrue, Commentf("currVer %d, newItem %v", currVer, newItem))
	// The current schema version is older than the oldest schema version.
	isTablesChanged = validator.isRelatedTablesChanged(-1, nil)
	c.Assert(isTablesChanged, IsTrue, Commentf("currVer %d, newItem %v", currVer, newItem))

	// All schema versions is expired.
	ts = uint64(time.Now().Add(2 * lease).UnixNano())
	valid = validator.Check(ts, newItem.schemaVer, nil)
	c.Assert(valid, Equals, ResultUnknown)

	close(exit)
	wg.Wait()
}

func getGreaterVersionItem(c *C, lease time.Duration, leaseGrantCh chan leaseGrantItem, currVer int64) leaseGrantItem {
	var newItem leaseGrantItem
	for i := 0; i < 10; i++ {
		time.Sleep(lease / 2)
		newItem = <-leaseGrantCh
		if newItem.schemaVer > currVer {
			break
		}
	}
	c.Assert(newItem.schemaVer, Greater, currVer, Commentf("currVer %d, newItem %v", currVer, newItem))

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

func (*testSuite) TestEnqueue(c *C) {
	lease := 10 * time.Millisecond
	originalCnt := variable.GetMaxDeltaSchemaCount()
	defer variable.SetMaxDeltaSchemaCount(originalCnt)

	validator := NewSchemaValidator(lease).(*schemaValidator)
	c.Assert(validator.IsStarted(), IsTrue)
	// maxCnt is 0.
	variable.SetMaxDeltaSchemaCount(0)
	validator.enqueue(1, []int64{11})
	c.Assert(validator.deltaSchemaInfos, HasLen, 0)

	// maxCnt is 10.
	variable.SetMaxDeltaSchemaCount(10)
	ds := []deltaSchemaInfo{
		{0, []int64{1}},
		{1, []int64{1}},
		{2, []int64{1}},
		{3, []int64{2, 2}},
		{4, []int64{2}},
		{5, []int64{1, 4}},
		{6, []int64{1, 4}},
		{7, []int64{3, 1, 3}},
		{8, []int64{1, 2, 3}},
		{9, []int64{1, 2, 3}},
	}
	for _, d := range ds {
		validator.enqueue(d.schemaVersion, d.relatedIDs)
	}
	validator.enqueue(10, []int64{1})
	ret := []deltaSchemaInfo{
		{0, []int64{1}},
		{2, []int64{1}},
		{3, []int64{2, 2}},
		{4, []int64{2}},
		{6, []int64{1, 4}},
		{9, []int64{1, 2, 3}},
		{10, []int64{1}},
	}
	c.Assert(validator.deltaSchemaInfos, DeepEquals, ret)
	// The Items' relatedTableIDs have different order.
	validator.enqueue(11, []int64{1, 2, 3, 4})
	validator.enqueue(12, []int64{4, 1, 2, 3, 1})
	validator.enqueue(13, []int64{4, 1, 3, 2, 5})
	ret[len(ret)-1] = deltaSchemaInfo{13, []int64{4, 1, 3, 2, 5}}
	c.Assert(validator.deltaSchemaInfos, DeepEquals, ret)
	// The length of deltaSchemaInfos is greater then maxCnt.
	validator.enqueue(14, []int64{1})
	validator.enqueue(15, []int64{2})
	validator.enqueue(16, []int64{3})
	validator.enqueue(17, []int64{4})
	ret = append(ret, deltaSchemaInfo{14, []int64{1}})
	ret = append(ret, deltaSchemaInfo{15, []int64{2}})
	ret = append(ret, deltaSchemaInfo{16, []int64{3}})
	ret = append(ret, deltaSchemaInfo{17, []int64{4}})
	c.Assert(validator.deltaSchemaInfos, DeepEquals, ret[1:])
}
