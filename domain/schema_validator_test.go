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
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/testleak"
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
