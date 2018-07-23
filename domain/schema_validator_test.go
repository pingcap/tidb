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
	"time"

	. "github.com/pingcap/check"
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
	go serverFunc(lease, leaseGrantCh, oracleCh, exit)

	validator := NewSchemaValidator(lease).(*schemaValidator)

	for i := 0; i < 10; i++ {
		delay := time.Duration(100+rand.Intn(900)) * time.Microsecond
		time.Sleep(delay)
		// Reload can run arbitrarily, at any time.
		reload(validator, leaseGrantCh, 0)
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
	c.Assert(valid, Equals, ResultFail)
	validator.Restart()

	// Sleep for a long time, check schema is invalid.
	time.Sleep(lease)
	ts := <-oracleCh
	valid = validator.Check(ts, item.schemaVer, []int64{10})
	c.Assert(valid, Equals, ResultUnknown)

	currVer := reload(validator, leaseGrantCh, 0)
	valid = validator.Check(ts, item.schemaVer, nil)
	c.Assert(valid, Equals, ResultFail)
	valid = validator.Check(ts, item.schemaVer, []int64{0})
	c.Assert(valid, Equals, ResultFail)
	// Check the latest schema version must changed.
	c.Assert(item.schemaVer, Less, validator.latestSchemaVer)

	// Make sure newItem's version is bigger than currVer.
	time.Sleep(lease * 2)
	newItem := <-leaseGrantCh

	// Update current schema version to newItem's version and the delta table IDs is 1, 2, 3.
	validator.Update(ts, currVer, newItem.schemaVer, []int64{1, 2, 3})
	// Make sure the updated table IDs don't be covered with the same schema version.
	validator.Update(ts, newItem.schemaVer, newItem.schemaVer, nil)
	isTablesChanged = validator.isRelatedTablesChanged(currVer, nil)
	c.Assert(isTablesChanged, IsFalse)
	isTablesChanged = validator.isRelatedTablesChanged(currVer, []int64{2})
	c.Assert(isTablesChanged, IsTrue)
	// The current schema version is older than the oldest schema version.
	isTablesChanged = validator.isRelatedTablesChanged(-1, nil)
	c.Assert(isTablesChanged, IsTrue)

	// All schema versions is expired.
	ts = uint64(time.Now().Add(lease).UnixNano())
	valid = validator.Check(ts, newItem.schemaVer, nil)
	c.Assert(valid, Equals, ResultUnknown)

	close(exit)
}

func reload(validator SchemaValidator, leaseGrantCh chan leaseGrantItem, ids ...int64) int64 {
	item := <-leaseGrantCh
	validator.Update(item.leaseGrantTS, item.oldVer, item.schemaVer, ids)
	return item.schemaVer
}

// serverFunc plays the role as a remote server, runs in a separate goroutine.
// It can grant lease and provide timestamp oracle.
// Caller should communicate with it through channel to mock network.
func serverFunc(lease time.Duration, requireLease chan leaseGrantItem, oracleCh chan uint64, exit chan struct{}) {
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
