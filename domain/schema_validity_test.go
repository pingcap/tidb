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
)

type leaseItem struct {
	leaseGrantTS uint64
	schemaVer    int64
}

func (*testSuite) TestSchemaValidity(c *C) {
	lease := 2 * time.Millisecond
	leaseCh := make(chan leaseItem)
	oracleCh := make(chan uint64)
	exit := make(chan struct{})
	go serverFunc(lease, leaseCh, oracleCh, exit)

	svi := newSchemaValidityInfo(lease)

	for i := 0; i < 10; i++ {
		delay := time.Duration(time.Duration(100+rand.Intn(900)) * time.Microsecond)
		time.Sleep(delay)
		// reload can run arbitrarily, at any time.
		reload(svi, leaseCh)
	}

	// take a lease, check it's valid.
	item := <-leaseCh
	svi.Update(item.leaseGrantTS, item.schemaVer)
	valid := svi.Check(item.leaseGrantTS, item.schemaVer)
	c.Assert(valid, IsTrue)

	// sleep for a long time, check schema is invalid.
	time.Sleep(lease)
	ts := <-oracleCh
	valid = svi.Check(ts, item.schemaVer)
	c.Assert(valid, IsFalse)

	reload(svi, leaseCh)
	valid = svi.Check(ts, item.schemaVer)
	c.Assert(valid, IsFalse)

	// check the latest schema version must changed.
	c.Assert(item.schemaVer, LessEqual, svi.Latest())

	exit <- struct{}{}
}

func reload(svi SchemaValidityInfo, leaseCh chan leaseItem) {
	item := <-leaseCh
	svi.Update(item.leaseGrantTS, item.schemaVer)
}

func serverFunc(lease time.Duration, requireLease chan leaseItem, oracleCh chan uint64, exit chan struct{}) {
	var version int64
	leaseTS := uint64(time.Now().UnixNano())
	for {
		select {
		case <-time.Tick(lease):
			version++
			leaseTS = uint64(time.Now().UnixNano())
		case requireLease <- leaseItem{
			leaseGrantTS: leaseTS,
			schemaVer:    version,
		}:
		case oracleCh <- uint64(time.Now().UnixNano()):
		case <-exit:
			return
		}
	}
}
