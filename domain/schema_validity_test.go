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
	lease := 5 * time.Millisecond
	leaseCh := make(chan leaseItem)
	go serverFunc(lease, leaseCh)

	svi := newSchemaValidityInfo(lease)

	for i := 0; i < 10; i++ {
		delay := time.Duration(time.Duration(100+rand.Intn(900)) * time.Microsecond)
		time.Sleep(delay)
		// reload can run arbitrarily, at any time.
		reload(svi, leaseCh)
	}

	// take a lease, check it's valid.
	item := <-leaseCh
	valid := svi.Check(item.leaseGrantTS, item.schemaVer)
	c.Assert(valid, IsTrue)

	// sleep for a while, check it's still valid.
	time.Sleep(time.Millisecond)
	reload(svi, leaseCh)
	valid = svi.Check(item.leaseGrantTS, item.schemaVer)
	c.Assert(valid, IsTrue)

	// sleep for a long time, check it's invalid.
	time.Sleep(lease)
	reload(svi, leaseCh)
	valid = svi.Check(item.leaseGrantTS, item.schemaVer)
	c.Assert(valid, IsFalse)

	// check the latest schema version must changed.
	c.Assert(item.schemaVer, LessEqual, svi.Latest())
}

func reload(svi SchemaValidityInfo, leaseCh chan leaseItem) {
	item := <-leaseCh
	svi.Update(item.leaseGrantTS, item.schemaVer)
}

func serverFunc(lease time.Duration, requireLease chan leaseItem) {
	var version int64
	for {
		select {
		case <-time.Tick(lease):
			version++
		case requireLease <- leaseItem{
			leaseGrantTS: uint64(time.Now().UnixNano()),
			schemaVer:    version,
		}:
		}
	}
}
