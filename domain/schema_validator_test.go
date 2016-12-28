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
	schemaVer    int64
}

func (*testSuite) TestSchemaValidator(c *C) {
	defer testleak.AfterTest(c)()
	lease := 2 * time.Millisecond
	leaseGrantCh := make(chan leaseGrantItem)
	oracleCh := make(chan uint64)
	exit := make(chan struct{})
	go serverFunc(lease, leaseGrantCh, oracleCh, exit)

	validator := newSchemaValidator(lease)

	for i := 0; i < 10; i++ {
		delay := time.Duration(100+rand.Intn(900)) * time.Microsecond
		time.Sleep(delay)
		// Reload can run arbitrarily, at any time.
		reload(validator, leaseGrantCh)
	}

	// Take a lease, check it's valid.
	item := <-leaseGrantCh
	validator.Update(item.leaseGrantTS, item.schemaVer)
	valid := validator.Check(item.leaseGrantTS, item.schemaVer)
	c.Assert(valid, IsTrue)

	// Sleep for a long time, check schema is invalid.
	time.Sleep(lease)
	ts := <-oracleCh
	valid = validator.Check(ts, item.schemaVer)
	c.Assert(valid, IsFalse)

	reload(validator, leaseGrantCh)
	valid = validator.Check(ts, item.schemaVer)
	c.Assert(valid, IsFalse)

	// Check the latest schema version must changed.
	c.Assert(item.schemaVer, Less, validator.Latest())

	exit <- struct{}{}
}

func reload(validator SchemaValidator, leaseGrantCh chan leaseGrantItem) {
	item := <-leaseGrantCh
	validator.Update(item.leaseGrantTS, item.schemaVer)
}

// serverFunc plays the role as a remote server, runs in a seperate goroutine.
// It can grant lease and provide timestamp oracle.
// Caller should communicate with it through channel to mock network.
func serverFunc(lease time.Duration, requireLease chan leaseGrantItem, oracleCh chan uint64, exit chan struct{}) {
	var version int64
	leaseTS := uint64(time.Now().UnixNano())
	ticker := time.NewTicker(lease)
	for {
		select {
		case <-ticker.C:
			version++
			leaseTS = uint64(time.Now().UnixNano())
		case requireLease <- leaseGrantItem{
			leaseGrantTS: leaseTS,
			schemaVer:    version,
		}:
		case oracleCh <- uint64(time.Now().UnixNano()):
		case <-exit:
			return
		}
	}
}
