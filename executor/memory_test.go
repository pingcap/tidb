// Copyright 2019 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"fmt"
	"runtime"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testMemoryLeak{})

type testMemoryLeak struct {
	store  kv.Storage
	domain *domain.Domain
}

func (s *testMemoryLeak) SetUpSuite(c *C) {
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	s.domain, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testMemoryLeak) TearDownSuite(c *C) {
	s.domain.Close()
	c.Assert(s.store.Close(), IsNil)
}

func (s *testMemoryLeak) TestPBMemoryLeak(c *C) {
	c.Skip("too slow")

	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "create database test_mem")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_mem")
	c.Assert(err, IsNil)

	// prepare data
	totalSize := uint64(256 << 20) // 256MB
	blockSize := uint64(8 << 10)   // 8KB
	delta := totalSize / 5
	numRows := totalSize / blockSize
	_, err = se.Execute(context.Background(), fmt.Sprintf("create table t (c varchar(%v))", blockSize))
	c.Assert(err, IsNil)
	defer func() {
		_, err = se.Execute(context.Background(), "drop table t")
		c.Assert(err, IsNil)
	}()
	sql := fmt.Sprintf("insert into t values (space(%v))", blockSize)
	for i := uint64(0); i < numRows; i++ {
		_, err = se.Execute(context.Background(), sql)
		c.Assert(err, IsNil)
	}

	// read data
	runtime.GC()
	allocatedBegin, inUseBegin := s.readMem()
	records, err := se.Execute(context.Background(), "select * from t")
	c.Assert(err, IsNil)
	record := records[0]
	rowCnt := 0
	chk := record.NewChunk()
	for {
		c.Assert(record.Next(context.Background(), chk), IsNil)
		rowCnt += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	c.Assert(rowCnt, Equals, int(numRows))

	// check memory before close
	runtime.GC()
	allocatedAfter, inUseAfter := s.readMem()
	c.Assert(allocatedAfter-allocatedBegin, GreaterEqual, totalSize)
	c.Assert(s.memDiff(inUseAfter, inUseBegin), Less, delta)

	se.Close()
	runtime.GC()
	allocatedFinal, inUseFinal := s.readMem()
	c.Assert(allocatedFinal-allocatedAfter, Less, delta)
	c.Assert(s.memDiff(inUseFinal, inUseAfter), Less, delta)
}

func (s *testMemoryLeak) readMem() (allocated, heapInUse uint64) {
	var stat runtime.MemStats
	runtime.ReadMemStats(&stat)
	return stat.TotalAlloc, stat.HeapInuse
}

func (s *testMemoryLeak) memDiff(m1, m2 uint64) uint64 {
	if m1 > m2 {
		return m1 - m2
	}
	return m2 - m1
}

func (s *testMemoryLeak) TestGlobalMemoryTrackerOnCleanUp(c *C) {
	originMaxConsumed := executor.GlobalMemoryUsageTracker.MaxConsumed()
	defer executor.GlobalMemoryUsageTracker.SetMaxConsumed(originMaxConsumed)
	executor.GlobalMemoryUsageTracker.SetMaxConsumed(0)
	originConsume := executor.GlobalMemoryUsageTracker.BytesConsumed()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")

	// assert insert
	tk.MustExec("insert t (id) values (1)")
	tk.MustExec("insert t (id) values (2)")
	tk.MustExec("insert t (id) values (3)")
	afterConsume := executor.GlobalMemoryUsageTracker.BytesConsumed()
	c.Assert(originConsume, Equals, afterConsume)

	// assert update
	tk.MustExec("update t set id = 4 where id = 1")
	tk.MustExec("update t set id = 5 where id = 2")
	tk.MustExec("update t set id = 6 where id = 3")
	afterConsume = executor.GlobalMemoryUsageTracker.BytesConsumed()
	c.Assert(originConsume, Equals, afterConsume)

	// assert consume happened
	afterMaxConsumed := executor.GlobalMemoryUsageTracker.MaxConsumed()
	c.Assert(afterMaxConsumed, Greater, int64(0))
}
