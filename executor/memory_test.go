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
	"flag"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"runtime"
	"strings"
)

var _ = SerialSuites(&testMemoryLeak{})

type testMemoryLeak struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

func (s *testMemoryLeak) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.SetSchemaLease(0)
		session.SetStatsLease(0)
	}
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *testMemoryLeak) TestPBMemoryLeak(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test_mem")
	tk.MustExec("use test_mem")

	// prepare data
	totalSize := uint64(128 << 20) // 128MB
	blockSize := uint64(8 << 10)   // 8KB
	numRows := totalSize / blockSize
	tk.MustExec(fmt.Sprintf("create table t (c varchar(%v))", blockSize))
	defer tk.MustExec("drop table t")
	randStr := strings.Repeat("x", int(blockSize))
	sql := fmt.Sprintf("insert into t values ('%s')", randStr)
	for i := uint64(0); i < numRows; i++ {
		tk.MustExec(sql)
	}

	_, err = se.Execute(context.Background(), "use test_mem")
	c.Assert(err, IsNil)
	runtime.GC()
	allocatedBegin, inUseBegin := s.readMem()
	records, err := se.Execute(context.Background(), "select * from t")
	c.Assert(err, IsNil)
	record := records[0]
	rowCnt := 0
	chk := chunk.RecordBatch{new(chunk.Chunk)}
	for {
		c.Assert(record.Next(context.Background(), &chk), IsNil)
		rowCnt += chk.NumRows()
		if chk.NumRows() == 0 {
			break
		}
	}
	runtime.GC()
	allocatedAfter, inUseAfter := s.readMem()

	fmt.Println(">>> ", allocatedAfter-allocatedBegin, inUseAfter-inUseBegin)

	c.Assert(allocatedAfter-allocatedBegin, GreaterEqual, totalSize)
	fmt.Println(">>>>>>>>>>>>> ", inUseAfter-inUseBegin)

	se.Close()
	runtime.GC()
}

func (s *testMemoryLeak) readMem() (allocated, heapInUse uint64) {
	var stat runtime.MemStats
	runtime.ReadMemStats(&stat)
	return stat.TotalAlloc, stat.HeapInuse
}
