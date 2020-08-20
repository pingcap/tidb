// Copyright 2020 PingCAP, Inc.
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

package handle_test

// This file contains benchmarks of our expression evaluation.

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

type queryType int8

type statsCacheTestCase struct {
	queryType      [][]queryType
	queryTables    [][]int64
	tables         []*statistics.Table
	memoryLimit    int64
	concurrency    int
	numClient      int
	tableSize      int
	queryPerClient uint64
}

const (
	typeInsert queryType = iota
	typeLookUp
)

func defaultstatsCacheTestCase(numClient int, tableSize int, memoryLimit int64, queryPerClient uint64) *statsCacheTestCase {
	cas := &statsCacheTestCase{
		numClient:      numClient,
		tableSize:      tableSize,
		memoryLimit:    memoryLimit,
		queryPerClient: queryPerClient,
	}

	colPerTable := 5
	idxPerTable := 5
	maxBucket := 256
	pesudoTables := make([]*statistics.Table, 0)
	for i := 0; i < tableSize; i++ {
		newHistColl := statistics.HistColl{
			PhysicalID:     int64(i + 1),
			HavePhysicalID: true,
			Columns:        make(map[int64]*statistics.Column, 0),
			Indices:        make(map[int64]*statistics.Index, 0),
		}
		table := &statistics.Table{
			HistColl: newHistColl,
			Version:  0,
			Name:     "pesudoname",
		}
		for j := 0; j < colPerTable; j++ {
			hist := statistics.NewHistogram(int64(j), 0, 0, 0, types.NewFieldType(mysql.TypeVarchar), 256, 0)
			cms := statistics.NewCMSketch(5, 2048)
			for k := 0; k < maxBucket; k++ {
				lower := types.NewBytesDatum(make([]byte, 0))
				upper := types.NewBytesDatum(make([]byte, 0))
				hist.AppendBucket(&lower, &upper, 0, 0)
			}
			col := &statistics.Column{
				Histogram:  *hist,
				CMSketch:   cms,
				PhysicalID: table.PhysicalID,
			}
			table.Columns[hist.ID] = col
		}
		for j := 0; j < idxPerTable; j++ {
			hist := statistics.NewHistogram(int64(j), 0, 0, 0, types.NewFieldType(mysql.TypeBlob), 256, 0)
			for k := 0; k < maxBucket; k++ {
				lower := types.NewBytesDatum(make([]byte, 0))
				upper := types.NewBytesDatum(make([]byte, 0))
				hist.AppendBucket(&lower, &upper, 0, 0)
			}
			cms := statistics.NewCMSketch(5, 2048)

			col := &statistics.Index{
				Histogram:  *hist,
				CMSketch:   cms,
				PhysicalID: table.PhysicalID,
			}
			table.Indices[hist.ID] = col
		}
		pesudoTables = append(pesudoTables, table)
	}
	cas.tables = pesudoTables
	for j := 0; j < int(cas.numClient); j++ {
		tps := make([]queryType, 0)
		tbls := make([]int64, 0)
		for i := 0; i < int(cas.queryPerClient); i++ {
			tp := getType()
			tblID := rand.Int63() % int64(tableSize)
			tps = append(tps, tp)
			tbls = append(tbls, tblID)
		}
		cas.queryType = append(cas.queryType, tps)
		cas.queryTables = append(cas.queryTables, tbls)
	}
	return cas
}
func BenchmarkStatisticCache(b *testing.B) {
	numClients := []int{2, 8, 16, 32, 64}
	tableSizes := []int{100, 200, 1000}
	memoryLimits := []int64{300000, 3000000}
	for _, numClient := range numClients {
		for _, tableSize := range tableSizes {
			for _, memoryLimit := range memoryLimits {
				data := defaultstatsCacheTestCase(numClient, tableSize, memoryLimit, 100)
				benchmarkStatisticCache(b, data)
			}
		}
	}

}
func getType() queryType {
	rnd := rand.Int() % 100
	if rnd < 10 {
		return typeInsert
	} else {
		return typeLookUp
	}
}
func benchmarkStatisticCacheExc(b *testing.B, data *statsCacheTestCase, statsCache handle.StatsCache) {
	b.StartTimer()
	var wg sync.WaitGroup
	for i := 0; i < data.numClient; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < int(data.queryPerClient); j++ {
				tp := data.queryType[id][j]
				tblID := data.queryTables[id][j]
				switch tp {
				case typeInsert:
					newTable := data.tables[tblID].Copy()
					statsCache.Update([]*statistics.Table{newTable}, nil, 0)
				case typeLookUp:
					statsCache.Lookup(tblID)
				}
			}
		}(i)
	}
	wg.Wait()
	b.StopTimer()
}
func benchmarkStatisticCache(b *testing.B, data *statsCacheTestCase) {
	BaseTestName := "scTest" + "-Client" + fmt.Sprintf("%d", data.numClient) + "-tblSize" + fmt.Sprintf("%d", data.tableSize) + "-mem" + fmt.Sprintf("%d", data.memoryLimit)

	//	var wg sync.WaitGroup

	b.Run(BaseTestName+"-Simple", func(b *testing.B) {
		SimplestatsCache, _ := handle.NewStatsCache(data.memoryLimit, handle.SimpleStatsCacheType)
		benchmarkStatisticCacheExc(b, data, SimplestatsCache)
		SimplestatsCache.Close()
		for i := 0; i < b.N; i++ {
			SimplestatsCache, _ := handle.NewStatsCache(data.memoryLimit, handle.SimpleStatsCacheType)
			benchmarkStatisticCacheExc(b, data, SimplestatsCache)
		}
	})
	b.Run(BaseTestName+"-Ristretto", func(b *testing.B) {
		RistrettostatsCache, _ := handle.NewStatsCache(data.memoryLimit, handle.RistrettoStatsCacheType)
		benchmarkStatisticCacheExc(b, data, RistrettostatsCache)
		RistrettostatsCache.Close()
		for i := 0; i < b.N; i++ {
			RistrettostatsCache, _ := handle.NewStatsCache(data.memoryLimit, handle.RistrettoStatsCacheType)
			benchmarkStatisticCacheExc(b, data, RistrettostatsCache)
			RistrettostatsCache.Close()
		}

	})
}
func newStoreWithStatsBootstrap() (kv.Storage, *domain.Domain, error) {
	clearRW.RLock()
	defer clearRW.RUnlock()
	store, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	session.SetSchemaLease(0)
	domain.RunAutoAnalyze = false
	do, err := session.BootstrapSession(store)
	do.SetStatsUpdating(true)
	return store, do, errors.Trace(err)
}
func (s *testStatsSuite) SetUpStatsSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	s.registerHook()
	var err error
	s.store, s.do, err = newStoreWithStatsBootstrap()
	c.Assert(err, IsNil)
}

func BenchmarkSelectivityWithCachex(b *testing.B) {
	BenchmarkSelectivityWithCache(b)
	BenchmarkSelectivityWithSimpleCache(b)
	BenchmarkSelectivityWithCacheNolimit(b)
}
func BenchmarkSelectivityWithCacheNolimit(b *testing.B) {
	c := &C{}
	s := &testStatsSuite{}
	s.SetUpStatsSuite(c)
	defer s.TearDownSuite(c)
	testKit := testkit.NewTestKit(c, s.store)

	h := s.do.StatsHandle()
	origLease := h.Lease()
	defer func() { h.SetLease(origLease) }()
	s.prepareSelectivity(testKit, c)

	file, err := os.Create("cpu.profile")
	c.Assert(err, IsNil)
	defer file.Close()
	pprof.StartCPUProfile(file)

	b.Run("SelectivityWithCacheNolimit", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 100; j++ {
				id := rand.Int() % 10
				testKit.MustQuery(fmt.Sprintf("select * from t%d where a > 1 and b < 2 and c > 3 and d < 4 and e > 5", id))
			}
			for j := 0; j < 100; j++ {
				id := rand.Int()%10 + 10
				testKit.MustQuery(fmt.Sprintf("select * from t%d where a > 1 and b < 2 and c > 3 and d < 4 and e > 5", id))
			}
		}
		b.ReportAllocs()
	})
	pprof.StopCPUProfile()
}
func BenchmarkSelectivityWithSimpleCache(b *testing.B) {
	c := &C{}
	s := &testStatsSuite{}
	s.SetUpStatsSuite(c)
	defer s.TearDownSuite(c)
	testKit := testkit.NewTestKit(c, s.store)

	h := s.do.StatsHandle()
	limit := int64(51000 * 8 * 10)
	h.SetBytesLimit(limit)
	h.SetSimpleCache()

	origLease := h.Lease()
	defer func() { h.SetLease(origLease) }()
	s.prepareSelectivity(testKit, c)

	file, err := os.Create("cpu.profile")
	c.Assert(err, IsNil)
	defer file.Close()
	pprof.StartCPUProfile(file)

	b.Run("SelectivityWithSimpleCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 100; j++ {
				id := rand.Int() % 10
				testKit.MustQuery(fmt.Sprintf("select * from t%d where a > 1 and b < 2 and c > 3 and d < 4 and e > 5", id))
			}
			for j := 0; j < 100; j++ {
				id := rand.Int()%10 + 10
				testKit.MustQuery(fmt.Sprintf("select * from t%d where a > 1 and b < 2 and c > 3 and d < 4 and e > 5", id))
			}
		}
		b.ReportAllocs()
	})
	pprof.StopCPUProfile()
}
func BenchmarkSelectivityWithCache(b *testing.B) {
	c := &C{}
	s := &testStatsSuite{}
	s.SetUpStatsSuite(c)
	defer s.TearDownSuite(c)
	testKit := testkit.NewTestKit(c, s.store)

	h := s.do.StatsHandle()
	limit := int64(51000 * 8 * 10)
	h.SetBytesLimit(limit)
	origLease := h.Lease()
	defer func() { h.SetLease(origLease) }()
	s.prepareSelectivity(testKit, c)

	file, err := os.Create("cpu.profile")
	c.Assert(err, IsNil)
	defer file.Close()
	pprof.StartCPUProfile(file)

	b.Run("SelectivityWithCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 100; j++ {
				id := rand.Int() % 10
				testKit.MustQuery(fmt.Sprintf("select * from t%d where a > 1 and b < 2 and c > 3 and d < 4 and e > 5", id))
			}
			for j := 0; j < 100; j++ {
				id := rand.Int()%10 + 10
				testKit.MustQuery(fmt.Sprintf("select * from t%d where a > 1 and b < 2 and c > 3 and d < 4 and e > 5", id))
			}
		}
		b.ReportAllocs()
	})
	pprof.StopCPUProfile()
}
func (s *testStatsSuite) prepareSelectivity(testKit *testkit.TestKit, c *C) {
	testKit.MustExec("use test")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("drop table if exists t%d", i))
		//create a table with 5  column,2 index
		testKit.MustExec(fmt.Sprintf("create table t%d(a int primary key, b int, c int, d int, e int, index idx_cd(c, d), index idx_de(d, e))", i))
	}
	for i := 0; i < 20; i++ {
		for j := 1; j <= 5000; j++ {
			sql := fmt.Sprintf("insert into t%d values ", i)
			sql = sql + fmt.Sprintf("(%d,%d,%d,%d,%d)", j, rand.Int()%10, rand.Int()%10, rand.Int()%10, rand.Int()%10)
			testKit.MustExec(sql)
		}
	}

}
