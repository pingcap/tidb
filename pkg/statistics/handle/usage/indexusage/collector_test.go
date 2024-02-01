// Copyright 2023 PingCAP, Inc.
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

package indexusage

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// GetIndexUsageForTest returns the index usage information
func (s *SessionIndexUsageCollector) GetIndexUsageForTest(tableID int64, indexID int64) *Sample {
	info, ok := s.indexUsage[GlobalIndexID{tableID, indexID}]
	if !ok {
		return nil
	}
	return &info
}

func TestGetBucket(t *testing.T) {
	testCases := []struct {
		value  float64
		expect int
	}{
		{0.0, 0},
		{0.005, 1},
		{0.01, 2},
		{0.05, 2},
		{0.1, 3},
		{0.15, 3},
		{0.2, 4},
		{0.4, 4},
		{0.5, 5},
		{0.7, 5},
		{1.0, 6},
	}
	for _, c := range testCases {
		require.Equal(t,
			c.expect, getIndexUsageAccessBucket(c.value))
	}
}

func TestUpdateIndex(t *testing.T) {
	globalCollector := NewCollector()
	globalCollector.StartWorker()
	collector := globalCollector.SpawnSessionCollector()

	// report a normal full scan
	collector.Update(1, 1, NewSample(1, 1, 1, 1))
	usage := collector.GetIndexUsageForTest(1, 1)
	require.Equal(t, uint64(1), usage.QueryTotal)
	require.Equal(t, uint64(1), usage.KvReqTotal)
	require.Equal(t, uint64(1), usage.RowAccessTotal)
	require.Equal(t, [7]uint64{0, 0, 0, 0, 0, 0, 1}, usage.PercentageAccess)

	// report a partial scan
	collector.Update(1, 1, NewSample(10, 10, 5, 50))
	usage = collector.GetIndexUsageForTest(1, 1)
	require.Equal(t, uint64(11), usage.QueryTotal)
	require.Equal(t, uint64(11), usage.KvReqTotal)
	require.Equal(t, uint64(6), usage.RowAccessTotal)
	require.Equal(t, [7]uint64{0, 0, 0, 1, 0, 0, 1}, usage.PercentageAccess)

	// report a 0 total row
	collector.Update(1, 1, NewSample(10, 10, 5, 0))
	usage = collector.GetIndexUsageForTest(1, 1)
	require.Equal(t, uint64(21), usage.QueryTotal)
	require.Equal(t, uint64(21), usage.KvReqTotal)
	require.Equal(t, uint64(11), usage.RowAccessTotal)
	require.Equal(t, [7]uint64{0, 0, 0, 1, 0, 0, 2}, usage.PercentageAccess)
}

type testOp struct {
	info Sample
	idx  GlobalIndexID
}

type testOpGenerator struct {
	tableCount         int64
	indexPerTableCount int64
	maxQueryTotal      uint64
	maxKvReqTotal      uint64
	maxTableTotalRows  uint64
}

func (g *testOpGenerator) generateTestOp() testOp {
	idx := GlobalIndexID{
		rand.Int63() % g.tableCount,
		rand.Int63() % g.indexPerTableCount,
	}
	queryTotal := rand.Uint64() % g.maxQueryTotal
	kvReqTotal := rand.Uint64() % g.maxKvReqTotal
	totalRows := rand.Uint64() % g.maxTableTotalRows
	rowAccess := uint64(0)
	if totalRows > 0 {
		rowAccess = rand.Uint64() % totalRows
	}

	info := NewSample(queryTotal, kvReqTotal, rowAccess, totalRows)
	return testOp{
		info,
		idx,
	}
}

func TestFlushConcurrentIndexCollector(t *testing.T) {
	const sessionCount = 64
	const opPerSess = 100000
	const opCount = opPerSess * sessionCount

	expectCollector := NewCollector()
	expectCollector.StartWorker()
	expectSessionCollector := expectCollector.SpawnSessionCollector()

	opGenerator := &testOpGenerator{
		10, 10, 10000, 10000, 10000,
	}
	ops := make([]testOp, 0, opCount)
	for i := 0; i < opCount; i++ {
		op := opGenerator.generateTestOp()
		ops = append(ops, op)
		expectSessionCollector.Update(op.idx.TableID, op.idx.IndexID, op.info)
	}
	expectSessionCollector.Flush()

	iuc := NewCollector()
	iuc.StartWorker()
	wg := &sync.WaitGroup{}
	for i := 0; i < sessionCount; i++ {
		localOps := ops[i*opPerSess : (i+1)*opPerSess]
		localCollector := iuc.SpawnSessionCollector()
		wg.Add(1)
		go func() {
			for _, op := range localOps {
				localCollector.Update(op.idx.TableID, op.idx.IndexID, op.info)

				// randomly report
				if rand.Int()%4 == 1 {
					localCollector.Report()
				}
			}
			localCollector.Flush()
			wg.Done()
		}()
	}
	wg.Wait()

	expectCollector.Close()
	iuc.Close()
	require.Equal(t, expectCollector.indexUsage, iuc.indexUsage)
}

func benchmarkIndexCollector(b *testing.B, reportPerOp int) {
	b.StopTimer()
	opGenerator := &testOpGenerator{
		10, 10, 10000, 10000, 10000,
	}
	ops := make([]testOp, 0, b.N)
	for i := 0; i < b.N; i++ {
		op := opGenerator.generateTestOp()
		ops = append(ops, op)
	}

	iuc := NewCollector()
	iuc.StartWorker()
	b.StartTimer()

	var i atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		localCollector := iuc.SpawnSessionCollector()
		localCounter := 0

		for pb.Next() {
			op := ops[i.Load()]
			localCollector.Update(op.idx.TableID, op.idx.IndexID, op.info)
			if localCounter%reportPerOp == 0 {
				localCollector.Report()
			}

			localCounter += 1
			i.Add(1)
		}

		localCollector.Flush()
	})
	iuc.Close()

	b.StopTimer()
}

func BenchmarkIndexCollector(b *testing.B) {
	b.Run("Report per 1 op", func(b *testing.B) {
		benchmarkIndexCollector(b, 1)
	})
	b.Run("Report per 4 ops", func(b *testing.B) {
		benchmarkIndexCollector(b, 4)
	})
	b.Run("Report per 8 ops", func(b *testing.B) {
		benchmarkIndexCollector(b, 8)
	})
}

func TestStmtIndexUsageCollector(t *testing.T) {
	iuc := NewCollector()
	iuc.StartWorker()
	defer iuc.Close()
	sessionCollector := iuc.SpawnSessionCollector()

	statementCollector := NewStmtIndexUsageCollector(sessionCollector)
	statementCollector.Update(1, 1, NewSample(10, 0, 0, 0))
	sessionCollector.Flush()
	require.Eventuallyf(t, func() bool {
		return iuc.GetIndexUsage(1, 1) != Sample{}
	}, time.Second, time.Millisecond, "wait for report")
	require.Equal(t, iuc.GetIndexUsage(1, 1).QueryTotal, uint64(1))

	// duplicated index will be ignored
	statementCollector.Update(1, 1, NewSample(10, 0, 0, 0))
	sessionCollector.Flush()
	require.Eventuallyf(t, func() bool {
		iu := iuc.GetIndexUsage(1, 1)
		emptySample := Sample{}
		if iu != emptySample {
			return iu.QueryTotal == 1
		}
		return false
	}, time.Second, time.Millisecond, "wait for report")

	statementCollector.Update(1, 2, NewSample(10, 0, 0, 0))
	sessionCollector.Flush()
	require.Eventuallyf(t, func() bool {
		return iuc.GetIndexUsage(1, 2) != Sample{}
	}, time.Second, time.Millisecond, "wait for report")
	require.Equal(t, iuc.GetIndexUsage(1, 2).QueryTotal, uint64(1))

	// `queryTotal` will be 1, even if it's set 0
	statementCollector.Update(1, 3, NewSample(0, 0, 0, 0))
	sessionCollector.Flush()
	require.Eventuallyf(t, func() bool {
		return iuc.GetIndexUsage(1, 3) != Sample{}
	}, time.Second, time.Millisecond, "wait for report")
	require.Equal(t, iuc.GetIndexUsage(1, 3).QueryTotal, uint64(1))
}
