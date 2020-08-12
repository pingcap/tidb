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
	"sync"
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/types"
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
	numClients := []int{2, 8, 32, 64}
	tableSizes := []int{10, 20, 100}
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
	if rnd < 15 {
		return typeInsert
	} else {
		return typeLookUp
	}
}
func benchmarkStatisticCacheExc(b *testing.B, data *statsCacheTestCase, statsCache handle.StatsCache) {
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
		wg.Wait()
	}
}
func benchmarkStatisticCache(b *testing.B, data *statsCacheTestCase) {
	BaseTestName := "scTest" + "-Client" + fmt.Sprintf("%d", data.numClient) + "-tblSize" + fmt.Sprintf("%d", data.tableSize) + "-mem" + fmt.Sprintf("%d", data.memoryLimit)

	RistrettostatsCache, _ := handle.NewStatsCache(data.memoryLimit, handle.RistrettoStatsCacheType)
	SimplestatsCache, _ := handle.NewStatsCache(data.memoryLimit, handle.SimpleStatsCacheType)
	//	var wg sync.WaitGroup

	b.Run(BaseTestName+"-Simple", func(b *testing.B) {
		b.ResetTimer()
		benchmarkStatisticCacheExc(b, data, SimplestatsCache)
		for i := 0; i < b.N; i++ {
			benchmarkStatisticCacheExc(b, data, SimplestatsCache)
		}
	})
	b.Run(BaseTestName+"-Ristretto", func(b *testing.B) {
		b.ResetTimer()
		benchmarkStatisticCacheExc(b, data, RistrettostatsCache)
		for i := 0; i < b.N; i++ {
			benchmarkStatisticCacheExc(b, data, RistrettostatsCache)
		}

	})
}

func getRandomTime(r *rand.Rand) types.CoreTime {
	return types.FromDate(r.Intn(2200), r.Intn(10)+1, r.Intn(20)+1,
		r.Intn(12), r.Intn(60), r.Intn(60), r.Intn(1000000))

}
