// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/types"
)

type benchHelper struct {
	ctx sessionctx.Context
}

func (h *benchHelper) init() {
}

func writeTable() {

}

type queryType int8

const (
	//typeInsert type
	typeInsert queryType = iota
	//typeLookUp simple type
	typeLookUp
)

func BenchmarkStatisticCache(b *testing.B) {
	numClients := []int{2, 8, 32, 64}
	tableSizes := []int{20, 100, 1000}
	for _, numClient := range numClients {
		for _, tableSize := range tableSizes {
			benchmarkStatisticCache(b, numClient, tableSize)
		}
	}

}
func getType() queryType {
	rnd := rand.Int() % 100
	if rnd < 25 {
		return typeInsert
	} else {
		return typeLookUp
	}
}

func benchmarkStatisticCache(b *testing.B, numClient int, tableSize int) {
	BaseTestName := "statsCacheTest" + "-numClient" + fmt.Sprintf("%d", numClient) + "-tableSize" + fmt.Sprintf("%d", tableSize)

	colPerTable := 5
	idxPerTable := 5
	maxBucket := 256
	BytesLimit := int64(300000)
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

	RistrettostatsCache, _ := handle.NewStatsCache(BytesLimit, handle.RistrettoStatsCacheType)
	SimplestatsCache, _ := handle.NewStatsCache(BytesLimit, handle.SimpleStatsCacheType)
	queryPerClient := 1000
	//	var wg sync.WaitGroup
	b.Run(BaseTestName+"-Ristretto", func(b *testing.B) {
		var wg sync.WaitGroup

		for i := 0; i < numClient; i++ {
			wg.Add(1)
			go func() {
				defer wg.Add(-1)
				for j := 0; j < queryPerClient; j++ {
					tp := getType()
					switch tp {
					case typeInsert:
						newTable := pesudoTables[rand.Int()%tableSize].Copy()
						RistrettostatsCache.Update([]*statistics.Table{newTable}, nil, 0)
					case typeLookUp:
						RistrettostatsCache.Lookup(rand.Int63() % int64(tableSize))
					}
				}
			}()
		}
		wg.Wait()
	})

	b.Run(BaseTestName+"-Simple", func(b *testing.B) {
		var wg sync.WaitGroup

		for i := 0; i < numClient; i++ {
			wg.Add(1)
			go func() {
				defer wg.Add(-1)
				for j := 0; j < queryPerClient; j++ {
					tp := getType()
					switch tp {
					case typeInsert:
						newTable := pesudoTables[rand.Int()%tableSize].Copy()
						SimplestatsCache.Update([]*statistics.Table{newTable}, nil, 0)
					case typeLookUp:
						SimplestatsCache.Lookup(rand.Int63() % int64(tableSize))
					}
				}
			}()
			wg.Wait()
		}

	})
}

func getRandomTime(r *rand.Rand) types.CoreTime {
	return types.FromDate(r.Intn(2200), r.Intn(10)+1, r.Intn(20)+1,
		r.Intn(12), r.Intn(60), r.Intn(60), r.Intn(1000000))

}
