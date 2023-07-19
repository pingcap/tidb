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

package sharedisk

import (
	"bytes"
	"container/heap"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/rand"
	"sort"
	"testing"
	"time"
)

func TestKvPairHeap(t *testing.T) {
	t.Skip("")
	dataSizePerSortedBatch := 100
	batchCnt := 1000
	dataBatch := make([]kvPairHeap, 0, batchCnt)
	dataBatchOffset := make([]int, 0, batchCnt)
	dataBatchTime := make([][]time.Time, 0, batchCnt)
	for i := 0; i < batchCnt; i++ {
		h := make([]*kvPair, 0, dataSizePerSortedBatch)
		for j := 0; j < dataSizePerSortedBatch; j++ {
			h = append(h, &kvPair{key: []byte(rand.String(10000)), value: []byte(rand.String(100)), fileOffset: i})
		}
		sort.Slice(h, func(i, j int) bool {
			return bytes.Compare(h[i].key, h[j].key) < 0
		})
		dataBatch = append(dataBatch, h)
		dataBatchOffset = append(dataBatchOffset, 0)
		dataBatchTime = append(dataBatchTime, make([]time.Time, 0))
	}

	logutil.BgLogger().Info("prepare key done", zap.Any("dataSizePerSortedBatch", dataSizePerSortedBatch), zap.Any("batchCnt", batchCnt))

	ts := time.Now()

	globalHeap := make(kvPairHeap, 0, batchCnt)

	getNextKV := func(pairHeap *kvPairHeap, i int) (kv *kvPair, ok bool) {
		if dataBatchOffset[i] == dataSizePerSortedBatch {
			return nil, false
		}
		if dataBatchOffset[i]%10 == 0 {
			dataBatchTime[i] = append(dataBatchTime[i], time.Now())
		}
		kv = (*pairHeap)[dataBatchOffset[i]]
		dataBatchOffset[i]++
		return kv, true
	}

	for i := 0; i < batchCnt; i++ {
		kv, _ := getNextKV(&dataBatch[i], i)
		globalHeap = append(globalHeap, kv)
		dataBatchTime[i] = append(dataBatchTime[i], time.Now())
	}
	heap.Init(&globalHeap)

	times := 0
	//var preKey []byte
	for globalHeap.Len() > 0 {
		times++
		kv := heap.Pop(&globalHeap)
		//if len(preKey) > 0 {
		//	require.Truef(t, bytes.Compare(preKey, kv.(*kvPair).key) <= 0, fmt.Sprintf("time %d", times))
		//}
		//preKey = kv.(*kvPair).key

		newKV, ok := getNextKV(&dataBatch[kv.(*kvPair).fileOffset], kv.(*kvPair).fileOffset)
		if ok {
			heap.Push(&globalHeap, newKV)
		}
	}
	for i := 0; i < batchCnt; i++ {
		var d time.Duration
		for j := range dataBatchTime[i] {
			if j != 0 {
				d += dataBatchTime[i][j].Sub(dataBatchTime[i][j-1])
			}
		}
		logutil.BgLogger().Info("get time", zap.Any("total duration", d.String()))
	}
	logutil.BgLogger().Info("time", zap.Any("elasp", time.Since(ts)), zap.Any("time", times))
}
