// Copyright 2021 PingCAP, Inc.

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

package txninfo

import (
	"container/list"
	"encoding/json"
	"hash/fnv"
	"sync"
	"time"

	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/v2/oracle"
)

func digest(digests []string) uint32 {
	hash := fnv.New32a()
	for _, digest := range digests {
		hash.Write([]byte(digest))
	}
	return hash.Sum32()
}

type trxSummaryEntry struct {
	trxDigest uint32
	digests   []string
}

type trxSummaries struct {
	capacity uint
	size     uint

	// digest -> trxSummaryEntry
	elements map[uint32]*list.Element
	cache    *list.List
}

func newTrxSummaries(capacity uint) trxSummaries {
	return trxSummaries{
		capacity: capacity,
		size:     0,
		cache:    list.New(),
		elements: make(map[uint32]*list.Element),
	}
}

func (s *trxSummaries) onTrxEnd(digests []string) {
	key := digest(digests)
	element, exists := s.elements[key]
	if exists {
		s.cache.MoveToFront(element)
		return
	}
	e := trxSummaryEntry{
		trxDigest: key,
		digests:   digests,
	}
	s.elements[key] = s.cache.PushFront(e)
	if s.size == s.capacity {
		last := s.cache.Back()
		delete(s.elements, last.Value.(trxSummaryEntry).trxDigest)
		s.cache.Remove(last)
	} else {
		s.size++
	}
}

func (s *trxSummaries) dumpTrxSummary() [][]types.Datum {
	var result [][]types.Datum
	for ele := s.cache.Front(); ele != nil; ele = ele.Next() {
		sqls := ele.Value.(trxSummaryEntry).digests
		digest := ele.Value.(trxSummaryEntry).trxDigest
		res, err := json.Marshal(sqls)
		if err != nil {
			panic(err)
		}
		result = append(result, []types.Datum{
			types.NewUintDatum(uint64(digest)),
			types.NewDatum(string(res)),
		})
	}
	return result
}

// TrxHistoryRecorder is a history recorder for transaction.
type TrxHistoryRecorder struct {
	mu sync.Mutex

	summaries trxSummaries
}

// DumpTrxSummary dumps the transaction summary to Datum for displaying in `TRX_SUMMARY` table.
func (recorder *TrxHistoryRecorder) DumpTrxSummary() [][]types.Datum {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return recorder.summaries.dumpTrxSummary()
}

// OnTrxEnd should be called when a transaction ends, ie. leaves `TIDB_TRX` table.
func (recorder *TrxHistoryRecorder) OnTrxEnd(info *TxnInfo) {
	now := time.Now()
	startTime := time.Unix(0, oracle.ExtractPhysical(info.StartTS)*1e6)
	if now.Sub(startTime) < time.Second {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.summaries.onTrxEnd(info.AllSQLDigests)
}

func new(summariesCap uint) TrxHistoryRecorder {
	return TrxHistoryRecorder{
		summaries: newTrxSummaries(summariesCap),
	}
}

// Clean clears the history recorder. For test only.
func (recorder *TrxHistoryRecorder) Clean() {
	recorder.summaries.size = 0
}

// Recorder is the recorder instance.
var Recorder TrxHistoryRecorder = new(8192)
