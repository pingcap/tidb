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
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/tikv/client-go/v2/oracle"
)

func digest(digests []string) uint64 {
	// We use FNV-1a hash to generate the 64bit digest
	// since 64bit digest use less memory and FNV-1a is faster than most of other hash algorithms
	// You can refer to https://softwareengineering.stackexchange.com/questions/49550/which-hashing-algorithm-is-best-for-uniqueness-and-speed
	hash := fnv.New64a()
	for _, digest := range digests {
		hash.Write([]byte(digest))
	}
	return hash.Sum64()
}

type trxSummaryEntry struct {
	trxDigest uint64
	digests   []string
}

type trxSummaries struct {
	capacity uint

	// lru cache for digest -> trxSummaryEntry
	elements map[uint64]*list.Element
	cache    *list.List
}

func newTrxSummaries(capacity uint) trxSummaries {
	return trxSummaries{
		capacity: capacity,
		cache:    list.New(),
		elements: make(map[uint64]*list.Element),
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
	if uint(s.cache.Len()) > s.capacity {
		last := s.cache.Back()
		delete(s.elements, last.Value.(trxSummaryEntry).trxDigest)
		s.cache.Remove(last)
	}
}

func (s *trxSummaries) dumpTrxSummary() [][]types.Datum {
	var result [][]types.Datum
	for element := s.cache.Front(); element != nil; element = element.Next() {
		sqls := element.Value.(trxSummaryEntry).digests
		// for consistency with other digests in TiDB, we calculate sum256 here to generate varchar(64) digest
		digest := fmt.Sprintf("%x", element.Value.(trxSummaryEntry).trxDigest)

		res, err := json.Marshal(sqls)
		if err != nil {
			panic(err)
		}

		result = append(result, []types.Datum{
			types.NewDatum(digest),
			types.NewDatum(string(res)),
		})
	}
	return result
}

func (s *trxSummaries) resize(capacity uint) {
	s.capacity = capacity
	for uint(s.cache.Len()) > s.capacity {
		last := s.cache.Back()
		delete(s.elements, last.Value.(trxSummaryEntry).trxDigest)
		s.cache.Remove(last)
	}
}

// TrxHistoryRecorder is a history recorder for transaction.
type TrxHistoryRecorder struct {
	mu          sync.Mutex
	minDuration time.Duration
	summaries   trxSummaries
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
	startTime := time.UnixMilli(oracle.ExtractPhysical(info.StartTS))
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if now.Sub(startTime) < recorder.minDuration {
		return
	}
	recorder.summaries.onTrxEnd(info.AllSQLDigests)
}

func newTrxHistoryRecorder(summariesCap uint) TrxHistoryRecorder {
	return TrxHistoryRecorder{
		summaries:   newTrxSummaries(summariesCap),
		minDuration: 1 * time.Second,
	}
}

// Clean clears the history recorder. For test only.
func (recorder *TrxHistoryRecorder) Clean() {
	recorder.summaries.cache = list.New()
}

// SetMinDuration sets the minimum duration for a transaction to be recorded.
func (recorder *TrxHistoryRecorder) SetMinDuration(d time.Duration) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.minDuration = d
}

// ResizeSummaries resizes the summaries capacity.
func (recorder *TrxHistoryRecorder) ResizeSummaries(capacity uint) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.summaries.resize(capacity)
}

// Recorder is the recorder instance.
var Recorder = newTrxHistoryRecorder(0)
