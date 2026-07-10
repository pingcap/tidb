// Copyright 2026 PingCAP, Inc.
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

package earlystopprofile

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/util/kvcache"
)

const (
	defaultCapacity          = 3000
	minSamplesForUse         = 3
	defaultTTL               = 48 * time.Hour
	ewmaAlpha                = 0.2
	highOverReadRatio        = 64
	midOverReadRatio         = 16
	lowOverReadRatio         = 4
	healthySamplesForRecover = 3
	probeIntervalSamples     = 32
)

// ReaderType is the physical reader kind that registered an early-stop profile candidate.
type ReaderType uint8

const (
	// ReaderTypeTable is a TableReader candidate.
	ReaderTypeTable ReaderType = iota + 1
	// ReaderTypeIndex is an IndexReader candidate.
	ReaderTypeIndex
	// ReaderTypeIndexLookup is an IndexLookUpReader candidate.
	ReaderTypeIndexLookup
	// ReaderTypeIndexLookupPushDown is an IndexLookUpReader candidate with index lookup pushdown.
	ReaderTypeIndexLookupPushDown
	// ReaderTypeIndexJoin is an IndexJoin candidate.
	ReaderTypeIndexJoin
)

// String returns a stable low-cardinality label for the reader type.
func (t ReaderType) String() string {
	switch t {
	case ReaderTypeTable:
		return "table"
	case ReaderTypeIndex:
		return "index"
	case ReaderTypeIndexLookup:
		return "index_lookup"
	case ReaderTypeIndexLookupPushDown:
		return "index_lookup_pushdown"
	case ReaderTypeIndexJoin:
		return "index_join"
	default:
		return "unknown"
	}
}

// LimitBucket groups LIMIT values so SQL digests that normalize constants do
// not share one profile for very different early-stop demands.
type LimitBucket uint8

const (
	// LimitBucketLE1 is for LIMIT rows <= 1.
	LimitBucketLE1 LimitBucket = iota + 1
	// LimitBucketLE10 is for LIMIT rows <= 10.
	LimitBucketLE10
	// LimitBucketLE100 is for LIMIT rows <= 100.
	LimitBucketLE100
	// LimitBucketLE1024 is for LIMIT rows <= 1024.
	LimitBucketLE1024
	// LimitBucketLE10000 is for LIMIT rows <= 10000.
	LimitBucketLE10000
	// LimitBucketGT10000 is for LIMIT rows > 10000.
	LimitBucketGT10000
)

// LimitBucketForRows maps offset+count into a profile bucket.
func LimitBucketForRows(limitRows uint64) LimitBucket {
	switch {
	case limitRows <= 1:
		return LimitBucketLE1
	case limitRows <= 10:
		return LimitBucketLE10
	case limitRows <= 100:
		return LimitBucketLE100
	case limitRows <= 1024:
		return LimitBucketLE1024
	case limitRows <= 10000:
		return LimitBucketLE10000
	default:
		return LimitBucketGT10000
	}
}

// ScanRatioBucket groups the estimated scan rows relative to LIMIT demand.
// It separates parameter values with materially different scan shapes while
// keeping the profile cardinality bounded.
type ScanRatioBucket uint8

const (
	// ScanRatioBucketUnknown is used when no reliable scan-row estimate is available.
	ScanRatioBucketUnknown ScanRatioBucket = iota
	// ScanRatioBucketLE4 is for estimated scan rows <= 4x LIMIT demand.
	ScanRatioBucketLE4
	// ScanRatioBucketLE16 is for estimated scan rows <= 16x LIMIT demand.
	ScanRatioBucketLE16
	// ScanRatioBucketLE64 is for estimated scan rows <= 64x LIMIT demand.
	ScanRatioBucketLE64
	// ScanRatioBucketLE256 is for estimated scan rows <= 256x LIMIT demand.
	ScanRatioBucketLE256
	// ScanRatioBucketGT256 is for estimated scan rows > 256x LIMIT demand.
	ScanRatioBucketGT256
)

// ScanRatioBucketForRows maps an estimated scan size and LIMIT demand into a
// bounded profile bucket. Non-positive and NaN estimates map to unknown.
func ScanRatioBucketForRows(estimatedScanRows float64, limitRows uint64) ScanRatioBucket {
	if limitRows == 0 || !(estimatedScanRows > 0) {
		return ScanRatioBucketUnknown
	}
	ratio := estimatedScanRows / float64(limitRows)
	switch {
	case ratio <= 4:
		return ScanRatioBucketLE4
	case ratio <= 16:
		return ScanRatioBucketLE16
	case ratio <= 64:
		return ScanRatioBucketLE64
	case ratio <= 256:
		return ScanRatioBucketLE256
	default:
		return ScanRatioBucketGT256
	}
}

// Key identifies a keep-order LIMIT scan profile.
type Key struct {
	SchemaName      string
	SQLDigest       string
	PlanDigest      string
	ReaderType      ReaderType
	KeepOrder       bool
	LimitBucket     LimitBucket
	ScanRatioBucket ScanRatioBucket
}

// Hash implements kvcache.Key.
func (k Key) Hash() []byte {
	b := make([]byte, 0, len(k.SchemaName)+len(k.SQLDigest)+len(k.PlanDigest)+9)
	b = append(b, k.SchemaName...)
	b = append(b, 0)
	b = append(b, k.SQLDigest...)
	b = append(b, 0)
	b = append(b, k.PlanDigest...)
	b = append(b, 0, byte(k.ReaderType), boolByte(k.KeepOrder), byte(k.LimitBucket), byte(k.ScanRatioBucket))
	return b
}

// Candidate is registered during executor construction when the current plan
// proves that a reader is a keep-order LIMIT early-stop candidate.
type Candidate struct {
	Key Key
	// LimitRows is the scan demand, including OFFSET rows.
	LimitRows uint64
	// ExpectedOutputRows is the LIMIT count visible to the client.
	ExpectedOutputRows uint64
	BaseCap            int
	CapUsed            int

	ReaderPlanID int
	LookupPlanID int
	IndexPlanID  int
	TablePlanID  int
}

// Sample is observed after statement execution.
type Sample struct {
	Candidate     Candidate
	ResultRows    uint64
	RequestCount  int
	ProcessedKeys uint64
	TotalKeys     uint64
	Latency       time.Duration
	Succeed       bool
	Internal      bool

	ReaderActRows uint64
	LookupActRows uint64
	IndexActRows  uint64
	TableActRows  uint64
}

// Profile is the historical feedback used to recommend a scan cap.
type Profile struct {
	Samples uint32

	EWMARowsPerTask            float64
	EWMAProcessedKeysPerResult float64
	EWMAOverReadRatio          float64
	EWMARequestCount           float64
	EWMALatencyMS              float64
	EWMAReaderActRowsPerResult float64
	EWMALookupActRowsPerResult float64
	EWMAIndexActRowsPerResult  float64
	EWMATableActRowsPerResult  float64

	BaseCap             int
	RecommendedCap      int
	HealthySamples      uint32
	SamplesSinceLastTry uint32
	LastUpdatedUnix     int64
}

// Recommendation is the stable feedback exposed to executor builders.
type Recommendation struct {
	Cap int

	RowsPerTask            float64
	ProcessedKeysPerResult float64
	OverReadRatio          float64
	RequestCount           float64
	LatencyMS              float64
	ReaderRowsPerResult    float64
	LookupRowsPerResult    float64
	IndexRowsPerResult     float64
	TableRowsPerResult     float64
}

// Store is a mutex-protected LRU cache of early-stop scan profiles.
type Store struct {
	mu    sync.Mutex
	cache *kvcache.SimpleLRUCache
	ttl   time.Duration
}

// NewStore creates a profile store.
func NewStore(capacity uint) *Store {
	return &Store{
		cache: kvcache.NewSimpleLRUCache(capacity, 0, 0),
		ttl:   defaultTTL,
	}
}

var globalStore = NewStore(defaultCapacity)

// LookupCap returns a recommended concurrency cap from the global store.
func LookupCap(key Key) (int, bool) {
	return globalStore.LookupCap(key)
}

// LookupRecommendation returns a recommendation from the global store.
func LookupRecommendation(key Key) (Recommendation, bool) {
	return globalStore.LookupRecommendation(key)
}

// Observe updates the global store with one statement sample and reports whether it was accepted.
func Observe(sample Sample) bool {
	return globalStore.Observe(sample)
}

// ResetForTest clears the global store.
func ResetForTest() {
	globalStore = NewStore(defaultCapacity)
}

// LookupCap returns a recommended concurrency cap.
func (s *Store) LookupCap(key Key) (int, bool) {
	recommendation, ok := s.LookupRecommendation(key)
	if !ok {
		return 0, false
	}
	return recommendation.Cap, true
}

// LookupRecommendation returns a recommended concurrency cap and budget signals.
func (s *Store) LookupRecommendation(key Key) (Recommendation, bool) {
	now := time.Now().Unix()
	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.cache.Get(key)
	if !ok {
		return Recommendation{}, false
	}
	profile := value.(*Profile)
	if profile.Samples < minSamplesForUse || profile.RecommendedCap <= 0 {
		return Recommendation{}, false
	}
	if s.ttl > 0 && now-profile.LastUpdatedUnix > int64(s.ttl.Seconds()) {
		return Recommendation{}, false
	}
	return Recommendation{
		Cap:                    profile.RecommendedCap,
		RowsPerTask:            profile.EWMARowsPerTask,
		ProcessedKeysPerResult: profile.EWMAProcessedKeysPerResult,
		OverReadRatio:          profile.EWMAOverReadRatio,
		RequestCount:           profile.EWMARequestCount,
		LatencyMS:              profile.EWMALatencyMS,
		ReaderRowsPerResult:    profile.EWMAReaderActRowsPerResult,
		LookupRowsPerResult:    profile.EWMALookupActRowsPerResult,
		IndexRowsPerResult:     profile.EWMAIndexActRowsPerResult,
		TableRowsPerResult:     profile.EWMATableActRowsPerResult,
	}, true
}

// Observe updates the store with one statement sample and reports whether it was accepted.
func (s *Store) Observe(sample Sample) bool {
	candidate := sample.Candidate
	if !sample.Succeed || sample.Internal || candidate.LimitRows == 0 ||
		candidate.ExpectedOutputRows == 0 || candidate.ExpectedOutputRows > candidate.LimitRows ||
		candidate.CapUsed <= 0 || sample.ResultRows < candidate.ExpectedOutputRows {
		return false
	}
	overReadRows := max(
		sample.ProcessedKeys,
		sample.ReaderActRows,
		sample.LookupActRows,
		sample.IndexActRows,
		sample.TableActRows,
	)
	if overReadRows == 0 {
		return false
	}

	key := candidate.Key
	now := time.Now().Unix()
	demandRows := max(candidate.LimitRows, sample.ResultRows, 1)
	overReadRatio := float64(overReadRows) / float64(demandRows)
	rowsPerTask := float64(sample.ResultRows) / float64(maxInt(sample.RequestCount, 1))
	keysPerResult := float64(sample.ProcessedKeys) / float64(max(sample.ResultRows, 1))
	latencyMS := float64(sample.Latency) / float64(time.Millisecond)
	readerActRowsPerResult := float64(sample.ReaderActRows) / float64(demandRows)
	lookupActRowsPerResult := float64(sample.LookupActRows) / float64(demandRows)
	indexActRowsPerResult := float64(sample.IndexActRows) / float64(demandRows)
	tableActRowsPerResult := float64(sample.TableActRows) / float64(demandRows)

	s.mu.Lock()
	defer s.mu.Unlock()

	profile := &Profile{}
	if value, ok := s.cache.Get(key); ok {
		profile = value.(*Profile)
	}
	baseCap := candidate.BaseCap
	if baseCap <= 0 {
		baseCap = candidate.CapUsed
	}
	if profile.BaseCap < baseCap {
		profile.BaseCap = baseCap
	}
	profile.Samples++
	if profile.Samples == 1 {
		profile.EWMARowsPerTask = rowsPerTask
		profile.EWMAProcessedKeysPerResult = keysPerResult
		profile.EWMAOverReadRatio = overReadRatio
		profile.EWMARequestCount = float64(sample.RequestCount)
		profile.EWMALatencyMS = latencyMS
		profile.EWMAReaderActRowsPerResult = readerActRowsPerResult
		profile.EWMALookupActRowsPerResult = lookupActRowsPerResult
		profile.EWMAIndexActRowsPerResult = indexActRowsPerResult
		profile.EWMATableActRowsPerResult = tableActRowsPerResult
	} else {
		profile.EWMARowsPerTask = ewma(profile.EWMARowsPerTask, rowsPerTask)
		profile.EWMAProcessedKeysPerResult = ewma(profile.EWMAProcessedKeysPerResult, keysPerResult)
		profile.EWMAOverReadRatio = ewma(profile.EWMAOverReadRatio, overReadRatio)
		profile.EWMARequestCount = ewma(profile.EWMARequestCount, float64(sample.RequestCount))
		profile.EWMALatencyMS = ewma(profile.EWMALatencyMS, latencyMS)
		profile.EWMAReaderActRowsPerResult = ewma(profile.EWMAReaderActRowsPerResult, readerActRowsPerResult)
		profile.EWMALookupActRowsPerResult = ewma(profile.EWMALookupActRowsPerResult, lookupActRowsPerResult)
		profile.EWMAIndexActRowsPerResult = ewma(profile.EWMAIndexActRowsPerResult, indexActRowsPerResult)
		profile.EWMATableActRowsPerResult = ewma(profile.EWMATableActRowsPerResult, tableActRowsPerResult)
	}
	profile.RecommendedCap = recommendCap(profile)
	profile.LastUpdatedUnix = now
	s.cache.Put(key, profile)
	return true
}

func recommendCap(profile *Profile) int {
	baseCap := maxInt(profile.BaseCap, 1)
	currentCap := profile.RecommendedCap
	if currentCap <= 0 {
		currentCap = baseCap
	}

	switch {
	case profile.EWMAOverReadRatio >= highOverReadRatio:
		profile.HealthySamples = 0
		profile.SamplesSinceLastTry = 0
		return 1
	case profile.EWMAOverReadRatio >= midOverReadRatio:
		profile.HealthySamples = 0
		profile.SamplesSinceLastTry = 0
		return min(baseCap, 2)
	case profile.EWMAOverReadRatio <= lowOverReadRatio:
		profile.HealthySamples++
		if profile.HealthySamples >= healthySamplesForRecover {
			profile.HealthySamples = 0
			profile.SamplesSinceLastTry = 0
			return min(baseCap, currentCap+1)
		}
	default:
		profile.HealthySamples = 0
	}

	if currentCap < baseCap {
		profile.SamplesSinceLastTry++
		if profile.SamplesSinceLastTry >= probeIntervalSamples {
			profile.SamplesSinceLastTry = 0
			return min(baseCap, currentCap+1)
		}
	} else {
		profile.SamplesSinceLastTry = 0
	}
	return min(currentCap, baseCap)
}

func ewma(old, current float64) float64 {
	return old*(1-ewmaAlpha) + current*ewmaAlpha
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func max(values ...uint64) uint64 {
	var ret uint64
	for _, value := range values {
		if value > ret {
			ret = value
		}
	}
	return ret
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
