// Copyright 2025 PingCAP, Inc.
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

package local

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// defaultMaxBatchSplitRanges is the default max ranges count in a batch to split and scatter.
	defaultMaxBatchSplitRanges = 2048
	// defaultSplitRangesPerSec is the default max ranges count to split and scatter per second.
	defaultSplitRangesPerSec = 0
	// defaultMaxIngestInflight is the default max concurrent ingest requests.
	defaultMaxIngestInflight = 0
	// default MaxIngestPerSec is the default max ingest requests per second.
	defaultMaxIngestPerSec = 0
)

var (
	// CurrentMaxBatchSplitRanges stores the current limit for batch split ranges.
	CurrentMaxBatchSplitRanges atomic.Int64
	// CurrentMaxSplitRangesPerSec stores the current limit for split ranges per second.
	CurrentMaxSplitRangesPerSec atomic.Float64
	// CurrentMaxIngestInflight stores the current limit for concurrent ingest requests.
	CurrentMaxIngestInflight atomic.Int64
	// CurrentMaxIngestPerSec stores the current limit for maximum ingest requests per second.
	CurrentMaxIngestPerSec atomic.Float64
)

// InitializeGlobalMaxBatchSplitRanges loads the maxBatchSplitRanges value using meta.Meta or sets a default.
func InitializeGlobalMaxBatchSplitRanges(m *meta.Mutator, logger *zap.Logger) error {
	valInt, isNull, err := m.GetIngestMaxBatchSplitRanges()
	if err != nil {
		return errors.Annotate(err, "failed to read maxBatchSplitRanges from meta store")
	}
	if isNull || valInt <= 0 {
		valInt = defaultMaxBatchSplitRanges
		logger.Info("maxBatchSplitRanges not found in meta store, initialized to default and persisted",
			zap.Int("value", defaultMaxBatchSplitRanges))
	} else {
		logger.Info("loaded maxBatchSplitRanges from meta store", zap.Int("value", valInt))
	}
	CurrentMaxBatchSplitRanges.Store(int64(valInt))
	return nil
}

// InitializeGlobalSplitRangesPerSec loads the splitRangesPerSec value using meta.Meta or sets a default.
func InitializeGlobalSplitRangesPerSec(m *meta.Mutator, logger *zap.Logger) error {
	val, isNull, err := m.GetIngestMaxSplitRangesPerSec()
	if err != nil {
		return errors.Annotate(err, "failed to read splitRangesPerSec from meta store")
	}
	if isNull || val <= 0 {
		val = defaultSplitRangesPerSec
		logger.Info("splitRangesPerSec not found in meta store, initialized to default and persisted",
			zap.Float64("value", defaultSplitRangesPerSec))
	} else {
		logger.Info("loaded splitRangesPerSec from meta store", zap.Float64("value", val))
	}
	CurrentMaxSplitRangesPerSec.Store(val)
	return nil
}

// InitializeGlobalIngestConcurrency loads the maxIngestConcurrency value using meta.Meta or sets a default.
func InitializeGlobalIngestConcurrency(m *meta.Mutator, logger *zap.Logger) error {
	valInt, isNull, err := m.GetIngestMaxInflight()
	if err != nil {
		return errors.Annotate(err, "failed to read maxIngestConcurrency from meta store")
	}
	if isNull {
		valInt = defaultMaxIngestInflight
		logger.Info("maxIngestConcurrency not found in meta store, initialized to default and persisted",
			zap.Int("value", defaultMaxIngestInflight))
	} else {
		logger.Info("loaded maxIngestConcurrency from meta store", zap.Int("value", valInt))
	}
	CurrentMaxIngestInflight.Store(int64(valInt))
	return nil
}

// InitializeGlobalIngestPerSec loads the maxIngestPerSec value using meta.Meta or sets a default.
func InitializeGlobalIngestPerSec(m *meta.Mutator, logger *zap.Logger) error {
	val, isNull, err := m.GetIngestMaxPerSec()
	if err != nil {
		return errors.Annotate(err, "failed to read maxIngestPerSec from meta store")
	}
	if isNull {
		val = defaultMaxIngestPerSec
		logger.Info("maxIngestPerSec not found in meta store, initialized to default and persisted",
			zap.Float64("value", defaultMaxIngestPerSec))
	} else {
		logger.Info("loaded maxIngestPerSec from meta store", zap.Float64("value", val))
	}
	CurrentMaxIngestPerSec.Store(val)
	return nil
}

// GetMaxBatchSplitRanges returns the current maximum number of ranges in a batch to split and scatter.
func GetMaxBatchSplitRanges() int {
	val := CurrentMaxBatchSplitRanges.Load()
	if val == 0 { // Not yet initialized from TiKV or invalid value caused fallback to 0
		return defaultMaxBatchSplitRanges
	}
	return int(val)
}

// GetMaxSplitRangePerSec returns the current maximum number of ranges to split and scatter per second.
func GetMaxSplitRangePerSec() float64 {
	val := CurrentMaxSplitRangesPerSec.Load()
	return val
}

// GetMaxIngestConcurrency returns the current maximum number of concurrent ingest requests.
func GetMaxIngestConcurrency() int {
	val := CurrentMaxIngestInflight.Load()
	return int(val)
}

// GetMaxIngestPerSec returns the current maximum number of ingest requests per second.
func GetMaxIngestPerSec() float64 {
	val := CurrentMaxIngestPerSec.Load()
	return val
}
