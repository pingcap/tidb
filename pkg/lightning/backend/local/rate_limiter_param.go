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
	CurrentMaxBatchSplitRanges atomic.Pointer[int]
	// CurrentMaxSplitRangesPerSec stores the current limit for split ranges per second.
	CurrentMaxSplitRangesPerSec atomic.Pointer[float64]
	// CurrentMaxIngestInflight stores the current limit for concurrent ingest requests.
	CurrentMaxIngestInflight atomic.Pointer[int]
	// CurrentMaxIngestPerSec stores the current limit for maximum ingest requests per second.
	CurrentMaxIngestPerSec atomic.Pointer[float64]
)

// InitializeRateLimiterParam initializes the rate limiter params.
func InitializeRateLimiterParam(m *meta.Mutator, logger *zap.Logger) error {
	err := initializeVariables(
		m.GetIngestMaxBatchSplitRanges, m.SetIngestMaxBatchSplitRanges,
		defaultMaxBatchSplitRanges, &CurrentMaxBatchSplitRanges,
		logger, "maxBatchSplitRanges")
	if err != nil {
		return err
	}
	err = initializeVariables(
		m.GetIngestMaxSplitRangesPerSec, m.SetIngestMaxSplitRangesPerSec,
		defaultSplitRangesPerSec, &CurrentMaxSplitRangesPerSec,
		logger, "maxSplitRangesPerSec")
	if err != nil {
		return err
	}
	err = initializeVariables(
		m.GetIngestMaxInflight, m.SetIngestMaxInflight,
		defaultMaxIngestInflight, &CurrentMaxIngestInflight,
		logger, "maxIngestInflight")
	if err != nil {
		return err
	}
	err = initializeVariables(
		m.GetIngestMaxPerSec, m.SetIngestMaxPerSec,
		defaultMaxIngestPerSec, &CurrentMaxIngestPerSec,
		logger, "maxIngestPerSec")
	if err != nil {
		return err
	}
	return nil
}

func initializeVariables[T comparable](
	metaGetter func() (v T, isNull bool, err error),
	metaSetter func(v T) error,
	defaultVal T,
	globalVar *atomic.Pointer[T],
	logger *zap.Logger,
	varName string,
) error {
	val, isNull, err := metaGetter()
	if err != nil {
		return errors.Annotatef(err, "failed to read %s value from meta store", varName)
	}
	var zero T
	if isNull {
		err = metaSetter(defaultVal)
		if err != nil {
			return errors.Annotatef(err, "failed to set %s value to meta store", varName)
		}
		val = defaultVal
		logger.Info("meta kv not found in meta store, initialized to default and persisted",
			zap.String("key", varName),
			zap.Any("value", defaultVal))
	} else if val == zero {
		val = defaultVal
	} else {
		logger.Info("loaded value from meta store",
			zap.String("key", varName),
			zap.Any("value", val))
	}
	globalVar.Store(&val)
	return nil
}

// GetMaxBatchSplitRanges returns the current maximum number of ranges in a batch to split and scatter.
func GetMaxBatchSplitRanges() int {
	val := CurrentMaxBatchSplitRanges.Load()
	if val == nil || *val == 0 { // Not yet initialized from TiKV or invalid value caused fallback to 0
		return defaultMaxBatchSplitRanges
	}
	return *val
}

// GetMaxSplitRangePerSec returns the current maximum number of ranges to split and scatter per second.
func GetMaxSplitRangePerSec() float64 {
	val := CurrentMaxSplitRangesPerSec.Load()
	if val == nil {
		return 0
	}
	return *val
}

// GetMaxIngestConcurrency returns the current maximum number of concurrent ingest requests.
func GetMaxIngestConcurrency() int {
	val := CurrentMaxIngestInflight.Load()
	if val == nil {
		return 0
	}
	return *val
}

// GetMaxIngestPerSec returns the current maximum number of ingest requests per second.
func GetMaxIngestPerSec() float64 {
	val := CurrentMaxIngestPerSec.Load()
	if val == nil {
		return 0
	}
	return *val
}
