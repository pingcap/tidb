// Copyright 2017 PingCAP, Inc.
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

package statistics

import (
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

// SampleCollector will collect Samples and calculate the count and ndv of an attribute.
type SampleCollector struct {
	Samples       []types.Datum
	NullCount     int64
	Count         int64
	MaxSampleSize int64
	Sketch        *FMSketch
}

// MergeSampleCollector merges two sample collectors.
func (c *SampleCollector) MergeSampleCollector(rc *SampleCollector) {
	c.NullCount += rc.NullCount
	c.Sketch.MergeFMSketch(rc.Sketch)
	for _, val := range rc.Samples {
		c.collect(val)
	}
}

func (c *SampleCollector) collect(d types.Datum) error {
	if d.IsNull() {
		c.NullCount++
		return nil
	}
	c.Count++
	// The following code use types.CopyDatum(d) because d may have a deep reference
	// to the underlying slice, GC can't free them which lead to memory leak eventually.
	// TODO: Refactor the proto to avoid copying here.
	if len(c.Samples) < int(c.MaxSampleSize) {
		c.Samples = append(c.Samples, types.CopyDatum(d))
	} else {
		shouldAdd := rand.Int63n(c.Count) < c.MaxSampleSize
		if shouldAdd {
			idx := rand.Intn(int(c.MaxSampleSize))
			c.Samples[idx] = types.CopyDatum(d)
		}
	}
	return errors.Trace(c.Sketch.InsertValue(d))
}

// SampleBuilder is used to build samples for columns.
// Also, if primary key is handle, it will directly build histogram for it.
type SampleBuilder struct {
	Sc            *variable.StatementContext
	RecordSet     ast.RecordSet
	ColLen        int   // ColLen is the number of columns need to be sampled.
	PkID          int64 // If primary key is handle, the PkID is the id of the primary key. If not exists, it is -1.
	MaxBucketSize int64
	MaxSampleSize int64
	MaxSketchSize int64
}

// CollectSamplesAndEstimateNDVs collects sample from the result set using Reservoir Sampling algorithm,
// and estimates NDVs using FM Sketch during the collecting process.
// It returns the sample collectors which contain total count, null count and distinct values count.
// It also returns the statistic builder for PK which contains the histogram.
// See https://en.wikipedia.org/wiki/Reservoir_sampling
func (s SampleBuilder) CollectSamplesAndEstimateNDVs() ([]*SampleCollector, *SortedBuilder, error) {
	var pkBuilder *SortedBuilder
	if s.PkID != -1 {
		pkBuilder = NewSortedBuilder(s.Sc, s.MaxBucketSize, s.PkID)
	}
	collectors := make([]*SampleCollector, s.ColLen)
	for i := range collectors {
		collectors[i] = &SampleCollector{
			MaxSampleSize: s.MaxSampleSize,
			Sketch:        NewFMSketch(int(s.MaxSketchSize)),
		}
	}
	for {
		row, err := s.RecordSet.Next()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if row == nil {
			return collectors, pkBuilder, nil
		}
		if s.PkID != -1 {
			err = pkBuilder.Iterate(row.Data[0])
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			row.Data = row.Data[1:]
		}
		for i, val := range row.Data {
			err = collectors[i].collect(val)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}
}
