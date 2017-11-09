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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

// SampleCollector will collect Samples and calculate the count and ndv of an attribute.
type SampleCollector struct {
	Samples       []types.Datum
	seenValues    int64 // seenValues is the current seen values.
	IsMerger      bool
	NullCount     int64
	Count         int64 // Count is the number of non-null rows.
	MaxSampleSize int64
	FMSketch      *FMSketch
	CMSketch      *CMSketch
}

// MergeSampleCollector merges two sample collectors.
func (c *SampleCollector) MergeSampleCollector(rc *SampleCollector) {
	c.NullCount += rc.NullCount
	c.Count += rc.Count
	c.FMSketch.mergeFMSketch(rc.FMSketch)
	if rc.CMSketch != nil {
		err := c.CMSketch.MergeCMSketch(rc.CMSketch)
		terror.Log(errors.Trace(err))
	}
	for _, val := range rc.Samples {
		err := c.collect(val)
		terror.Log(errors.Trace(err))
	}
}

// SampleCollectorToProto converts SampleCollector to its protobuf representation.
func SampleCollectorToProto(c *SampleCollector) *tipb.SampleCollector {
	collector := &tipb.SampleCollector{
		NullCount: c.NullCount,
		Count:     c.Count,
		FmSketch:  FMSketchToProto(c.FMSketch),
	}
	if c.CMSketch != nil {
		collector.CmSketch = CMSketchToProto(c.CMSketch)
	}
	for _, sample := range c.Samples {
		collector.Samples = append(collector.Samples, sample.GetBytes())
	}
	return collector
}

// SampleCollectorFromProto converts SampleCollector from its protobuf representation.
func SampleCollectorFromProto(collector *tipb.SampleCollector) *SampleCollector {
	s := &SampleCollector{
		NullCount: collector.NullCount,
		Count:     collector.Count,
		FMSketch:  FMSketchFromProto(collector.FmSketch),
	}
	if collector.CmSketch != nil {
		s.CMSketch = CMSketchFromProto(collector.CmSketch)
	}
	for _, val := range collector.Samples {
		s.Samples = append(s.Samples, types.NewBytesDatum(val))
	}
	return s
}

func (c *SampleCollector) collect(d types.Datum) error {
	if !c.IsMerger {
		if d.IsNull() {
			c.NullCount++
			return nil
		}
		c.Count++
		if err := c.FMSketch.InsertValue(d); err != nil {
			return errors.Trace(err)
		}
		if c.CMSketch != nil {
			c.CMSketch.InsertBytes(d.GetBytes())
		}
	}
	c.seenValues++
	// The following code use types.CopyDatum(d) because d may have a deep reference
	// to the underlying slice, GC can't free them which lead to memory leak eventually.
	// TODO: Refactor the proto to avoid copying here.
	if len(c.Samples) < int(c.MaxSampleSize) {
		c.Samples = append(c.Samples, types.CopyDatum(d))
	} else {
		shouldAdd := rand.Int63n(c.seenValues) < c.MaxSampleSize
		if shouldAdd {
			idx := rand.Intn(int(c.MaxSampleSize))
			c.Samples[idx] = types.CopyDatum(d)
		}
	}
	return nil
}

// SampleBuilder is used to build samples for columns.
// Also, if primary key is handle, it will directly build histogram for it.
type SampleBuilder struct {
	Sc              *variable.StatementContext
	RecordSet       ast.RecordSet
	ColLen          int   // ColLen is the number of columns need to be sampled.
	PkID            int64 // If primary key is handle, the PkID is the id of the primary key. If not exists, it is -1.
	MaxBucketSize   int64
	MaxSampleSize   int64
	MaxFMSketchSize int64
	CMSketchDepth   int32
	CMSketchWidth   int32
}

// CollectColumnStats collects sample from the result set using Reservoir Sampling algorithm,
// and estimates NDVs using FM Sketch during the collecting process.
// It returns the sample collectors which contain total count, null count, distinct values count and CM Sketch.
// It also returns the statistic builder for PK which contains the histogram.
// See https://en.wikipedia.org/wiki/Reservoir_sampling
func (s SampleBuilder) CollectColumnStats() ([]*SampleCollector, *SortedBuilder, error) {
	var pkBuilder *SortedBuilder
	if s.PkID != -1 {
		pkBuilder = NewSortedBuilder(s.Sc, s.MaxBucketSize, s.PkID)
	}
	collectors := make([]*SampleCollector, s.ColLen)
	for i := range collectors {
		collectors[i] = &SampleCollector{
			MaxSampleSize: s.MaxSampleSize,
			FMSketch:      NewFMSketch(int(s.MaxFMSketchSize)),
		}
	}
	if s.CMSketchDepth > 0 && s.CMSketchWidth > 0 {
		for i := range collectors {
			collectors[i].CMSketch = NewCMSketch(s.CMSketchDepth, s.CMSketchWidth)
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
