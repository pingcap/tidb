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

package statistics

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tipb/go-tipb"
)

// ndvSample holds what GEE needs besides the sketch: the visible rows, the rows
// sampled for NDV, and the NULL estimate that TiKV scaled to all rows. Size
// does not affect NDV.
type ndvSample struct {
	rows, samples, nulls int64
}

func newSampledFMSketch(pb *tipb.FMSketch, sample ndvSample) *FMSketch {
	sketch := &FMSketch{
		hashset:  make(map[uint64]struct{}, len(pb.Hashset)),
		repeated: make(map[uint64]struct{}, len(pb.MultiHashset)),
		mask:     pb.Mask, maxSize: MaxSketchSize, sample: &sample,
	}
	for _, hash := range pb.Hashset {
		sketch.hashset[hash] = struct{}{}
	}
	for _, hash := range pb.MultiHashset {
		sketch.repeated[hash] = struct{}{}
	}
	return sketch
}

// Sampled reports whether the NDV of s is estimated from sampled rows.
func (s *FMSketch) Sampled() bool {
	return s != nil && (s.sample != nil || s.weights != nil)
}

// MergePartitionFMSketch merges the sketch of another partition into s for
// global statistics. GEE scales every singleton by one ratio, which fits only
// one rate, so here each singleton keeps the scale of the partition that
// sampled it. The values of full-input partitions are known, so they are never
// scaled.
func (s *FMSketch) MergePartitionFMSketch(rs *FMSketch) {
	if s == nil || rs == nil {
		return
	}
	if s.Sampled() || rs.Sampled() {
		s.toWeighted()
	}
	if s.mask < rs.mask {
		s.mask = rs.mask
		s.filterHashes()
	}
	if s.weights == nil {
		for hash := range rs.hashset {
			s.insertHashValue(hash)
		}
		return
	}
	for hash := range rs.repeated {
		s.insertWeighted(hash, 0)
	}
	scale := rs.singletonScale()
	for hash := range rs.hashset {
		s.insertWeighted(hash, scale)
	}
}

// singletonScale is the GEE scale of a singleton, sqrt(N/n), or zero for full
// input, whose values need no scaling.
func (s *FMSketch) singletonScale() float64 {
	if s.sample == nil || s.sample.samples == 0 {
		return 0
	}
	return math.Sqrt(float64(s.sample.rows) / float64(s.sample.samples))
}

// toWeighted moves the scale of the singletons of s into weights.
func (s *FMSketch) toWeighted() {
	if s.weights != nil {
		return
	}
	if s.repeated == nil {
		s.repeated = make(map[uint64]struct{}, len(s.hashset))
	}
	s.weights = make(map[uint64]float64, len(s.hashset))
	scale := s.singletonScale()
	for hash := range s.hashset {
		if scale == 0 {
			delete(s.hashset, hash)
			s.repeated[hash] = struct{}{}
		} else {
			s.weights[hash] = scale
		}
	}
	s.sample = nil
}

// insertWeighted adds a hash seen once with the scale of its partition, or,
// with scale zero, a hash seen more than once or from full input.
func (s *FMSketch) insertWeighted(hash uint64, scale float64) {
	if hash&s.mask != 0 {
		return
	}
	if _, ok := s.repeated[hash]; ok {
		return
	}
	if _, ok := s.hashset[hash]; ok || scale == 0 {
		delete(s.hashset, hash)
		delete(s.weights, hash)
		s.repeated[hash] = struct{}{}
	} else {
		s.hashset[hash] = struct{}{}
		s.weights[hash] = scale
	}
	if len(s.hashset)+len(s.repeated) > s.maxSize {
		s.mask = s.mask*2 + 1
		s.filterHashes()
	}
}

func (s *FMSketch) weightedNDV() int64 {
	sum := float64(len(s.repeated))
	for _, scale := range s.weights {
		sum += scale
	}
	return int64(math.Round(float64(s.mask+1) * sum))
}

func (s *FMSketch) sampledNDV() int64 {
	sample := s.sample
	upper := sample.rows - sample.nulls
	if sample.samples == 0 || upper == 0 {
		return 0
	}
	weight := float64(s.mask + 1)
	d := min(float64(min(sample.samples, upper)), weight*float64(len(s.hashset)+len(s.repeated)))
	f1 := min(d, weight*float64(len(s.hashset)))
	// The non-NULL share cancels from N/n. Use T/S directly to avoid
	// reconstructing and rounding a per-column sample size.
	estimate := math.Round(d + (math.Sqrt(float64(sample.rows)/float64(sample.samples))-1)*f1)
	if estimate >= float64(upper) {
		return upper
	}
	return int64(estimate)
}

func (s *FMSketch) encodeSampled() ([]byte, error) {
	sample := s.sample
	collector := tipb.RowSampleCollector{
		Count: sample.rows, NdvSampleCount: &sample.samples,
		NullCounts: []int64{sample.nulls},
		FmSketch:   []*tipb.FMSketch{FMSketchToProto(s)},
	}
	data, err := collector.Marshal()
	if err != nil {
		return nil, err
	}
	// Tag zero is invalid protobuf, so old TiDB must reject this format.
	// The second byte versions the envelope.
	return append([]byte{0, 1}, data...), nil
}

func decodeSampledFMSketch(data []byte) (*FMSketch, error) {
	if len(data) < 2 || data[1] != 1 {
		return nil, errors.New("unsupported sampled NDV sketch format")
	}
	var collector tipb.RowSampleCollector
	if err := collector.Unmarshal(data[2:]); err != nil {
		return nil, err
	}
	return newSampledFMSketch(collector.FmSketch[0], ndvSample{
		rows: collector.Count, samples: *collector.NdvSampleCount, nulls: collector.NullCounts[0],
	}), nil
}
