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
	return s != nil && s.sample != nil
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
