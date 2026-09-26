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

// ndvCounts holds the row counts that GEE needs besides the sketch. The sketch
// outlives the collector that has them. Size does not affect NDV.
type ndvCounts struct {
	// rows is the number of visible rows in the scan, sampled or not.
	rows int64
	// samples is the number of rows that TiKV sampled for NDV. It includes the
	// rows in which the column is NULL.
	samples int64
	// nulls is the number of rows in which the column is NULL. TiKV counts the
	// NULLs in the sampled rows and scales that count up to rows.
	nulls int64
}

func newSampledFMSketch(pb *tipb.FMSketch, counts ndvCounts) *FMSketch {
	sketch := &FMSketch{
		hashset: make(map[uint64]bool, len(pb.Hashset)+len(pb.MultiHashset)),
		mask:    pb.Mask, maxSize: MaxSketchSize, ndvCounts: &counts,
	}
	for _, hash := range pb.Hashset {
		sketch.hashset[hash] = false
	}
	for _, hash := range pb.MultiHashset {
		sketch.hashset[hash] = true
	}
	return sketch
}

// Sampled reports whether the NDV of s is estimated from sampled rows.
func (s *FMSketch) Sampled() bool {
	return s != nil && s.ndvCounts != nil
}

func (s *FMSketch) sampledNDV() int64 {
	counts := s.ndvCounts
	// TiKV sets the sample count even if it sampled no row. The sketch is then
	// empty, and the division below would give NaN.
	if counts.samples == 0 {
		return 0
	}
	// A sketch over maxSize keeps about 1/weight of its hashes. weight times a
	// kept count is thus an estimate, and it can exceed the true count.
	weight := float64(s.mask + 1)
	// d is the number of distinct values in the sample. It cannot exceed the
	// number of sampled rows.
	d := min(float64(counts.samples), weight*float64(len(s.hashset)))
	singles := 0
	for _, repeated := range s.hashset {
		if !repeated {
			singles++
		}
	}
	// f1 is the number of values that the sample has once. They are a part of
	// d, so f1 cannot exceed d.
	f1 := min(d, weight*float64(singles))
	// GEE scales by non-NULL rows over non-NULL sampled rows. A random sample
	// has the same NULL share as the table, so that ratio is rows/samples.
	estimate := math.Round(d + (math.Sqrt(float64(counts.rows)/float64(counts.samples))-1)*f1)
	// The NDV cannot exceed the non-NULL rows. The limit on d does not ensure
	// this, because samples includes the NULL rows.
	return min(int64(estimate), counts.rows-counts.nulls)
}

func (s *FMSketch) encodeSampled() ([]byte, error) {
	counts := s.ndvCounts
	collector := tipb.RowSampleCollector{
		Count: counts.rows, NdvSampleCount: &counts.samples,
		NullCounts: []int64{counts.nulls},
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
	return newSampledFMSketch(collector.FmSketch[0], ndvCounts{
		rows: collector.Count, samples: *collector.NdvSampleCount, nulls: collector.NullCounts[0],
	}), nil
}
