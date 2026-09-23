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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tipb/go-tipb"
)

// ErrIncompatibleNDV prevents a failed sampled merge from being reported as a
// successful ANALYZE with only a log message.
var ErrIncompatibleNDV = errors.New("incompatible NDV statistics")

// ndvSample stays with the sketch so saved partitions use collection-time
// counts, including the NULL estimate already scaled by TiKV. Size does not
// affect NDV. The configured rate comes from the request, never rows / samples.
type ndvSample struct {
	rate                 float64
	rows, samples, nulls int64
}

func newSampledFMSketch(pb *tipb.FMSketch, sample ndvSample) (*FMSketch, error) {
	if !validNDVRate(sample.rate) ||
		sample.rows < 0 || sample.samples < 0 || sample.samples > sample.rows ||
		sample.nulls < 0 || sample.nulls > sample.rows ||
		pb.Mask == math.MaxUint64 || pb.Mask&(pb.Mask+1) != 0 {
		return nil, errors.New("invalid sampled NDV metadata")
	}
	retained := len(pb.Hashset) + len(pb.MultiHashset)
	if retained > MaxSketchSize || int64(len(pb.Hashset))+2*int64(len(pb.MultiHashset)) > min(sample.samples, sample.rows-sample.nulls) {
		return nil, errors.New("sampled NDV hashes exceed the sample or sketch size")
	}
	sketch := &FMSketch{
		hashset:  make(map[uint64]struct{}, retained),
		repeated: make(map[uint64]struct{}, len(pb.MultiHashset)),
		mask:     pb.Mask, maxSize: MaxSketchSize, sample: &sample,
	}
	for _, hashes := range [][]uint64{pb.Hashset, pb.MultiHashset} {
		for _, hash := range hashes {
			if hash&pb.Mask != 0 {
				return nil, errors.New("sampled NDV hash does not match its mask")
			}
			if _, exists := sketch.hashset[hash]; exists {
				return nil, errors.New("duplicate sampled NDV hash")
			}
			sketch.hashset[hash] = struct{}{}
		}
	}
	for _, hash := range pb.MultiHashset {
		delete(sketch.hashset, hash)
		sketch.repeated[hash] = struct{}{}
	}
	return sketch, nil
}

func validNDVRate(rate float64) bool {
	return !math.IsNaN(rate) && rate > 0 && rate <= 1
}

// checkCompatibility rejects mixtures, even if one of the inputs is empty.
func (s *FMSketch) checkCompatibility(other *FMSketch) error {
	if (s.sample == nil) != (other.sample == nil) {
		return fmt.Errorf("cannot merge full-input and sampled NDV sketches: %w", ErrIncompatibleNDV)
	}
	if s.sample != nil && s.sample.rate != other.sample.rate {
		return fmt.Errorf("cannot merge NDVRATE %g and %g: %w", s.sample.rate, other.sample.rate, ErrIncompatibleNDV)
	}
	return nil
}

func (s *FMSketch) sampledNDV() int64 {
	sample := s.sample
	upper := sample.rows - sample.nulls
	if sample.samples == 0 || upper == 0 || len(s.hashset)+len(s.repeated) == 0 {
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
	return int64(max(d, estimate))
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
	// The second byte versions the envelope; the next eight store the rate.
	header := make([]byte, 10, 10+len(data))
	header[1] = 1
	binary.LittleEndian.PutUint64(header[2:], math.Float64bits(sample.rate))
	return append(header, data...), nil
}

func decodeSampledFMSketch(data []byte) (*FMSketch, error) {
	if len(data) < 10 || data[1] != 1 {
		return nil, errors.New("unsupported sampled NDV sketch format")
	}
	rate := math.Float64frombits(binary.LittleEndian.Uint64(data[2:10]))
	if !validNDVRate(rate) || rate == 1 {
		return nil, errors.Errorf("invalid saved NDVRATE %g", rate)
	}
	var collector tipb.RowSampleCollector
	if err := collector.Unmarshal(data[10:]); err != nil {
		return nil, err
	}
	if collector.NdvSampleCount == nil || len(collector.FmSketch) != 1 || len(collector.NullCounts) != 1 {
		return nil, errors.New("invalid saved sampled NDV sketch")
	}
	return newSampledFMSketch(collector.FmSketch[0], ndvSample{
		rate: rate,
		rows: collector.Count, samples: *collector.NdvSampleCount,
		nulls: collector.NullCounts[0],
	})
}
