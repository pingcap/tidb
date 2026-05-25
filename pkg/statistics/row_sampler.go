// Copyright 2021 PingCAP, Inc.
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
	"container/heap"
	"context"
	"maps"
	"math/rand"
	"slices"
	"unsafe"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
)

// RowSampleCollector implements the needed interface for a row-based sample collector.
type RowSampleCollector interface {
	MergeCollector(collector RowSampleCollector)
	sampleRow(row []types.Datum, rng *rand.Rand)
	Base() *baseCollector
	DestroyAndPutToPool()
}

type baseCollector struct {
	Samples   WeightedRowSampleHeap
	NullCount []int64
	// FMSketches holds the per-column FM sketch used to estimate NDV.
	FMSketches []*FMSketch
	// SingletonSketches holds the per-column sketch of values seen exactly once in
	// the sketch sub-sample; it recovers population NDV when
	// NDVSampleRate < NDVSampleSkipRate and is empty otherwise. A merged collector
	// keeps per-region sketches in RegionSketchSummaries instead.
	SingletonSketches []*FMSketch
	// RegionSketchSummaries holds one summary per region merged into this collector
	// (see RegionSketchSummary); it is nil on a leaf collector or when singleton
	// sketches are not collected.
	//
	// It grows with the region count and is not counted in MemSize, so the consumer
	// must bound and account for it.
	RegionSketchSummaries []RegionSketchSummary
	TotalSizes            []int64
	Count                 int64
	// SketchSampleCount is the number of rows fed into FMSketches and
	// SingletonSketches.
	// It rescales NullCount and TotalSizes to the full population;
	// 0 means no sub-sampling.
	SketchSampleCount int64
	MemSize           int64
}

// RegionSketchSummary holds one region's NDV and singleton sketches and the row
// count that fed them. Regions are kept separate and never unioned: unioning the
// singleton sketches would break "seen exactly once" — a value that is a singleton
// in two regions occurs twice overall — and inflate the global singleton count.
// EstimateGlobalSingletonBySketches consumes the regions separately to avoid that.
type RegionSketchSummary struct {
	// NDVSketches is the region's per-column distinct-value sketch.
	NDVSketches []*FMSketch
	// SingletonSketches is the region's per-column sketch of values seen exactly
	// once in the region.
	SingletonSketches []*FMSketch
	// SketchSampleCount is the number of rows that fed these sketches.
	SketchSampleCount int64
}

// ReservoirRowSampleCollector collects the samples from the source and organize the samples by row.
// It will maintain the following things:
//
//	Row samples.
//	FM sketches(To calculate the NDV).
//	Null counts.
//	The data sizes.
//	The number of rows.
//
// It uses weighted reservoir sampling(A-Res) to do the sampling.
type ReservoirRowSampleCollector struct {
	*baseCollector
	MaxSampleSize int
}

// singletonSketchBuilder partitions hash values into singletons (seen exactly
// once) and the rest. Only singletons feed the final FM sketch, which is the
// basis for population NDV estimation under sub-sampling.
type singletonSketchBuilder struct {
	once     map[uint64]struct{}
	multiple map[uint64]struct{}
}

func newSingletonSketchBuilder() *singletonSketchBuilder {
	return &singletonSketchBuilder{
		once:     make(map[uint64]struct{}),
		multiple: make(map[uint64]struct{}),
	}
}

func (s *singletonSketchBuilder) insertHashValue(hashVal uint64) {
	if _, ok := s.multiple[hashVal]; ok {
		return
	}
	if _, ok := s.once[hashVal]; ok {
		delete(s.once, hashVal)
		s.multiple[hashVal] = struct{}{}
	} else {
		s.once[hashVal] = struct{}{}
	}
}

func (s *singletonSketchBuilder) insertValue(sc *stmtctx.StatementContext, value types.Datum) error {
	hashVal, err := hashDatum(sc, value)
	if err != nil {
		return err
	}
	s.insertHashValue(hashVal)
	return nil
}

func (s *singletonSketchBuilder) insertRowValue(sc *stmtctx.StatementContext, values []types.Datum) error {
	hashVal, err := hashRow(sc, values)
	if err != nil {
		return err
	}
	s.insertHashValue(hashVal)
	return nil
}

func (s *singletonSketchBuilder) clone() *singletonSketchBuilder {
	if s == nil {
		return nil
	}
	return &singletonSketchBuilder{
		once:     maps.Clone(s.once),
		multiple: maps.Clone(s.multiple),
	}
}

func (s *singletonSketchBuilder) build(maxSketchSize int) *FMSketch {
	if s == nil {
		return nil
	}
	intest.Assert(maxSketchSize > 0, "maxSketchSize should be greater than 0")
	sketch := NewFMSketch(maxSketchSize)
	for val := range s.once {
		sketch.insertHashValue(val)
	}
	return sketch
}

// ReservoirRowSampleItem is the item for the ReservoirRowSampleCollector. The weight is needed for the sampling algorithm.
type ReservoirRowSampleItem struct {
	Handle  kv.Handle
	Columns []types.Datum
	Weight  int64
}

// EmptyReservoirSampleItemSize = (24 + 16 + 8) now.
const EmptyReservoirSampleItemSize = int64(unsafe.Sizeof(ReservoirRowSampleItem{}))

// ShouldBuildSingletonSketches reports whether the configured NDV sample rate
// requires building per-region singleton sketches (rate < NDVSampleSkipRate means sketches are
// collected from a subset of rows and a global NDV estimate is derived from
// the singletons).
func ShouldBuildSingletonSketches(rate float64) bool {
	return rate < NDVSampleSkipRate
}

// MemUsage returns the memory usage of sample item.
func (i ReservoirRowSampleItem) MemUsage() (sum int64) {
	sum = EmptyReservoirSampleItemSize
	for _, col := range i.Columns {
		sum += col.MemUsage()
	}
	if i.Handle != nil {
		sum += int64(i.Handle.MemUsage())
	}
	return sum
}

// WeightedRowSampleHeap implements the Heap interface.
type WeightedRowSampleHeap []*ReservoirRowSampleItem

// Len implements the Heap interface.
func (h WeightedRowSampleHeap) Len() int {
	return len(h)
}

// Swap implements the Heap interface.
func (h WeightedRowSampleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Less implements the Heap interface.
func (h WeightedRowSampleHeap) Less(i, j int) bool {
	return h[i].Weight < h[j].Weight
}

// Push implements the Heap interface.
func (h *WeightedRowSampleHeap) Push(i any) {
	*h = append(*h, i.(*ReservoirRowSampleItem))
}

// Pop implements the Heap interface.
func (h *WeightedRowSampleHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// RowSampleBuilder is used to construct the ReservoirRowSampleCollector to get the samples.
type RowSampleBuilder struct {
	RecordSet     sqlexec.RecordSet
	Sc            *stmtctx.StatementContext
	Rng           *rand.Rand
	ColsFieldType []*types.FieldType
	Collators     []collate.Collator
	ColGroups     [][]int64
	MaxSampleSize int
	// SampleRate is the per-row keep probability for the row sample (the rows that
	// build histograms and TopN). It drives Bernoulli sampling only when
	// MaxSampleSize is 0; otherwise reservoir sampling is used and this is ignored.
	SampleRate float64
	// NDVSampleRate is the per-row keep probability for the per-column aggregates:
	// FMsketches, NULL counts, and TotalSizes. NULL counts and TotalSizes are
	// rescaled back to the full population afterwards. It is in (0, 1] and uses a
	// separate per-row draw from SampleRate. Below NDVSampleSkipRate (1) the
	// sketches see only a sub-sample, so singleton sketches are built to estimate
	// population NDV from it.
	NDVSampleRate   float64
	MaxFMSketchSize int
}

// NewRowSampleCollector creates a collector from the given inputs.
func NewRowSampleCollector(maxSampleSize int, sampleRate float64, totalLen int) RowSampleCollector {
	if maxSampleSize > 0 {
		return NewReservoirRowSampleCollector(maxSampleSize, totalLen)
	}
	if sampleRate > 0 {
		return NewBernoulliRowSampleCollector(sampleRate, totalLen)
	}
	return nil
}

// NewReservoirRowSampleCollector creates the new collector by the given inputs.
func NewReservoirRowSampleCollector(maxSampleSize int, totalLen int) *ReservoirRowSampleCollector {
	base := &baseCollector{
		Samples:           make(WeightedRowSampleHeap, 0, maxSampleSize),
		NullCount:         make([]int64, totalLen),
		FMSketches:        make([]*FMSketch, 0, totalLen),
		SingletonSketches: make([]*FMSketch, 0, totalLen),
		TotalSizes:        make([]int64, totalLen),
	}
	return &ReservoirRowSampleCollector{
		baseCollector: base,
		MaxSampleSize: maxSampleSize,
	}
}

// Collect first builds the collector. Then maintain the null count, FM sketch and the data size for each column and
// column group.
// Then use the weighted reservoir sampling to collect the samples.
func (s *RowSampleBuilder) Collect() (RowSampleCollector, error) {
	totalLen := len(s.ColsFieldType) + len(s.ColGroups)
	collector := NewRowSampleCollector(s.MaxSampleSize, s.SampleRate, totalLen)
	ndvSampleRate := s.NDVSampleRate
	intest.Assert(ndvSampleRate > 0 && ndvSampleRate <= NDVSampleSkipRate, "NDVSampleRate must be in (0, 1]")
	buildSingletons := ShouldBuildSingletonSketches(ndvSampleRate)
	var singletonBuilders []*singletonSketchBuilder
	if buildSingletons {
		singletonBuilders = make([]*singletonSketchBuilder, 0, totalLen)
	}
	for range totalLen {
		collector.Base().FMSketches = append(collector.Base().FMSketches, NewFMSketch(s.MaxFMSketchSize))
		if buildSingletons {
			singletonBuilders = append(singletonBuilders, newSingletonSketchBuilder())
		}
	}
	ctx := context.TODO()
	chk := s.RecordSet.NewChunk(nil)
	it := chunk.NewIterator4Chunk(chk)
	for {
		err := s.RecordSet.Next(ctx, chk)
		if err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			break
		}
		collector.Base().Count += int64(chk.NumRows())
		for row := it.Begin(); row != it.End(); row = it.Next() {
			datums := RowToDatums(row, s.RecordSet.Fields())
			newCols := make([]types.Datum, len(datums))
			// sizes are used to calculate the total size information. We calculate the sizes here because we need the
			// length of the original bytes instead of the collate key when it's a new collation string.
			sizes := make([]int64, 0, len(datums))
			for i := range datums {
				datums[i].Copy(&newCols[i])
				sizes = append(sizes, int64(len(datums[i].GetBytes())))
			}

			for i, val := range datums {
				// For string values, we use the collation key instead of the original value.
				if s.Collators[i] != nil && !val.IsNull() {
					decodedVal, err := tablecodec.DecodeColumnValue(val.GetBytes(), s.ColsFieldType[i], s.Sc.TimeZone())
					if err != nil {
						return nil, err
					}
					decodedVal.SetBytesAsString(s.Collators[i].Key(decodedVal.GetString()), decodedVal.Collation(), uint32(decodedVal.Length()))
					encodedKey, err := tablecodec.EncodeValue(s.Sc.TimeZone(), nil, decodedVal)
					err = s.Sc.HandleError(err)
					if err != nil {
						return nil, err
					}
					datums[i].SetBytes(encodedKey)
				}
			}
			collectSketch := !buildSingletons || s.Rng.Float64() < ndvSampleRate
			if collectSketch {
				if err := collector.Base().collectColumns(s.Sc, datums, sizes, singletonBuilders); err != nil {
					return nil, err
				}
				if err := collector.Base().collectColumnGroups(s.Sc, datums, s.ColGroups, sizes, singletonBuilders); err != nil {
					return nil, err
				}
				if buildSingletons {
					collector.Base().SketchSampleCount++
				}
			}
			collector.sampleRow(newCols, s.Rng)
		}
	}
	// NDV sub-sampling only tallies NullCount/TotalSizes for rows that passed
	// the rate check. Rescale them back to per-population estimates before the
	// single-column-group copy below picks them up.
	collector.Base().rescaleNullCountAndTotalSizes()
	for i, group := range s.ColGroups {
		if len(group) != 1 {
			continue
		}
		// For the single-column group, its FMSketch is the same as that of the corresponding column. Hence, we don't
		// maintain its FMSketch in collectColumnGroups. We just copy the corresponding column's FMSketch after
		// iterating all rows. Also, we can directly copy TotalSize and NullCount.
		colIdx := group[0]
		colGroupIdx := len(s.ColsFieldType) + i
		collector.Base().FMSketches[colGroupIdx] = collector.Base().FMSketches[colIdx].Copy()
		if buildSingletons {
			singletonBuilders[colGroupIdx] = singletonBuilders[colIdx].clone()
		}
		collector.Base().NullCount[colGroupIdx] = collector.Base().NullCount[colIdx]
		collector.Base().TotalSizes[colGroupIdx] = collector.Base().TotalSizes[colIdx]
	}
	if buildSingletons {
		collector.Base().buildSingletonSketches(singletonBuilders, s.MaxFMSketchSize)
	}
	return collector, nil
}

func (s *baseCollector) destroyAndPutToPool() {
	s.FMSketches = nil // Release for GC.
	s.SingletonSketches = nil
	s.RegionSketchSummaries = nil
}

func (s *baseCollector) collectColumns(
	sc *stmtctx.StatementContext,
	cols []types.Datum,
	sizes []int64,
	singletonBuilders []*singletonSketchBuilder,
) error {
	collectSingletonSketch := singletonBuilders != nil
	for i, col := range cols {
		if col.IsNull() {
			s.NullCount[i]++
			continue
		}
		// Minus one is to remove the flag byte.
		s.TotalSizes[i] += sizes[i] - 1
		if err := s.FMSketches[i].InsertValue(sc, col); err != nil {
			return err
		}
		if collectSingletonSketch {
			if err := singletonBuilders[i].insertValue(sc, col); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *baseCollector) collectColumnGroups(
	sc *stmtctx.StatementContext,
	cols []types.Datum,
	colGroups [][]int64,
	sizes []int64,
	singletonBuilders []*singletonSketchBuilder,
) error {
	colLen := len(cols)
	collectSingletonSketch := singletonBuilders != nil
	datumBuffer := make([]types.Datum, 0, len(cols))
	for i, group := range colGroups {
		if len(group) == 1 {
			// For the single-column group, its FMSketch is the same as that of the corresponding column. Hence, we
			// don't need to maintain its FMSketch. We just copy the corresponding column's FMSketch after iterating
			// all rows. Also, we can directly copy TotalSize and NullCount.
			continue
		}
		// We don't maintain the null counts information for the multi-column group.
		datumBuffer = datumBuffer[:0]
		for _, c := range group {
			datumBuffer = append(datumBuffer, cols[c])
			if !cols[c].IsNull() {
				s.TotalSizes[colLen+i] += sizes[c] - 1
			}
		}
		if err := s.FMSketches[colLen+i].InsertRowValue(sc, datumBuffer); err != nil {
			return err
		}
		if collectSingletonSketch {
			if err := singletonBuilders[colLen+i].insertRowValue(sc, datumBuffer); err != nil {
				return err
			}
		}
	}
	return nil
}

// rescaleSampledValue scales `sampled` (accumulated over `sampleCount` rows)
// into an estimate over `totalRowCount` rows using round-half-up division.
// Only used in the mock store path (unistore cophandler), so int64 is wide
// enough for any realistic test fixture. Non-positive inputs return 0.
func rescaleSampledValue(sampled, totalRowCount, sampleCount int64) int64 {
	intest.Assert(sampleCount > 0, "sampleCount must be positive")
	if sampled <= 0 {
		return 0
	}
	// Round-half-up integer division: floor(a*b/c + 0.5).
	return (sampled*totalRowCount + sampleCount/2) / sampleCount
}

// rescaleNullCountAndTotalSizes converts per-column null counts and total sizes
// gathered from the NDV sub-sample into estimates over the full row population.
// Count itself is exact (TiKV reports it from scanned_rows_per_range; the
// mock store has no scan-level sampling) and is only read here as the
// scaling divisor — it is never modified. No-op when no sub-sampling occurred
// (sampleCount == 0 or every row was sampled).
func (s *baseCollector) rescaleNullCountAndTotalSizes() {
	sampleCount := s.SketchSampleCount
	totalRowCount := s.Count
	// sampleCount > totalRowCount would scale stats *down* and corrupt them.
	intest.Assert(totalRowCount >= sampleCount, "totalRowCount must be bigger than or equal to sampleCount")
	// No sub-sampling: values are already exact.
	if sampleCount <= 0 {
		return
	}
	// Scaling factor is 1.0; nothing to do.
	if totalRowCount == sampleCount {
		return
	}
	for i, nc := range s.NullCount {
		s.NullCount[i] = rescaleSampledValue(nc, totalRowCount, sampleCount)
	}
	for i, ts := range s.TotalSizes {
		s.TotalSizes[i] = rescaleSampledValue(ts, totalRowCount, sampleCount)
	}
}

func (s *baseCollector) buildSingletonSketches(singletonBuilders []*singletonSketchBuilder, maxSketchSize int) {
	s.SingletonSketches = make([]*FMSketch, len(singletonBuilders))
	for i, builder := range singletonBuilders {
		s.SingletonSketches[i] = builder.build(maxSketchSize)
	}
}

// collectRegionSketchSummaries records the sub-collector's per-region sketches on
// this collector instead of unioning them; see RegionSketchSummary for why.
func (s *baseCollector) collectRegionSketchSummaries(sub *baseCollector) {
	// An already-merged sub-collector carries its own summaries; concatenate them.
	if len(sub.RegionSketchSummaries) > 0 {
		s.RegionSketchSummaries = append(s.RegionSketchSummaries, sub.RegionSketchSummaries...)
		return
	}
	// A leaf sub-collector is one region; skip it when it has no singleton sketches.
	if len(sub.SingletonSketches) == 0 {
		return
	}
	// Reference the region's sketches instead of copying them — they are not mutated
	// after the merge. slices.Clone duplicates only the pointer slice, not the maps.
	s.RegionSketchSummaries = append(s.RegionSketchSummaries, RegionSketchSummary{
		NDVSketches:       slices.Clone(sub.FMSketches),
		SingletonSketches: slices.Clone(sub.SingletonSketches),
		SketchSampleCount: sub.SketchSampleCount,
	})
}

// ToProto converts the collector to pb struct.
func (s *baseCollector) ToProto() *tipb.RowSampleCollector {
	pbFMSketches := make([]*tipb.FMSketch, 0, len(s.FMSketches))
	for _, sketch := range s.FMSketches {
		pbFMSketches = append(pbFMSketches, FMSketchToProto(sketch))
	}
	pbSingletonSketches := make([]*tipb.FMSketch, 0, len(s.SingletonSketches))
	for _, sketch := range s.SingletonSketches {
		pbSingletonSketches = append(pbSingletonSketches, FMSketchToProto(sketch))
	}
	collector := &tipb.RowSampleCollector{
		Samples:           RowSamplesToProto(s.Samples),
		NullCounts:        s.NullCount,
		Count:             s.Count,
		FmSketch:          pbFMSketches,
		TotalSize:         s.TotalSizes,
		SingletonSketch:   pbSingletonSketches,
		SketchSampleCount: s.SketchSampleCount,
	}
	return collector
}

func (s *baseCollector) FromProto(pbCollector *tipb.RowSampleCollector, memTracker *memory.Tracker) {
	s.Count = pbCollector.Count
	s.NullCount = pbCollector.NullCounts
	s.FMSketches = make([]*FMSketch, 0, len(pbCollector.FmSketch))
	s.SketchSampleCount = pbCollector.GetSketchSampleCount()
	for _, pbSketch := range pbCollector.FmSketch {
		s.FMSketches = append(s.FMSketches, FMSketchFromProto(pbSketch))
	}
	s.SingletonSketches = make([]*FMSketch, 0, len(pbCollector.GetSingletonSketch()))
	for _, pbSketch := range pbCollector.GetSingletonSketch() {
		s.SingletonSketches = append(s.SingletonSketches, FMSketchFromProto(pbSketch))
	}
	s.TotalSizes = pbCollector.TotalSize
	sampleNum := len(pbCollector.Samples)
	s.Samples = make(WeightedRowSampleHeap, 0, sampleNum)
	// consume mandatory memory at the beginning, including all empty ReservoirRowSampleItems and all empty Datums of all sample rows, if exceeds, fast fail
	if len(pbCollector.Samples) > 0 {
		rowLen := len(pbCollector.Samples[0].Row)
		// 8 is the size of reference
		initMemSize := int64(sampleNum) * (int64(rowLen)*types.EmptyDatumSize + EmptyReservoirSampleItemSize + 8)
		s.MemSize += initMemSize
		memTracker.Consume(initMemSize)
	}
	bufferedMemSize := int64(0)
	for _, pbSample := range pbCollector.Samples {
		rowLen := len(pbSample.Row)
		data := make([]types.Datum, 0, rowLen)
		for _, col := range pbSample.Row {
			data = append(data, types.NewBytesDatum(col))
		}
		// Directly copy the weight.
		sampleItem := &ReservoirRowSampleItem{Columns: data, Weight: pbSample.Weight}
		s.Samples = append(s.Samples, sampleItem)
		deltaSize := sampleItem.MemUsage() - EmptyReservoirSampleItemSize - int64(rowLen)*types.EmptyDatumSize
		memTracker.BufferedConsume(&bufferedMemSize, deltaSize)
		s.MemSize += deltaSize
	}
	memTracker.Consume(bufferedMemSize)
}

// Base implements the RowSampleCollector interface.
func (s *ReservoirRowSampleCollector) Base() *baseCollector {
	return s.baseCollector
}

func (s *ReservoirRowSampleCollector) sampleZippedRow(sample *ReservoirRowSampleItem) {
	if len(s.Samples) < s.MaxSampleSize {
		s.Samples = append(s.Samples, sample)
		if len(s.Samples) == s.MaxSampleSize {
			heap.Init(&s.Samples)
		}
		return
	}
	if s.Samples[0].Weight < sample.Weight {
		s.Samples[0] = sample
		heap.Fix(&s.Samples, 0)
	}
}

func (s *ReservoirRowSampleCollector) sampleRow(row []types.Datum, rng *rand.Rand) {
	weight := rng.Int63()
	if len(s.Samples) < s.MaxSampleSize {
		s.Samples = append(s.Samples, &ReservoirRowSampleItem{
			Columns: row,
			Weight:  weight,
		})
		if len(s.Samples) == s.MaxSampleSize {
			heap.Init(&s.Samples)
		}
		return
	}
	if s.Samples[0].Weight < weight {
		s.Samples[0] = &ReservoirRowSampleItem{
			Columns: row,
			Weight:  weight,
		}
		heap.Fix(&s.Samples, 0)
	}
}

// MergeCollector merges the collectors to a final one.
func (s *ReservoirRowSampleCollector) MergeCollector(subCollector RowSampleCollector) {
	s.Count += subCollector.Base().Count
	s.SketchSampleCount += subCollector.Base().SketchSampleCount
	for i, fms := range subCollector.Base().FMSketches {
		s.FMSketches[i].MergeFMSketch(fms)
	}
	s.collectRegionSketchSummaries(subCollector.Base())
	for i, nullCount := range subCollector.Base().NullCount {
		s.NullCount[i] += nullCount
	}
	for i, totSize := range subCollector.Base().TotalSizes {
		s.TotalSizes[i] += totSize
	}
	oldSampleNum := len(s.Samples)
	for _, sample := range subCollector.Base().Samples {
		s.sampleZippedRow(sample)
	}
	subSampleNum := len(subCollector.Base().Samples)
	newSampleNum := len(s.Samples)
	totalSampleNum := oldSampleNum + subSampleNum
	if totalSampleNum == 0 {
		s.MemSize = 0
	} else {
		s.MemSize = (s.MemSize + subCollector.Base().MemSize) * int64(newSampleNum) / int64(totalSampleNum)
	}
}

// DestroyAndPutToPool implements the interface RowSampleCollector.
func (s *ReservoirRowSampleCollector) DestroyAndPutToPool() {
	s.baseCollector.destroyAndPutToPool()
}

// RowSamplesToProto converts the samp slice to the pb struct.
func RowSamplesToProto(samples WeightedRowSampleHeap) []*tipb.RowSample {
	if len(samples) == 0 {
		return nil
	}
	rows := make([]*tipb.RowSample, 0, len(samples))
	colLen := len(samples[0].Columns)
	for _, sample := range samples {
		pbRow := &tipb.RowSample{
			Row:    make([][]byte, 0, colLen),
			Weight: sample.Weight,
		}
		for _, c := range sample.Columns {
			if c.IsNull() {
				pbRow.Row = append(pbRow.Row, []byte{codec.NilFlag})
				continue
			}
			pbRow.Row = append(pbRow.Row, c.GetBytes())
		}
		rows = append(rows, pbRow)
	}
	return rows
}

// BernoulliRowSampleCollector collects the samples from the source and organize the sample by row.
// It will maintain the following things:
//
//	Row samples.
//	FM sketches(To calculate the NDV).
//	Null counts.
//	The data sizes.
//	The number of rows.
//
// It uses the bernoulli sampling to collect the data.
type BernoulliRowSampleCollector struct {
	*baseCollector
	SampleRate float64
}

// NewBernoulliRowSampleCollector creates the new collector by the given inputs.
func NewBernoulliRowSampleCollector(sampleRate float64, totalLen int) *BernoulliRowSampleCollector {
	base := &baseCollector{
		Samples:           make(WeightedRowSampleHeap, 0, 8),
		NullCount:         make([]int64, totalLen),
		FMSketches:        make([]*FMSketch, 0, totalLen),
		SingletonSketches: make([]*FMSketch, 0, totalLen),
		TotalSizes:        make([]int64, totalLen),
	}
	return &BernoulliRowSampleCollector{
		baseCollector: base,
		SampleRate:    sampleRate,
	}
}

func (s *BernoulliRowSampleCollector) sampleRow(row []types.Datum, rng *rand.Rand) {
	if rng.Float64() > s.SampleRate {
		return
	}
	s.baseCollector.Samples = append(s.baseCollector.Samples, &ReservoirRowSampleItem{
		Columns: row,
		Weight:  0,
	})
}

// MergeCollector merges the collectors to a final one.
func (s *BernoulliRowSampleCollector) MergeCollector(subCollector RowSampleCollector) {
	s.Count += subCollector.Base().Count
	s.SketchSampleCount += subCollector.Base().SketchSampleCount
	for i := range subCollector.Base().FMSketches {
		s.FMSketches[i].MergeFMSketch(subCollector.Base().FMSketches[i])
	}
	s.collectRegionSketchSummaries(subCollector.Base())
	for i := range subCollector.Base().NullCount {
		s.NullCount[i] += subCollector.Base().NullCount[i]
	}
	for i := range subCollector.Base().TotalSizes {
		s.TotalSizes[i] += subCollector.Base().TotalSizes[i]
	}
	s.baseCollector.Samples = append(s.baseCollector.Samples, subCollector.Base().Samples...)
	s.MemSize += subCollector.Base().MemSize
}

// Base implements the interface RowSampleCollector.
func (s *BernoulliRowSampleCollector) Base() *baseCollector {
	return s.baseCollector
}

// DestroyAndPutToPool implements the interface RowSampleCollector.
func (s *BernoulliRowSampleCollector) DestroyAndPutToPool() {
	s.baseCollector.destroyAndPutToPool()
}
