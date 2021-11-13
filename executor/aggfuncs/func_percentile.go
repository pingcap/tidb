// Copyright 2020 PingCAP, Inc.
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

package aggfuncs

import (
	"math"
	"sort"
	"unsafe"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/selection"
)

const (
	// DefSliceSize represents size of an empty Slice
	DefSliceSize = int64(unsafe.Sizeof([]interface{}{}))
)

var (
	_ partialResult4Percentile = partialResult4PercentileInt{}
	_ partialResult4Percentile = partialResult4PercentileReal{}
	_ partialResult4Percentile = partialResult4PercentileDecimal{}
	_ partialResult4Percentile = partialResult4PercentileTime{}
	_ partialResult4Percentile = partialResult4PercentileDuration{}
)

func percentile(data sort.Interface, percent int) int {
	// Ordinal rank k = Ceil(P / 100 * N)
	k := int(math.Ceil(float64(data.Len()) * (float64(percent) / 100)))
	if k > data.Len() {
		k = data.Len()
	}
	return selection.Select(data, k)
}

type basePercentile struct {
	percent int

	baseAggFunc
}

func (e *basePercentile) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return
}

func (e *basePercentile) ResetPartialResult(pr PartialResult) {}

func (e *basePercentile) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	return
}

func (e *basePercentile) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	chk.AppendNull(e.ordinal)
	return nil
}

type partialResult4Percentile interface {
	sort.Interface

	MemSize() int64
}

type partialResult4PercentileInt []int64

func (p partialResult4PercentileInt) Len() int           { return len(p) }
func (p partialResult4PercentileInt) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p partialResult4PercentileInt) Less(i, j int) bool { return p[i] < p[j] }

func (p partialResult4PercentileInt) MemSize() int64 {
	return DefSliceSize + int64(len(p))*DefInt64Size
}

type partialResult4PercentileReal []float64

func (p partialResult4PercentileReal) Len() int           { return len(p) }
func (p partialResult4PercentileReal) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p partialResult4PercentileReal) Less(i, j int) bool { return p[i] < p[j] }

func (p partialResult4PercentileReal) MemSize() int64 {
	return DefSliceSize + int64(len(p))*DefFloat64Size
}

// TODO: use []*types.MyDecimal to prevent massive value copy
type partialResult4PercentileDecimal []types.MyDecimal

func (p partialResult4PercentileDecimal) Len() int           { return len(p) }
func (p partialResult4PercentileDecimal) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p partialResult4PercentileDecimal) Less(i, j int) bool { return p[i].Compare(&p[j]) < 0 }

func (p partialResult4PercentileDecimal) MemSize() int64 {
	return DefSliceSize + int64(len(p))*int64(types.MyDecimalStructSize)
}

type partialResult4PercentileTime []types.Time

func (p partialResult4PercentileTime) Len() int           { return len(p) }
func (p partialResult4PercentileTime) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p partialResult4PercentileTime) Less(i, j int) bool { return p[i].Compare(p[j]) < 0 }

func (p partialResult4PercentileTime) MemSize() int64 {
	return DefSliceSize + int64(len(p))*DefInt64Size
}

type partialResult4PercentileDuration []types.Duration

func (p partialResult4PercentileDuration) Len() int           { return len(p) }
func (p partialResult4PercentileDuration) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p partialResult4PercentileDuration) Less(i, j int) bool { return p[i].Compare(p[j]) < 0 }

func (p partialResult4PercentileDuration) MemSize() int64 {
	return DefSliceSize + int64(len(p))*DefInt64Size
}

type percentileOriginal4Int struct {
	basePercentile
}

func (e *percentileOriginal4Int) AllocPartialResult() (pr PartialResult, memDelta int64) {
	// TODO: Preserve appropriate capacity for data
	pr = PartialResult(&partialResult4PercentileInt{})
	return pr, DefSliceSize
}

func (e *percentileOriginal4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4PercentileInt)(pr)
	*p = partialResult4PercentileInt{}
}

func (e *percentileOriginal4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4PercentileInt)(pr)
	startMem := p.MemSize()
	for _, row := range rowsInGroup {
		v, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		*p = append(*p, v)
	}
	endMem := p.MemSize()
	return endMem - startMem, nil
}

func (e *percentileOriginal4Int) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4PercentileInt)(src), (*partialResult4PercentileInt)(dst)
	mergeBuff := make([]int64, len(*p1)+len(*p2))
	copy(mergeBuff, *p2)
	copy(mergeBuff[len(*p2):], *p1)
	*p1 = nil
	*p2 = mergeBuff
	return 0, nil
}

func (e *percentileOriginal4Int) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4PercentileInt)(pr)
	if len(*p) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	index := percentile(p, e.percent)
	chk.AppendInt64(e.ordinal, (*p)[index])
	return nil
}

type percentileOriginal4Real struct {
	basePercentile
}

func (e *percentileOriginal4Real) AllocPartialResult() (pr PartialResult, memDelta int64) {
	// TODO: Preserve appropriate capacity for data
	pr = PartialResult(&partialResult4PercentileReal{})
	return pr, DefSliceSize
}

func (e *percentileOriginal4Real) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4PercentileReal)(pr)
	*p = partialResult4PercentileReal{}
}

func (e *percentileOriginal4Real) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4PercentileReal)(pr)
	startMem := p.MemSize()
	for _, row := range rowsInGroup {
		v, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		*p = append(*p, v)
	}
	endMem := p.MemSize()
	return endMem - startMem, nil
}

func (e *percentileOriginal4Real) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4PercentileReal)(src), (*partialResult4PercentileReal)(dst)
	mergeBuff := make([]float64, len(*p1)+len(*p2))
	copy(mergeBuff, *p2)
	copy(mergeBuff[len(*p2):], *p1)
	*p1 = nil
	*p2 = mergeBuff
	return 0, nil
}

func (e *percentileOriginal4Real) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4PercentileReal)(pr)
	if len(*p) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	index := percentile(*p, e.percent)
	chk.AppendFloat64(e.ordinal, (*p)[index])
	return nil
}

type percentileOriginal4Decimal struct {
	basePercentile
}

func (e *percentileOriginal4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	// TODO: Preserve appropriate capacity for data
	pr = PartialResult(&partialResult4PercentileDecimal{})
	return pr, DefSliceSize
}

func (e *percentileOriginal4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4PercentileDecimal)(pr)
	*p = partialResult4PercentileDecimal{}
}

func (e *percentileOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4PercentileDecimal)(pr)
	startMem := p.MemSize()
	for _, row := range rowsInGroup {
		v, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		*p = append(*p, *v)
	}
	endMem := p.MemSize()
	return endMem - startMem, nil
}

func (e *percentileOriginal4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4PercentileDecimal)(src), (*partialResult4PercentileDecimal)(dst)
	mergeBuff := make([]types.MyDecimal, len(*p1)+len(*p2))
	copy(mergeBuff, *p2)
	copy(mergeBuff[len(*p2):], *p1)
	*p1 = nil
	*p2 = mergeBuff
	return 0, nil
}

func (e *percentileOriginal4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4PercentileDecimal)(pr)
	if len(*p) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	index := percentile(*p, e.percent)
	chk.AppendMyDecimal(e.ordinal, &(*p)[index])
	return nil
}

type percentileOriginal4Time struct {
	basePercentile
}

func (e *percentileOriginal4Time) AllocPartialResult() (pr PartialResult, memDelta int64) {
	// TODO: Preserve appropriate capacity for data
	pr = PartialResult(&partialResult4PercentileTime{})
	return pr, DefSliceSize
}

func (e *percentileOriginal4Time) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4PercentileTime)(pr)
	*p = partialResult4PercentileTime{}
}

func (e *percentileOriginal4Time) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4PercentileTime)(pr)
	startMem := p.MemSize()
	for _, row := range rowsInGroup {
		v, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		*p = append(*p, v)
	}
	endMem := p.MemSize()
	return endMem - startMem, nil
}

func (e *percentileOriginal4Time) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4PercentileTime)(src), (*partialResult4PercentileTime)(dst)
	mergeBuff := make(partialResult4PercentileTime, len(*p1)+len(*p2))
	copy(mergeBuff, *p2)
	copy(mergeBuff[len(*p2):], *p1)
	*p1 = nil
	*p2 = mergeBuff
	return 0, nil
}

func (e *percentileOriginal4Time) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4PercentileTime)(pr)
	if len(*p) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	index := percentile(p, e.percent)
	chk.AppendTime(e.ordinal, (*p)[index])
	return nil
}

type percentileOriginal4Duration struct {
	basePercentile
}

func (e *percentileOriginal4Duration) AllocPartialResult() (pr PartialResult, memDelta int64) {
	// TODO: Preserve appropriate capacity for data
	pr = PartialResult(&partialResult4PercentileTime{})
	return pr, DefSliceSize
}

func (e *percentileOriginal4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4PercentileDuration)(pr)
	*p = partialResult4PercentileDuration{}
}

func (e *percentileOriginal4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4PercentileDuration)(pr)
	startMem := p.MemSize()
	for _, row := range rowsInGroup {
		v, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		*p = append(*p, v)
	}
	endMem := p.MemSize()
	return endMem - startMem, nil
}

func (e *percentileOriginal4Duration) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4PercentileDuration)(src), (*partialResult4PercentileDuration)(dst)
	mergeBuff := make(partialResult4PercentileDuration, len(*p1)+len(*p2))
	copy(mergeBuff, *p2)
	copy(mergeBuff[len(*p2):], *p1)
	*p1 = nil
	*p2 = mergeBuff
	return 0, nil
}
func (e *percentileOriginal4Duration) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4PercentileDuration)(pr)
	if len(*p) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	index := percentile(*p, e.percent)

	chk.AppendDuration(e.ordinal, (*p)[index])
	return nil
}
