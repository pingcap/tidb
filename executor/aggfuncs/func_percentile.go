package aggfuncs

import (
	"sort"
	"unsafe"

	"github.com/leiysky/selection"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

const (
	// DefPartialResult4PercentileIntSize is the size of partialResult4PercentileReal
	DefPartialResult4PercentileIntSize = int64(unsafe.Sizeof(partialResult4PercentileInt{}))
	// DefPartialResult4PercentileRealSize is the size of partialResult4PercentileReal
	DefPartialResult4PercentileRealSize = int64(unsafe.Sizeof(partialResult4PercentileReal{}))
	// DefPartialResult4PercentileDecimalSize is the size of partialResult4PercentileInt
	DefPartialResult4PercentileDecimalSize = int64(unsafe.Sizeof(partialResult4PercentileDecimal{}))
	// DefPartialResult4PercentileDurationSize is the size of partialResult4PercentileInt
	DefPartialResult4PercentileDurationSize = int64(unsafe.Sizeof(partialResult4PercentileDuration{}))
)

func percentile(data sort.Interface, percent int) int {
	k := int(float64(data.Len()) * float64(percent) / 100.0)
	return selection.Select(data, k)
}

type basePercentile struct {
	percent int

	baseAggFunc
}

type percentilePartial14Int struct {
	percentileOriginal4Int
}

type percentileFinal4Int struct {
	percentileOriginal4Int
}

type intArray []int64

func (a intArray) Len() int           { return len(a) }
func (a intArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a intArray) Less(i, j int) bool { return a[i] < a[j] }

type partialResult4PercentileInt struct {
	data intArray
}

func (p *partialResult4PercentileInt) MemSize() int64 {
	return DefPartialResult4PercentileIntSize + int64(len(p.data))*DefInt64Size
}

type realArray []float64

func (a realArray) Len() int           { return len(a) }
func (a realArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a realArray) Less(i, j int) bool { return a[i] < a[j] }

type partialResult4PercentileReal struct {
	data realArray
}

func (p *partialResult4PercentileReal) MemSize() int64 {
	return DefPartialResult4PercentileRealSize + int64(len(p.data))*DefFloat64Size
}

type partialResult4PercentileDuration struct {
	data []types.Duration
}

// TODO: use []*types.MyDecimal to prevent massive value copy
type decimalArray []types.MyDecimal

func (a decimalArray) Len() int           { return len(a) }
func (a decimalArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a decimalArray) Less(i, j int) bool { return a[i].Compare(&a[j]) < 0 }

type partialResult4PercentileDecimal struct {
	data decimalArray
}

func (p *partialResult4PercentileDecimal) MemSize() int64 {
	return DefPartialResult4PercentileDecimalSize + int64(len(p.data))*int64(types.MyDecimalStructSize+unsafe.Sizeof(unsafe.Pointer(nil)))
}

type percentileOriginal4Int struct {
	basePercentile
}

func (e *percentileOriginal4Int) AllocPartialResult() (pr PartialResult, memDelta int64) {
	// TODO: Preserve appropriate capacity for data
	pr = PartialResult(&partialResult4PercentileInt{})
	return pr, DefPartialResult4PercentileIntSize
}

func (e *percentileOriginal4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4PercentileInt)(pr)
	p.data = intArray{}
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
		p.data = append(p.data, v)
	}
	endMem := p.MemSize()
	return endMem - startMem, nil
}

func (e *percentileOriginal4Int) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4PercentileInt)(src), (*partialResult4PercentileInt)(dst)
	mergeBuff := make([]int64, len(p1.data)+len(p2.data))
	copy(mergeBuff, p2.data)
	copy(mergeBuff[len(p2.data):], p1.data)
	p1.data = nil
	p2.data = mergeBuff
	return 0, nil
}

func (e *percentileOriginal4Int) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4PercentileInt)(pr)
	if len(p.data) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	index := percentile(p.data, e.percent)
	chk.AppendInt64(e.ordinal, p.data[index])
	return nil
}

type percentileOriginal4Real struct {
	basePercentile
}

func (e *percentileOriginal4Real) AllocPartialResult() (pr PartialResult, memDelta int64) {
	// TODO: Preserve appropriate capacity for data
	pr = PartialResult(&partialResult4PercentileReal{})
	return pr, DefPartialResult4PercentileRealSize
}

func (e *percentileOriginal4Real) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4PercentileReal)(pr)
	p.data = realArray{}
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
		p.data = append(p.data, v)
	}
	endMem := p.MemSize()
	return endMem - startMem, nil
}

func (e *percentileOriginal4Real) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4PercentileReal)(src), (*partialResult4PercentileReal)(dst)
	mergeBuff := make([]float64, len(p1.data)+len(p2.data))
	copy(mergeBuff, p2.data)
	copy(mergeBuff[len(p2.data):], p1.data)
	p1.data = nil
	p2.data = mergeBuff
	return 0, nil
}

func (e *percentileOriginal4Real) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4PercentileReal)(pr)
	if len(p.data) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	index := percentile(p.data, e.percent)
	chk.AppendFloat64(e.ordinal, p.data[index])
	return nil
}

type percentileOriginal4Decimal struct {
	basePercentile
}

func (e *percentileOriginal4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	// TODO: Preserve appropriate capacity for data
	pr = PartialResult(&partialResult4PercentileDecimal{})
	return pr, DefPartialResult4PercentileDecimalSize
}

func (e *percentileOriginal4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4PercentileDecimal)(pr)
	p.data = decimalArray{}
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
		p.data = append(p.data, *v)
	}
	endMem := p.MemSize()
	return endMem - startMem, nil
}

func (e *percentileOriginal4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4PercentileDecimal)(src), (*partialResult4PercentileDecimal)(dst)
	mergeBuff := make([]types.MyDecimal, len(p1.data)+len(p2.data))
	copy(mergeBuff, p2.data)
	copy(mergeBuff[len(p2.data):], p1.data)
	p1.data = nil
	p2.data = mergeBuff
	return 0, nil
}

func (e *percentileOriginal4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4PercentileDecimal)(pr)
	if len(p.data) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	index := percentile(p.data, e.percent)
	chk.AppendMyDecimal(e.ordinal, &p.data[index])
	return nil
}
