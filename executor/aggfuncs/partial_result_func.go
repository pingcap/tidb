package aggfuncs

import (
	"reflect"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// PartialResultMemoryTracker interface.
type PartialResultMemoryTracker interface {
	MemoryUsage(result PartialResult) int64
}

// PartialResultCoder interface.
type PartialResultCoder interface {
	LoadFrom([]byte) (PartialResult, []byte, error)
	DumpTo(PartialResult, []byte) ([]byte, error)
}

const partialResult4SumFloat64Size = int64(unsafe.Sizeof(partialResult4SumFloat64{}))

// MemoryUsage implements interface.
func (e *sum4Float64) MemoryUsage(result PartialResult) int64 {
	return partialResult4SumFloat64Size
}

// LoadFrom implements interface.
func (e *sum4Float64) LoadFrom(buf []byte) (PartialResult, []byte, error) {
	p := new(partialResult4SumFloat64)
	var err error
	buf, p.val, err = codec.DecodeFloat(buf)
	if err != nil {
		return nil, nil, err
	}
	buf, p.notNullRowCount, err = codec.DecodeInt(buf)
	return PartialResult(p), buf, err
}

// DumpTo implements interface.
func (e *sum4Float64) DumpTo(pr PartialResult, buf []byte) ([]byte, error) {
	p := (*partialResult4SumFloat64)(pr)
	buf = codec.EncodeFloat(buf, p.val)
	buf = codec.EncodeInt(buf, p.notNullRowCount)
	return buf, nil
}

// MemoryUsage implements interface.
func (e *baseCount) MemoryUsage(result PartialResult) int64 {
	return DefPartialResult4CountSize
}

// LoadFrom implements interface.
func (e *baseCount) LoadFrom(buf []byte) (PartialResult, []byte, error) {
	p := new(partialResult4Count)
	var err error
	buf, *p, err = codec.DecodeInt(buf)
	return PartialResult(p), buf, err
}

// DumpTo implements interface.
func (e *baseCount) DumpTo(pr PartialResult, buf []byte) ([]byte, error) {
	p := (*partialResult4Count)(pr)
	buf = codec.EncodeInt(buf, *p)
	return buf, nil
}

const partialResult4SumDecimalSize = int64(unsafe.Sizeof(partialResult4SumDecimal{}))

// MemoryUsage implements interface.
func (e *sum4Decimal) MemoryUsage(result PartialResult) int64 {
	return partialResult4SumDecimalSize
}

// LoadFrom implements interface.
func (e *sum4Decimal) LoadFrom(buf []byte) (PartialResult, []byte, error) {
	var err error
	var d *types.MyDecimal
	p := new(partialResult4SumDecimal)
	if buf, p.notNullRowCount, err = codec.DecodeInt(buf); err != nil {
		return nil, nil, err
	}
	if buf, d, _, _, err = codec.DecodeDecimal(buf); err != nil {
		return nil, nil, err
	}
	p.val = *d
	return PartialResult(p), buf, err
}

// DumpTo implements interface.
func (e *sum4Decimal) DumpTo(pr PartialResult, buf []byte) ([]byte, error) {
	var err error
	p := (*partialResult4SumDecimal)(pr)
	buf = codec.EncodeInt(buf, p.notNullRowCount)
	prec, frac := p.val.PrecisionAndFrac()
	if buf, err = codec.EncodeDecimal(buf, &p.val, prec, frac); err != nil {
		return nil, err
	}
	return buf, nil
}

// SupportDisk checks if disk spill is feasible.
func SupportDisk(aggFuncs []AggFunc) error {
	for _, agg := range aggFuncs {
		if _, ok := agg.(PartialResultCoder); !ok {
			return errors.Errorf("%v cannot support to spill", reflect.TypeOf(agg))
		}
	}
	return nil
}

// EncodePartialResult encodes result.
func EncodePartialResult(aggFuncs []AggFunc, prs []PartialResult) (data []byte, err error) {
	for i, agg := range aggFuncs {
		dAgg, ok := agg.(PartialResultCoder)
		if !ok {
			return nil, errors.Errorf("%v doesn't support to spill", dAgg)
		}
		if data, err = dAgg.DumpTo(prs[i], data); err != nil {
			return
		}
	}
	return
}

// DecodePartialResult decodes results.
func DecodePartialResult(aggFuncs []AggFunc, data []byte) (prs []PartialResult, err error) {
	prs = make([]PartialResult, len(aggFuncs))
	for i, agg := range aggFuncs {
		dAgg, ok := agg.(PartialResultCoder)
		if !ok {
			return nil, errors.Errorf("%v doesn't support to spill", dAgg)
		}
		if prs[i], data, err = dAgg.LoadFrom(data); err != nil {
			return
		}
	}
	return
}

// PartialResultsMemory calculates memory usage.
func PartialResultsMemory(aggFuncs []AggFunc, prs []PartialResult) (mem int64) {
	if prs == nil {
		return 0
	}
	for i, agg := range aggFuncs {
		mAgg, ok := agg.(PartialResultMemoryTracker)
		if !ok {
			continue
		}
		mem += mAgg.MemoryUsage(prs[i])
	}
	return
}
