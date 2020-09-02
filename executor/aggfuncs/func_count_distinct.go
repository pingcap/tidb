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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/stringutil"
)

const (
	// DefPartialResult4CountDistinctIntSize is the size of partialResult4CountDistinctInt
	DefPartialResult4CountDistinctIntSize = int64(unsafe.Sizeof(partialResult4CountDistinctInt{}))
	// DefPartialResult4CountDistinctRealSize is the size of partialResult4CountDistinctReal
	DefPartialResult4CountDistinctRealSize = int64(unsafe.Sizeof(partialResult4CountDistinctReal{}))
	// DefPartialResult4CountDistinctDecimalSize is the size of partialResult4CountDistinctDecimal
	DefPartialResult4CountDistinctDecimalSize = int64(unsafe.Sizeof(partialResult4CountDistinctDecimal{}))
	// DefPartialResult4CountDistinctDurationSize is the size of partialResult4CountDistinctDuration
	DefPartialResult4CountDistinctDurationSize = int64(unsafe.Sizeof(partialResult4CountDistinctDuration{}))
	// DefPartialResult4CountDistinctStringSize is the size of partialResult4CountDistinctString
	DefPartialResult4CountDistinctStringSize = int64(unsafe.Sizeof(partialResult4CountDistinctString{}))
	// DefPartialResult4CountWithDistinctSize is the size of partialResult4CountWithDistinct
	DefPartialResult4CountWithDistinctSize = int64(unsafe.Sizeof(partialResult4CountWithDistinct{}))
	// DefPartialResult4ApproxCountDistinctSize is the size of partialResult4ApproxCountDistinct
	DefPartialResult4ApproxCountDistinctSize = int64(unsafe.Sizeof(partialResult4ApproxCountDistinct{}))
)

type partialResult4CountDistinctInt struct {
	valSet set.Int64Set
}

type countOriginalWithDistinct4Int struct {
	baseCount
}

func (e *countOriginalWithDistinct4Int) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4CountDistinctInt{
		valSet: set.NewInt64Set(),
	}), DefPartialResult4CountDistinctIntSize
}

func (e *countOriginalWithDistinct4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctInt)(pr)
	p.valSet = set.NewInt64Set()
}

func (e *countOriginalWithDistinct4Int) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctInt)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctInt)(pr)

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.valSet.Exist(input) {
			continue
		}
		p.valSet.Insert(input)
		memDelta += DefInt64Size
	}

	return memDelta, nil
}

type partialResult4CountDistinctReal struct {
	valSet set.Float64Set
}

type countOriginalWithDistinct4Real struct {
	baseCount
}

func (e *countOriginalWithDistinct4Real) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4CountDistinctReal{
		valSet: set.NewFloat64Set(),
	}), DefPartialResult4CountDistinctRealSize
}

func (e *countOriginalWithDistinct4Real) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctReal)(pr)
	p.valSet = set.NewFloat64Set()
}

func (e *countOriginalWithDistinct4Real) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctReal)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4Real) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctReal)(pr)

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.valSet.Exist(input) {
			continue
		}
		p.valSet.Insert(input)
		memDelta += DefFloat64Size
	}

	return memDelta, nil
}

type partialResult4CountDistinctDecimal struct {
	valSet set.StringSet
}

type countOriginalWithDistinct4Decimal struct {
	baseCount
}

func (e *countOriginalWithDistinct4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4CountDistinctDecimal{
		valSet: set.NewStringSet(),
	}), DefPartialResult4CountDistinctDecimalSize
}

func (e *countOriginalWithDistinct4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctDecimal)(pr)
	p.valSet = set.NewStringSet()
}

func (e *countOriginalWithDistinct4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctDecimal)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctDecimal)(pr)

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		hash, err := input.ToHashKey()
		if err != nil {
			return memDelta, err
		}
		decStr := string(hack.String(hash))
		if p.valSet.Exist(decStr) {
			continue
		}
		p.valSet.Insert(decStr)
		memDelta += int64(len(decStr))
	}

	return memDelta, nil
}

type partialResult4CountDistinctDuration struct {
	valSet set.Int64Set
}

type countOriginalWithDistinct4Duration struct {
	baseCount
}

func (e *countOriginalWithDistinct4Duration) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4CountDistinctDuration{
		valSet: set.NewInt64Set(),
	}), DefPartialResult4CountDistinctDurationSize
}

func (e *countOriginalWithDistinct4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctDuration)(pr)
	p.valSet = set.NewInt64Set()
}

func (e *countOriginalWithDistinct4Duration) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctDuration)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctDuration)(pr)

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}

		if p.valSet.Exist(int64(input.Duration)) {
			continue
		}
		p.valSet.Insert(int64(input.Duration))
		memDelta += DefInt64Size
	}

	return memDelta, nil
}

type partialResult4CountDistinctString struct {
	valSet set.StringSet
}

type countOriginalWithDistinct4String struct {
	baseCount
}

func (e *countOriginalWithDistinct4String) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4CountDistinctString{
		valSet: set.NewStringSet(),
	}), DefPartialResult4CountDistinctStringSize
}

func (e *countOriginalWithDistinct4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctString)(pr)
	p.valSet = set.NewStringSet()
}

func (e *countOriginalWithDistinct4String) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctString)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctString)(pr)
	collator := collate.GetCollator(e.args[0].GetType().Collate)

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		input = string(collator.Key(input))

		if p.valSet.Exist(input) {
			continue
		}
		input = stringutil.Copy(input)
		p.valSet.Insert(input)
		memDelta += int64(len(input))
	}

	return memDelta, nil
}

type countOriginalWithDistinct struct {
	baseCount
}

type partialResult4CountWithDistinct struct {
	valSet set.StringSet
}

func (e *countOriginalWithDistinct) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4CountWithDistinct{
		valSet: set.NewStringSet(),
	}), DefPartialResult4CountWithDistinctSize
}

func (e *countOriginalWithDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountWithDistinct)(pr)
	p.valSet = set.NewStringSet()
}

func (e *countOriginalWithDistinct) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountWithDistinct)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountWithDistinct)(pr)

	encodedBytes := make([]byte, 0)
	// Decimal struct is the biggest type we will use.
	buf := make([]byte, types.MyDecimalStructSize)

	for _, row := range rowsInGroup {
		var err error
		var hasNull, isNull bool
		encodedBytes = encodedBytes[:0]

		for i := 0; i < len(e.args) && !hasNull; i++ {
			encodedBytes, isNull, err = evalAndEncode(sctx, e.args[i], row, buf, encodedBytes)
			if err != nil {
				return memDelta, err
			}
			if isNull {
				hasNull = true
				break
			}
		}
		encodedString := string(encodedBytes)
		if hasNull || p.valSet.Exist(encodedString) {
			continue
		}
		p.valSet.Insert(encodedString)
		memDelta += int64(len(encodedString))
	}

	return memDelta, nil
}

type countPartialWithDistinct4Int struct {
	countOriginalWithDistinct4Int
}

func (e *countPartialWithDistinct4Int) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4CountDistinctInt)(src), (*partialResult4CountDistinctInt)(dst)
	for k := range p1.valSet {
		if p2.valSet.Exist(k) {
			continue
		}
		p2.valSet.Insert(k)
	}
	return 0, nil
}

type countPartialWithDistinct4Real struct {
	countOriginalWithDistinct4Real
}

func (e *countPartialWithDistinct4Real) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4CountDistinctReal)(src), (*partialResult4CountDistinctReal)(dst)
	for k := range p1.valSet {
		if p2.valSet.Exist(k) {
			continue
		}
		p2.valSet.Insert(k)
	}
	return 0, nil
}

type countPartialWithDistinct4Decimal struct {
	countOriginalWithDistinct4Decimal
}

func (e *countPartialWithDistinct4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4CountDistinctDecimal)(src), (*partialResult4CountDistinctDecimal)(dst)
	for k := range p1.valSet {
		if p2.valSet.Exist(k) {
			continue
		}
		p2.valSet.Insert(k)
	}
	return 0, nil
}

type countPartialWithDistinct4Duration struct {
	countOriginalWithDistinct4Duration
}

func (e *countPartialWithDistinct4Duration) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4CountDistinctDuration)(src), (*partialResult4CountDistinctDuration)(dst)
	for k := range p1.valSet {
		if p2.valSet.Exist(k) {
			continue
		}
		p2.valSet.Insert(k)
	}
	return 0, nil
}

type countPartialWithDistinct4String struct {
	countOriginalWithDistinct4String
}

func (e *countPartialWithDistinct4String) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4CountDistinctString)(src), (*partialResult4CountDistinctString)(dst)
	for k := range p1.valSet {
		if p2.valSet.Exist(k) {
			continue
		}
		p2.valSet.Insert(k)
	}
	return 0, nil
}

type countPartialWithDistinct struct {
	countOriginalWithDistinct
}

func (e *countPartialWithDistinct) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4CountWithDistinct)(src), (*partialResult4CountWithDistinct)(dst)
	for k := range p1.valSet {
		if p2.valSet.Exist(k) {
			continue
		}
		p2.valSet.Insert(k)
	}
	return 0, nil
}

// evalAndEncode eval one row with an expression and encode value to bytes.
func evalAndEncode(
	sctx sessionctx.Context, arg expression.Expression,
	row chunk.Row, buf, encodedBytes []byte,
) (_ []byte, isNull bool, err error) {
	switch tp := arg.GetType().EvalType(); tp {
	case types.ETInt:
		var val int64
		val, isNull, err = arg.EvalInt(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendInt64(encodedBytes, buf, val)
	case types.ETReal:
		var val float64
		val, isNull, err = arg.EvalReal(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendFloat64(encodedBytes, buf, val)
	case types.ETDecimal:
		var val *types.MyDecimal
		val, isNull, err = arg.EvalDecimal(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes, err = appendDecimal(encodedBytes, val)
	case types.ETTimestamp, types.ETDatetime:
		var val types.Time
		val, isNull, err = arg.EvalTime(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendTime(encodedBytes, buf, val)
	case types.ETDuration:
		var val types.Duration
		val, isNull, err = arg.EvalDuration(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendDuration(encodedBytes, buf, val)
	case types.ETJson:
		var val json.BinaryJSON
		val, isNull, err = arg.EvalJSON(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendJSON(encodedBytes, buf, val)
	case types.ETString:
		var val string
		val, isNull, err = arg.EvalString(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = codec.EncodeCompactBytes(encodedBytes, hack.Slice(val))
	default:
		return nil, false, errors.Errorf("unsupported column type for encode %d", tp)
	}
	return encodedBytes, isNull, err
}

func appendInt64(encodedBytes, buf []byte, val int64) []byte {
	*(*int64)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:8]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendFloat64(encodedBytes, buf []byte, val float64) []byte {
	*(*float64)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:8]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendDecimal(encodedBytes []byte, val *types.MyDecimal) ([]byte, error) {
	hash, err := val.ToHashKey()
	encodedBytes = append(encodedBytes, hash...)
	return encodedBytes, err
}

func writeTime(buf []byte, t types.Time) {
	binary.BigEndian.PutUint16(buf, uint16(t.Year()))
	buf[2] = uint8(t.Month())
	buf[3] = uint8(t.Day())
	buf[4] = uint8(t.Hour())
	buf[5] = uint8(t.Minute())
	buf[6] = uint8(t.Second())
	binary.BigEndian.PutUint32(buf[8:], uint32(t.Microsecond()))
	buf[12] = t.Type()
	buf[13] = uint8(t.Fsp())
}

func appendTime(encodedBytes, buf []byte, val types.Time) []byte {
	writeTime(buf, val)
	buf = buf[:16]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendDuration(encodedBytes, buf []byte, val types.Duration) []byte {
	*(*types.Duration)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:16]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendJSON(encodedBytes, _ []byte, val json.BinaryJSON) []byte {
	encodedBytes = append(encodedBytes, val.TypeCode)
	encodedBytes = append(encodedBytes, val.Value...)
	return encodedBytes
}

func intHash64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

type baseApproxCountDistinct struct {
	baseAggFunc
}

const (
	// The maximum degree of buffer size before the values are discarded
	uniquesHashMaxSizeDegree uint8 = 17
	// The maximum number of elements before the values are discarded
	uniquesHashMaxSize = uint32(1) << (uniquesHashMaxSizeDegree - 1)
	// Initial buffer size degree
	uniquesHashSetInitialSizeDegree uint8 = 4
	// The number of least significant bits used for thinning. The remaining high-order bits are used to determine the position in the hash table.
	uniquesHashBitsForSkip = 32 - uniquesHashMaxSizeDegree
)

type approxCountDistinctHashValue uint32

// partialResult4ApproxCountDistinct use `BJKST` algorithm to compute approximate result of count distinct.
// According to an experimental survey http://www.vldb.org/pvldb/vol11/p499-harmouch.pdf, the error guarantee of BJKST
// was even better than the theoretical lower bounds.
// For the calculation state, it uses a sample of element hash values with a size up to uniquesHashMaxSize. Compared
// with the widely known HyperLogLog algorithm, this algorithm is less effective in terms of accuracy and
// memory consumption (even up to proportionality), but it is adaptive. This means that with fairly high accuracy, it
// consumes less memory during simultaneous computation of cardinality for a large number of data sets whose cardinality
// has power law distribution (i.e. in cases when most of the data sets are small).
// This algorithm is also very accurate for data sets with small cardinality and very efficient on CPU. If number of
// distinct element is more than 2^32, relative error may be high.
type partialResult4ApproxCountDistinct struct {
	size       uint32 /// Number of elements.
	sizeDegree uint8  /// The size of the table as a power of 2.
	skipDegree uint8  /// Skip elements not divisible by 2 ^ skipDegree.
	hasZero    bool   /// The hash table contains an element with a hash value of 0.
	buf        []approxCountDistinctHashValue
}

// NewPartialResult4ApproxCountDistinct builds a partial result for agg function ApproxCountDistinct.
func NewPartialResult4ApproxCountDistinct() *partialResult4ApproxCountDistinct {
	p := &partialResult4ApproxCountDistinct{}
	p.reset()
	return p
}

func (p *partialResult4ApproxCountDistinct) InsertHash64(x uint64) {
	// no need to rehash, just cast into uint32
	p.insertHash(approxCountDistinctHashValue(x))
}

func (p *partialResult4ApproxCountDistinct) MemUsage() int64 {
	return int64(len(p.buf)) * DefUint32Size
}

func (p *partialResult4ApproxCountDistinct) alloc(newSizeDegree uint8) {
	p.size = 0
	p.skipDegree = 0
	p.hasZero = false
	p.buf = make([]approxCountDistinctHashValue, uint32(1)<<newSizeDegree)
	p.sizeDegree = newSizeDegree
}

func (p *partialResult4ApproxCountDistinct) reset() {
	p.alloc(uniquesHashSetInitialSizeDegree)
}

func max(a, b uint8) uint8 {
	if a > b {
		return a
	}

	return b
}

func (p *partialResult4ApproxCountDistinct) bufSize() uint32 {
	return uint32(1) << p.sizeDegree
}

func (p *partialResult4ApproxCountDistinct) mask() uint32 {
	return p.bufSize() - 1
}

func (p *partialResult4ApproxCountDistinct) place(x approxCountDistinctHashValue) uint32 {
	return uint32(x>>uniquesHashBitsForSkip) & p.mask()
}

// Increase the size of the buffer 2 times or up to new size degree.
func (p *partialResult4ApproxCountDistinct) resize(newSizeDegree uint8) {
	oldSize := p.bufSize()
	oldBuf := p.buf

	if 0 == newSizeDegree {
		newSizeDegree = p.sizeDegree + 1
	}

	p.buf = make([]approxCountDistinctHashValue, uint32(1)<<newSizeDegree)
	p.sizeDegree = newSizeDegree

	// Move some items to new locations.
	for i := uint32(0); i < oldSize; i++ {
		x := oldBuf[i]
		if x != 0 {
			p.reinsertImpl(x)
		}
	}
}

func (p *partialResult4ApproxCountDistinct) readAndMerge(rb []byte) error {
	rhsSkipDegree := rb[0]
	rb = rb[1:]

	if rhsSkipDegree > p.skipDegree {
		p.skipDegree = rhsSkipDegree
		p.rehash()
	}

	rb, rhsSize, err := codec.DecodeUvarint(rb)

	if err != nil {
		return err
	}

	if rhsSize > uint64(uniquesHashMaxSize) {
		return errors.New("Cannot read partialResult4ApproxCountDistinct: too large size degree")
	}

	if p.bufSize() < uint32(rhsSize) {
		newSizeDegree := max(uniquesHashSetInitialSizeDegree, uint8(math.Log2(float64(rhsSize-1)))+2)
		p.resize(newSizeDegree)
	}

	for i := uint32(0); i < uint32(rhsSize); i++ {
		x := *(*approxCountDistinctHashValue)(unsafe.Pointer(&rb[0]))
		rb = rb[4:]
		p.insertHash(x)
	}

	return err
}

// Correct system errors due to collisions during hashing in uint32.
func (p *partialResult4ApproxCountDistinct) fixedSize() uint64 {
	if 0 == p.skipDegree {
		return uint64(p.size)
	}

	res := uint64(p.size) * (uint64(1) << p.skipDegree)

	// Pseudo-random remainder.
	res += intHash64(uint64(p.size)) & ((uint64(1) << p.skipDegree) - 1)

	// When different elements randomly scattered across 2^32 buckets, filled buckets with average of `res` obtained.
	p32 := uint64(1) << 32
	fixedRes := math.Round(float64(p32) * (math.Log(float64(p32)) - math.Log(float64(p32-res))))
	return uint64(fixedRes)
}

func (p *partialResult4ApproxCountDistinct) insertHash(hashValue approxCountDistinctHashValue) {
	if !p.good(hashValue) {
		return
	}

	p.insertImpl(hashValue)
	p.shrinkIfNeed()
}

// The value is divided by 2 ^ skip_degree
func (p *partialResult4ApproxCountDistinct) good(hash approxCountDistinctHashValue) bool {
	return hash == ((hash >> p.skipDegree) << p.skipDegree)
}

// Insert a value
func (p *partialResult4ApproxCountDistinct) insertImpl(x approxCountDistinctHashValue) {
	if x == 0 {
		if !p.hasZero {
			p.size += 1
		}
		p.hasZero = true
		return
	}

	placeValue := p.place(x)
	for p.buf[placeValue] != 0 && p.buf[placeValue] != x {
		placeValue++
		placeValue &= p.mask()
	}

	if p.buf[placeValue] == x {
		return
	}

	p.buf[placeValue] = x
	p.size++
}

// If the hash table is full enough, then do resize.
// If there are too many items, then throw half the pieces until they are small enough.
func (p *partialResult4ApproxCountDistinct) shrinkIfNeed() {
	if p.size > p.maxFill() {
		if p.size > uniquesHashMaxSize {
			for p.size > uniquesHashMaxSize {
				p.skipDegree++
				p.rehash()
			}
		} else {
			p.resize(0)
		}
	}
}

func (p *partialResult4ApproxCountDistinct) maxFill() uint32 {
	return uint32(1) << (p.sizeDegree - 1)
}

// Delete all values whose hashes do not divide by 2 ^ skip_degree
func (p *partialResult4ApproxCountDistinct) rehash() {
	for i := uint32(0); i < p.bufSize(); i++ {
		if p.buf[i] != 0 && !p.good(p.buf[i]) {
			p.buf[i] = 0
			p.size--
		}
	}

	for i := uint32(0); i < p.bufSize(); i++ {
		if p.buf[i] != 0 && i != p.place(p.buf[i]) {
			x := p.buf[i]
			p.buf[i] = 0
			p.reinsertImpl(x)
		}
	}
}

// Insert a value into the new buffer that was in the old buffer.
// Used when increasing the size of the buffer, as well as when reading from a file.
func (p *partialResult4ApproxCountDistinct) reinsertImpl(x approxCountDistinctHashValue) {
	placeValue := p.place(x)
	for p.buf[placeValue] != 0 {
		placeValue++
		placeValue &= p.mask()
	}

	p.buf[placeValue] = x
}

func (p *partialResult4ApproxCountDistinct) merge(tar *partialResult4ApproxCountDistinct) {
	if tar.skipDegree > p.skipDegree {
		p.skipDegree = tar.skipDegree
		p.rehash()
	}

	if !p.hasZero && tar.hasZero {
		p.hasZero = true
		p.size++
		p.shrinkIfNeed()
	}

	for i := uint32(0); i < tar.bufSize(); i++ {
		if tar.buf[i] != 0 && p.good(tar.buf[i]) {
			p.insertImpl(tar.buf[i])
			p.shrinkIfNeed()
		}
	}
}

func (p *partialResult4ApproxCountDistinct) Serialize() []byte {
	var buf [4]byte
	res := make([]byte, 0, 1+binary.MaxVarintLen64+p.size*4)

	res = append(res, p.skipDegree)
	res = codec.EncodeUvarint(res, uint64(p.size))

	if p.hasZero {
		binary.LittleEndian.PutUint32(buf[:], 0)
		res = append(res, buf[:]...)
	}

	for i := uint32(0); i < p.bufSize(); i++ {
		if p.buf[i] != 0 {
			binary.LittleEndian.PutUint32(buf[:], uint32(p.buf[i]))
			res = append(res, buf[:]...)
		}
	}
	return res
}

func (e *baseApproxCountDistinct) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4ApproxCountDistinct)(pr)
	chk.AppendInt64(e.ordinal, int64(p.fixedSize()))
	return nil
}

func (e *baseApproxCountDistinct) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return (PartialResult)(NewPartialResult4ApproxCountDistinct()), DefPartialResult4ApproxCountDistinctSize
}

func (e *baseApproxCountDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4ApproxCountDistinct)(pr)
	p.reset()
}

func (e *baseApproxCountDistinct) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4ApproxCountDistinct)(src), (*partialResult4ApproxCountDistinct)(dst)
	p2.merge(p1)
	return 0, nil
}

type approxCountDistinctOriginal struct {
	baseApproxCountDistinct
}

func (e *approxCountDistinctOriginal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4ApproxCountDistinct)(pr)
	encodedBytes := make([]byte, 0)
	// Decimal struct is the biggest type we will use.
	buf := make([]byte, types.MyDecimalStructSize)

	for _, row := range rowsInGroup {
		var err error
		var hasNull, isNull bool
		encodedBytes = encodedBytes[:0]

		for i := 0; i < len(e.args) && !hasNull; i++ {
			encodedBytes, isNull, err = evalAndEncode(sctx, e.args[i], row, buf, encodedBytes)
			if err != nil {
				return memDelta, err
			}
			if isNull {
				hasNull = true
				break
			}
		}
		if hasNull {
			continue
		}
		oldMemUsage := p.MemUsage()
		x := farm.Hash64(encodedBytes)
		p.InsertHash64(x)
		newMemUsage := p.MemUsage()
		memDelta += newMemUsage - oldMemUsage
	}

	return memDelta, nil
}

type approxCountDistinctPartial1 struct {
	approxCountDistinctOriginal
}

func (e *approxCountDistinctPartial1) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4ApproxCountDistinct)(pr)
	chk.AppendBytes(e.ordinal, p.Serialize())
	return nil
}

type approxCountDistinctPartial2 struct {
	approxCountDistinctPartial1
}

func (e *approxCountDistinctPartial2) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4ApproxCountDistinct)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return memDelta, err
		}

		if isNull {
			continue
		}

		oldMemUsage := p.MemUsage()
		err = p.readAndMerge(hack.Slice(input))
		if err != nil {
			return memDelta, err
		}
		newMemUsage := p.MemUsage()
		memDelta += newMemUsage - oldMemUsage
	}
	return memDelta, nil
}

type approxCountDistinctFinal struct {
	approxCountDistinctPartial2
}

func (e *approxCountDistinctFinal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	return e.baseApproxCountDistinct.AppendFinalResult2Chunk(sctx, pr, chk)
}
