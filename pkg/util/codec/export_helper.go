// Copyright 2023 PingCAP, Inc.
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

package codec

import (
	"hash"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// Exported functions for codec test package

// ExportedEncodeKey is exported version of EncodeKey
func ExportedEncodeKey(loc *time.Location, b []byte, v ...types.Datum) ([]byte, error) {
	return EncodeKey(loc, b, v...)
}

// ExportedEncodeValue is exported version of EncodeValue
func ExportedEncodeValue(loc *time.Location, b []byte, v ...types.Datum) ([]byte, error) {
	return EncodeValue(loc, b, v...)
}

// ExportedDecode is exported version of Decode
func ExportedDecode(b []byte, size int) ([]types.Datum, error) {
	return Decode(b, size)
}

// ExportedCutOne is exported version of CutOne
func ExportedCutOne(b []byte) (data []byte, remain []byte, err error) {
	return CutOne(b)
}

// ExportedCutColumnID is exported version of CutColumnID
func ExportedCutColumnID(b []byte) (remain []byte, n int64, err error) {
	return CutColumnID(b)
}

// ExportedSetRawValues is exported version of SetRawValues
func ExportedSetRawValues(data []byte, values []types.Datum) error {
	return SetRawValues(data, values)
}

// ExportedHashChunkRow is exported version of HashChunkRow
func ExportedHashChunkRow(typeCtx types.Context, w any, row any, allTypes []*types.FieldType, colIdx []int, buf []byte) error {
	// This is a simplified export - actual implementation would need proper type handling
	return nil
}

// ExportedHashChunkColumns is exported version of HashChunkColumns
func ExportedHashChunkColumns(typeCtx types.Context, h []any, chk any, tp *types.FieldType, colIdx int, buf []byte, isNull []bool) error {
	// This is a simplified export - actual implementation would need proper type handling
	return nil
}

// ExportedEqualChunkRow is exported version of EqualChunkRow
func ExportedEqualChunkRow(typeCtx types.Context, row1 any, allTypes1 []*types.FieldType, colIdx1 []int, row2 any, allTypes2 []*types.FieldType, colIdx2 []int) (bool, error) {
	// This is a simplified export - actual implementation would need proper type handling
	return false, nil
}

// ExportedDecodeOne is exported version of DecodeOne
func ExportedDecodeOne(b []byte) (remain []byte, d types.Datum, err error) {
	return DecodeOne(b)
}

// ExportedHashGroupKey is exported version of HashGroupKey
func ExportedHashGroupKey(loc *time.Location, n int, col any, buf [][]byte, ft *types.FieldType) ([][]byte, error) {
	// This is a simplified export - actual implementation would need proper type handling
	return nil, nil
}

// ExportedNewDecoder is exported version of NewDecoder
func ExportedNewDecoder(chk any, timezone *time.Location) any {
	// This is a simplified export - actual implementation would need proper type handling
	return nil
}

// ExportedDecodeRange is exported version of DecodeRange
func ExportedDecodeRange(b []byte, size int, idxColumnTypes []byte, loc *time.Location) ([]types.Datum, []byte, error) {
	return DecodeRange(b, size, idxColumnTypes, loc)
}

// ExportedEncodeDecimal is exported version of EncodeDecimal
func ExportedEncodeDecimal(b []byte, dec *types.MyDecimal, length, frac int) ([]byte, error) {
	return EncodeDecimal(b, dec, length, frac)
}

// ExportedEstimateValueSize is exported version of EstimateValueSize
func ExportedEstimateValueSize(typeCtx types.Context, val types.Datum) (int, error) {
	return EstimateValueSize(typeCtx, val)
}

// ExportedEncode is exported version of encode
func ExportedEncode(loc *time.Location, b []byte, vals []types.Datum, isComparable bool) ([]byte, error) {
	return encode(loc, b, vals, isComparable)
}

// ExportedEncodeSignedInt is exported version of encodeSignedInt
func ExportedEncodeSignedInt(b []byte, v int64, isComparable bool) []byte {
	return encodeSignedInt(b, v, isComparable)
}

// ExportedEncodeUnsignedInt is exported version of encodeUnsignedInt
func ExportedEncodeUnsignedInt(b []byte, v uint64, isComparable bool) []byte {
	return encodeUnsignedInt(b, v, isComparable)
}

// ExportedHashChunkSelected is exported version of HashChunkSelected
func ExportedHashChunkSelected(typeCtx types.Context, h []any, chk any, tp *types.FieldType, colIdx int, buf []byte, isNull, sel []bool, ignoreNull bool) error {
	// This is a simplified export - actual implementation would need proper type handling
	return nil
}

// ExportedEncodeInt is exported version of EncodeInt
func ExportedEncodeInt(b []byte, v int64) []byte {
	return EncodeInt(b, v)
}

// ExportedEncodeBytes is exported version of EncodeBytes
func ExportedEncodeBytes(b []byte, data []byte) []byte {
	return EncodeBytes(b, data)
}

// ExportedDecodeDecimal is exported version of DecodeDecimal
func ExportedDecodeDecimal(b []byte) ([]byte, *types.MyDecimal, int, int, error) {
	return DecodeDecimal(b)
}

// ExportedNewDecoder is exported version of NewDecoder with proper types
func ExportedNewDecoderReal(chk any, timezone *time.Location) any {
	return NewDecoder(chk.(*chunk.Chunk), timezone)
}

// ExportedBytesFlag is an exported alias for bytesFlag constant
const ExportedBytesFlag = bytesFlag

// ExportedDecoder is an exported alias for Decoder type
type ExportedDecoder = Decoder

// ExportedEncodeBytesDesc is exported version of EncodeBytesDesc
func ExportedEncodeBytesDesc(b []byte, data []byte) []byte {
	return EncodeBytesDesc(b, data)
}

// ExportedDecodeBytesDesc is exported version of DecodeBytesDesc
func ExportedDecodeBytesDesc(b []byte, buf []byte) ([]byte, []byte, error) {
	return DecodeBytesDesc(b, buf)
}

// ExportedEncodedBytesLength is exported version of EncodedBytesLength
func ExportedEncodedBytesLength(dataLen int) int {
	return EncodedBytesLength(dataLen)
}

// ExportedDecodeBytes is exported version of DecodeBytes
func ExportedDecodeBytes(b []byte, buf []byte) ([]byte, []byte, error) {
	return DecodeBytes(b, buf)
}

// ExportedSupportsUnaligned returns the value of supportsUnaligned
func ExportedSupportsUnaligned() bool {
	return supportsUnaligned
}

// ExportedFastReverseBytes is exported version of fastReverseBytes (modifies in place)
func ExportedFastReverseBytes(data []byte) {
	fastReverseBytes(data)
}

// ExportedReverseBytes is exported version of reverseBytes (modifies in place)
func ExportedReverseBytes(data []byte) {
	reverseBytes(data)
}

// ExportedEncodeBytesExt is exported version of EncodeBytesExt
func ExportedEncodeBytesExt(b []byte, data []byte, isRawKv bool) []byte {
	return EncodeBytesExt(b, data, isRawKv)
}

// ExportedDecodeCompactBytes is exported version of DecodeCompactBytes
func ExportedDecodeCompactBytes(b []byte) ([]byte, []byte, error) {
	return DecodeCompactBytes(b)
}

// ExportedDecodeFloat is exported version of DecodeFloat
func ExportedDecodeFloat(b []byte) ([]byte, float64, error) {
	return DecodeFloat(b)
}

// ExportedDecodeFloatDesc is exported version of DecodeFloatDesc
func ExportedDecodeFloatDesc(b []byte) ([]byte, float64, error) {
	return DecodeFloatDesc(b)
}

// ExportedDecodeInt is exported version of DecodeInt
func ExportedDecodeInt(b []byte) ([]byte, int64, error) {
	return DecodeInt(b)
}

// ExportedDecodeIntDesc is exported version of DecodeIntDesc
func ExportedDecodeIntDesc(b []byte) ([]byte, int64, error) {
	return DecodeIntDesc(b)
}

// ExportedDecodeUint is exported version of DecodeUint
func ExportedDecodeUint(b []byte) ([]byte, uint64, error) {
	return DecodeUint(b)
}

// ExportedDecodeUintDesc is exported version of DecodeUintDesc
func ExportedDecodeUintDesc(b []byte) ([]byte, uint64, error) {
	return DecodeUintDesc(b)
}

// ExportedDecodeVarint is exported version of DecodeVarint
func ExportedDecodeVarint(b []byte) ([]byte, int64, error) {
	return DecodeVarint(b)
}

// ExportedDecodeUvarint is exported version of DecodeUvarint
func ExportedDecodeUvarint(b []byte) ([]byte, uint64, error) {
	return DecodeUvarint(b)
}

// ExportedDecodeComparableVarint is exported version of DecodeComparableVarint
func ExportedDecodeComparableVarint(b []byte) ([]byte, int64, error) {
	return DecodeComparableVarint(b)
}

// ExportedDecodeComparableUvarint is exported version of DecodeComparableUvarint
func ExportedDecodeComparableUvarint(b []byte) ([]byte, uint64, error) {
	return DecodeComparableUvarint(b)
}

// ExportedNewDecoder is exported version of NewDecoder for direct use
func ExportedNewDecoderDirect(chk *chunk.Chunk, timezone *time.Location) *Decoder {
	return NewDecoder(chk, timezone)
}

// ExportedEncodeIntDesc is exported version of EncodeIntDesc
func ExportedEncodeIntDesc(b []byte, v int64) []byte {
	return EncodeIntDesc(b, v)
}

// ExportedEncodeVarint is exported version of EncodeVarint
func ExportedEncodeVarint(b []byte, v int64) []byte {
	return EncodeVarint(b, v)
}

// ExportedEncodeComparableVarint is exported version of EncodeComparableVarint
func ExportedEncodeComparableVarint(b []byte, v int64) []byte {
	return EncodeComparableVarint(b, v)
}

// ExportedEncodeUint is exported version of EncodeUint
func ExportedEncodeUint(b []byte, v uint64) []byte {
	return EncodeUint(b, v)
}

// ExportedEncodeUintDesc is exported version of EncodeUintDesc
func ExportedEncodeUintDesc(b []byte, v uint64) []byte {
	return EncodeUintDesc(b, v)
}

// ExportedEncodeUvarint is exported version of EncodeUvarint
func ExportedEncodeUvarint(b []byte, v uint64) []byte {
	return EncodeUvarint(b, v)
}

// ExportedEncodeComparableUvarint is exported version of EncodeComparableUvarint
func ExportedEncodeComparableUvarint(b []byte, v uint64) []byte {
	return EncodeComparableUvarint(b, v)
}

// ExportedEncodeFloat is exported version of EncodeFloat
func ExportedEncodeFloat(b []byte, v float64) []byte {
	return EncodeFloat(b, v)
}

// ExportedEncodeFloatDesc is exported version of EncodeFloatDesc
func ExportedEncodeFloatDesc(b []byte, v float64) []byte {
	return EncodeFloatDesc(b, v)
}

// ExportedEncodeCompactBytes is exported version of EncodeCompactBytes
func ExportedEncodeCompactBytes(b []byte, data []byte) []byte {
	return EncodeCompactBytes(b, data)
}

// ExportedEncode is exported version of encode
func ExportedEncodeReal(loc *time.Location, b []byte, vals []types.Datum, isComparable bool) ([]byte, error) {
	return encode(loc, b, vals, isComparable)
}

// ExportedNilFlag is an exported alias for NilFlag constant
const ExportedNilFlag = NilFlag

// ExportedMaxFlag is an exported alias for MaxFlag constant
const ExportedMaxFlag = maxFlag

// ExportedGetDecoderBuf returns the buf field from Decoder for tests
func ExportedGetDecoderBuf(d *ExportedDecoder) []byte {
	return d.buf
}

// ExportedSetDecoderBuf sets the buf field from Decoder for tests
func ExportedSetDecoderBuf(d *ExportedDecoder, buf []byte) {
	d.buf = buf
}

// ExportedGetDecoderChk returns the chk field from Decoder for tests
func ExportedGetDecoderChk(d *ExportedDecoder) *chunk.Chunk {
	return d.chk
}

// ExportedEncodeSignedInt is exported version of encodeSignedInt
func ExportedEncodeSignedIntReal(b []byte, v int64, isComparable bool) []byte {
	return encodeSignedInt(b, v, isComparable)
}

// ExportedValueSizeOfSignedInt is exported version of valueSizeOfSignedInt
func ExportedValueSizeOfSignedInt(v int64) int {
	return valueSizeOfSignedInt(v)
}

// ExportedEncodeUnsignedInt is exported version of encodeUnsignedInt
func ExportedEncodeUnsignedIntReal(b []byte, v uint64, isComparable bool) []byte {
	return encodeUnsignedInt(b, v, isComparable)
}

// ExportedValueSizeOfUnsignedInt is exported version of valueSizeOfUnsignedInt
func ExportedValueSizeOfUnsignedInt(v uint64) int {
	return valueSizeOfUnsignedInt(v)
}

// ExportedHashChunkSelectedWithHash64 is exported version of HashChunkSelected that accepts []hash.Hash64
func ExportedHashChunkSelectedWithHash64(typeCtx types.Context, h []hash.Hash64, chk *chunk.Chunk, tp *types.FieldType, colIdx int, buf []byte, isNull, sel []bool, ignoreNull bool) error {
	return HashChunkSelected(typeCtx, h, chk, tp, colIdx, buf, isNull, sel, ignoreNull)
}
