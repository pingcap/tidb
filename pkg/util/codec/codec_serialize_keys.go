// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/size"
)

func encodeHashChunkRowIdx(typeCtx types.Context, row chunk.Row, tp *types.FieldType, idx int) (flag byte, b []byte, err error) {
	if row.IsNull(idx) {
		flag = NilFlag
		return
	}
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		flag = uvarintFlag
		if !mysql.HasUnsignedFlag(tp.GetFlag()) && row.GetInt64(idx) < 0 {
			flag = varintFlag
		}
		b = row.GetRaw(idx)
	case mysql.TypeFloat:
		flag = floatFlag
		f := float64(row.GetFloat32(idx))
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		if f == 0 {
			f = 0
		}
		b = unsafe.Slice((*byte)(unsafe.Pointer(&f)), unsafe.Sizeof(f))
	case mysql.TypeDouble:
		flag = floatFlag
		f := row.GetFloat64(idx)
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		if f == 0 {
			f = 0
		}
		b = unsafe.Slice((*byte)(unsafe.Pointer(&f)), unsafe.Sizeof(f))
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		flag = compactBytesFlag
		b = row.GetBytes(idx)
		b = ConvertByCollation(b, tp)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		flag = uintFlag
		t := row.GetTime(idx)

		var v uint64
		v, err = t.ToPackedUint()
		if err != nil {
			return
		}
		b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), unsafe.Sizeof(v))
	case mysql.TypeDuration:
		flag = durationFlag
		// duration may have negative value, so we cannot use String to encode directly.
		b = row.GetRaw(idx)
	case mysql.TypeNewDecimal:
		flag = decimalFlag
		// If hash is true, we only consider the original value of this decimal and ignore it's precision.
		dec := row.GetMyDecimal(idx)
		b, err = dec.ToHashKey()
		if err != nil {
			return
		}
	case mysql.TypeEnum:
		if mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
			flag = uvarintFlag
			v := row.GetEnum(idx).Value
			b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)
		} else {
			flag = compactBytesFlag
			v := row.GetEnum(idx).Value
			str := ""
			if enum, err := types.ParseEnumValue(tp.GetElems(), v); err == nil {
				// str will be empty string if v out of definition of enum.
				str = enum.Name
			}
			b = ConvertByCollation(hack.Slice(str), tp)
		}
	case mysql.TypeSet:
		flag = compactBytesFlag
		s, err := types.ParseSetValue(tp.GetElems(), row.GetSet(idx).Value)
		if err != nil {
			return 0, nil, err
		}
		b = ConvertByCollation(hack.Slice(s.Name), tp)
	case mysql.TypeBit:
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		flag = uvarintFlag
		v, err1 := types.BinaryLiteral(row.GetBytes(idx)).ToInt(typeCtx)
		terror.Log(errors.Trace(err1))
		b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), unsafe.Sizeof(v))
	case mysql.TypeJSON:
		flag = jsonFlag
		json := row.GetJSON(idx)
		b = json.HashValue(b)
	case mysql.TypeTiDBVectorFloat32:
		flag = vectorFloat32Flag
		v := row.GetVectorFloat32(idx)
		b = v.SerializeTo(nil)
	default:
		return 0, nil, errors.Errorf("unsupport column type for encode %d", tp.GetType())
	}
	return
}

// SerializeMode is for some special cases during serialize key
type SerializeMode int

const (
	// Normal means serialize in the normal way
	Normal SerializeMode = iota
	// NeedSignFlag when serialize integer column, if the join key is <signed, signed> or <unsigned, unsigned>,
	// the unsigned flag can be ignored, if the join key is <unsigned, signed> or <signed, unsigned>
	// the unsigned flag can not be ignored, if the unsigned flag can not be ignored, the key can not be inlined
	NeedSignFlag
	// KeepVarColumnLength when serialize var-length column, whether record the column length or not. If the join key only contains one var-length
	// column, and the key is not inlined, then no need to record the column length, otherwise, always need to record the column length
	KeepVarColumnLength
)

func preAllocForSerializedKeyBuffer(
	buildKeyIndexs []int,
	chk *chunk.Chunk,
	tps []*types.FieldType,
	usedRows []int,
	filterVector []bool,
	nullVector []bool,
	serializeModes []SerializeMode,
	serializedKeys [][]byte,
	serializedKeyLens []int,
	serializedKeysBuffer []byte) ([]byte, error) {
	for i, idx := range buildKeyIndexs {
		column := chk.Column(idx)
		canSkip := func(index int) bool {
			if column.IsNull(index) {
				nullVector[index] = true
			}
			return (filterVector != nil && !filterVector[index]) || (nullVector != nil && nullVector[index])
		}

		switch tps[i].GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			flagByteNum := int(0)
			if serializeModes[i] == NeedSignFlag {
				flagByteNum = int(size.SizeOfByte)
			}

			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				serializedKeyLens[j] += flagByteNum + 8
			}
		case mysql.TypeFloat, mysql.TypeDouble:
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				serializedKeyLens[j] += int(sizeFloat64)
			}
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			collator := collate.GetCollator(tps[i].GetCollate())

			sizeByteNum := int(0)
			if serializeModes[i] == KeepVarColumnLength {
				sizeByteNum = int(sizeUint32)
			}

			for j, physicalRowIndex := range usedRows {
				if canSkip(physicalRowIndex) {
					continue
				}
				strLen := collator.MaxKeyLen(string(hack.String(column.GetBytes(physicalRowIndex))))
				serializedKeyLens[j] += sizeByteNum + strLen
			}
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				serializedKeyLens[j] += int(sizeUint64)
			}
		case mysql.TypeDuration:
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				serializedKeyLens[j] += 8
			}
		case mysql.TypeNewDecimal:
			sizeByteNum := int(0)
			if serializeModes[i] == KeepVarColumnLength {
				sizeByteNum = int(sizeUint32)
			}

			ds := column.Decimals()
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}

				size, err := ds[physicalRowindex].HashKeySize()
				if err != nil {
					return serializedKeysBuffer, err
				}

				serializedKeyLens[j] += size + sizeByteNum
			}
		case mysql.TypeEnum:
			if mysql.HasEnumSetAsIntFlag(tps[i].GetFlag()) {
				elemLen := 0
				if serializeModes[i] == NeedSignFlag {
					elemLen += int(size.SizeOfByte)
				}
				elemLen += int(sizeUint64)

				for j, physicalRowindex := range usedRows {
					if canSkip(physicalRowindex) {
						continue
					}
					serializedKeyLens[j] += elemLen
				}
			} else {
				sizeByteNum := int64(0)
				if serializeModes[i] == KeepVarColumnLength {
					sizeByteNum = int64(sizeUint32)
				}

				collator := collate.GetCollator(tps[i].GetCollate())
				for j, physicalRowindex := range usedRows {
					if canSkip(physicalRowindex) {
						continue
					}

					v := column.GetEnum(physicalRowindex).Value
					str := ""
					if enum, err := types.ParseEnumValue(tps[i].GetElems(), v); err == nil {
						str = enum.Name
					}

					serializedKeyLens[j] += int(sizeByteNum) + collator.MaxKeyLen(str)
				}
			}
		case mysql.TypeSet:
			sizeByteNum := int64(0)
			if serializeModes[i] == KeepVarColumnLength {
				sizeByteNum = int64(sizeUint32)
			}

			collator := collate.GetCollator(tps[i].GetCollate())
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}

				s, err := types.ParseSetValue(tps[i].GetElems(), column.GetSet(physicalRowindex).Value)
				if err != nil {
					return serializedKeysBuffer, err
				}

				serializedKeyLens[j] += int(sizeByteNum) + collator.MaxKeyLen(s.Name)
			}
		case mysql.TypeBit:
			signFlagLen := 0
			if serializeModes[i] == NeedSignFlag {
				signFlagLen = int(size.SizeOfByte)
			}
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}

				serializedKeyLens[j] += signFlagLen + int(sizeUint64)
			}
		case mysql.TypeJSON:
			sizeByteNum := 0
			if serializeModes[i] == KeepVarColumnLength {
				sizeByteNum = int(sizeUint32)
			}

			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}

				serializedKeyLens[j] += sizeByteNum + int(column.GetJSON(physicalRowindex).CalculateHashValueSize())
			}
		case mysql.TypeNull:
		default:
			return serializedKeysBuffer, errors.Errorf("unsupport column type for pre-alloc %d", tps[i].GetType())
		}
	}

	totalMemUsage := 0
	for _, usage := range serializedKeyLens {
		totalMemUsage += usage
	}

	if cap(serializedKeysBuffer) < totalMemUsage {
		serializedKeysBuffer = make([]byte, totalMemUsage)
	} else {
		serializedKeysBuffer = serializedKeysBuffer[:totalMemUsage]
	}

	start := 0
	for i := range serializedKeys {
		rowLen := serializedKeyLens[i]
		serializedKeys[i] = serializedKeysBuffer[start : start : start+rowLen]
		start += rowLen
	}
	return serializedKeysBuffer, nil
}

func serializeKeysImpl(
	typeCtx types.Context,
	chk *chunk.Chunk,
	tps []*types.FieldType,
	buildKeyIndexs []int,
	usedRows []int,
	filterVector []bool,
	nullVector []bool,
	serializeModes []SerializeMode,
	serializedKeys [][]byte) error {
	canSkip := func(index int) bool {
		return (filterVector != nil && !filterVector[index]) || (nullVector != nil && nullVector[index])
	}

	for i, idx := range buildKeyIndexs {
		column := chk.Column(idx)
		serializeMode := serializeModes[i]
		tp := tps[i]
		switch tp.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			i64s := column.Int64s()
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				if serializeMode == NeedSignFlag {
					if !mysql.HasUnsignedFlag(tp.GetFlag()) && i64s[physicalRowindex] < 0 {
						serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], intFlag)
					} else {
						serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], uintFlag)
					}
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], column.GetRaw(physicalRowindex)...)
			}
		case mysql.TypeFloat:
			f32s := column.Float32s()
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				d := float64(f32s[physicalRowindex])
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				if d == 0 {
					d = 0
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&d)), sizeFloat64)...)
			}
		case mysql.TypeDouble:
			f64s := column.Float64s()
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				f := f64s[physicalRowindex]
				if f == 0 {
					f = 0
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&f)), sizeFloat64)...)
			}
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			collator := collate.GetCollator(tp.GetCollate())
			for logicalRowIndex, physicalRowIndex := range usedRows {
				if canSkip(physicalRowIndex) {
					continue
				}
				data := collator.ImmutableKey(string(hack.String(column.GetBytes(physicalRowIndex))))
				size := uint32(len(data))
				if serializeMode == KeepVarColumnLength {
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint32)...)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], data...)
			}
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			ts := column.Times()
			for logicalRowIndex, physicalRowIndex := range usedRows {
				if canSkip(physicalRowIndex) {
					continue
				}
				v, err := ts[physicalRowIndex].ToPackedUint()
				if err != nil {
					return err
				}
				// don't need to check serializeMode since date/datetime/timestamp must be compared with date/datetime/timestamp, so the serializeMode must be normal
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)...)
			}
		case mysql.TypeDuration:
			for logicalRowIndex, physicalRowIndex := range usedRows {
				if canSkip(physicalRowIndex) {
					continue
				}
				// don't need to check serializeMode since duration must be compared with duration, so the serializeMode must be normal
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], column.GetRaw(physicalRowIndex)...)
			}
		case mysql.TypeNewDecimal:
			ds := column.Decimals()
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				b, err := ds[physicalRowindex].ToHashKey()
				if err != nil {
					return err
				}
				if serializeMode == KeepVarColumnLength {
					// for decimal, the size must be less than uint8.MAX, so use uint8 here
					size := uint8(len(b))
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint8)...)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], b...)
			}
		case mysql.TypeEnum:
			if mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
				for logicalRowIndex, physicalRowindex := range usedRows {
					if canSkip(physicalRowindex) {
						continue
					}
					v := column.GetEnum(physicalRowindex).Value
					// check serializeMode here because enum maybe compare to integer type directly
					if serializeMode == NeedSignFlag {
						serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], uintFlag)
					}
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)...)
				}
			} else {
				collator := collate.GetCollator(tp.GetCollate())
				for logicalRowIndex, physicalRowindex := range usedRows {
					if canSkip(physicalRowindex) {
						continue
					}
					v := column.GetEnum(physicalRowindex).Value
					str := ""
					if enum, err := types.ParseEnumValue(tp.GetElems(), v); err == nil {
						str = enum.Name
					}
					b := collator.ImmutableKey(str)
					if serializeMode == KeepVarColumnLength {
						// for enum, the size must be less than uint32.MAX, so use uint32 here
						size := uint32(len(b))
						serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint32)...)
					}
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], b...)
				}
			}
		case mysql.TypeSet:
			collator := collate.GetCollator(tp.GetCollate())
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				s, err := types.ParseSetValue(tp.GetElems(), column.GetSet(physicalRowindex).Value)
				if err != nil {
					return err
				}
				b := collator.ImmutableKey(s.Name)
				if serializeMode == KeepVarColumnLength {
					// for enum, the size must be less than uint32.MAX, so use uint32 here
					size := uint32(len(b))
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint32)...)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], b...)
			}
		case mysql.TypeBit:
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				v, err1 := types.BinaryLiteral(column.GetBytes(physicalRowindex)).ToInt(typeCtx)
				terror.Log(errors.Trace(err1))
				// check serializeMode here because bit maybe compare to integer type directly
				if serializeMode == NeedSignFlag {
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], uintFlag)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)...)
			}
		case mysql.TypeJSON:
			jsonHashBuffer := make([]byte, 0)
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				jsonHashBuffer = jsonHashBuffer[:0]
				jsonHashBuffer = column.GetJSON(physicalRowindex).HashValue(jsonHashBuffer)
				if serializeMode == KeepVarColumnLength {
					size := uint32(len(jsonHashBuffer))
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint32)...)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], jsonHashBuffer...)
			}
		case mysql.TypeNull:
		default:
			return errors.Errorf("unsupport column type for encode %d", tp.GetType())
		}
	}
	return nil
}

// SerializeKeys is used in join
func SerializeKeys(
	typeCtx types.Context,
	chk *chunk.Chunk,
	tps []*types.FieldType,
	buildKeyIndexs []int,
	usedRows []int,
	filterVector []bool,
	nullVector []bool,
	serializeModes []SerializeMode,
	serializedKeys [][]byte,
	serializedKeyLens []int,
	serializedKeysBuffer []byte) ([]byte, error) {
	serializedKeysBuffer, err := preAllocForSerializedKeyBuffer(
		buildKeyIndexs,
		chk,
		tps,
		usedRows,
		filterVector,
		nullVector,
		serializeModes,
		serializedKeys,
		serializedKeyLens,
		serializedKeysBuffer)
	if err != nil {
		return serializedKeysBuffer, err
	}

	var serializedKeyVectorBufferCapsForTest []int
	if intest.InTest {
		serializedKeyVectorBufferCapsForTest = make([]int, len(serializedKeys))
		for i := range serializedKeys {
			serializedKeyVectorBufferCapsForTest[i] = cap(serializedKeys[i])
		}
	}

	err = serializeKeysImpl(
		typeCtx,
		chk,
		tps,
		buildKeyIndexs,
		usedRows,
		filterVector,
		nullVector,
		serializeModes,
		serializedKeys)
	if err != nil {
		return serializedKeysBuffer, err
	}

	if intest.InTest {
		for i := range serializedKeys {
			if serializedKeyVectorBufferCapsForTest[i] < cap(serializedKeys[i]) {
				panic(fmt.Sprintf("Before: %d, After: %d", serializedKeyVectorBufferCapsForTest[i], cap(serializedKeys[i])))
			}
		}
	}

	return serializedKeysBuffer, nil
}
