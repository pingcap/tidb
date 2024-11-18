// Copyright 2024 PingCAP, Inc.
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

package join

import (
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/serialization"
)

// joinTableMeta is the join table meta used in hash join v2
type joinTableMeta struct {
	// if the row has fixed length
	isFixedLength bool
	// the row length if the row is fixed length
	rowLength int
	// if the join keys has fixed length
	isJoinKeysFixedLength bool
	// the join keys length if it is fixed length
	joinKeysLength int
	// is the join key inlined in the row data, the join key can be inlined if and only if
	// 1. keyProb.canBeInlined returns true for all the keys
	// 2. there is no duplicate join keys
	isJoinKeysInlined bool
	// the length of null map, the null map include null bit for each column in the row and the used flag for right semi/outer join
	nullMapLength int
	// the column order in row layout, as described above, the save column order maybe different from the column order in build schema
	// for example, the build schema maybe [col1, col2, col3], and the column order in row maybe [col2, col1, col3], then this array
	// is [1, 0, 2]
	rowColumnsOrder []int
	// the column size of each column, -1 mean variable column, the order is the same as rowColumnsOrder
	columnsSize []int
	// the serialize mode for each key
	serializeModes []codec.SerializeMode
	// the first n columns in row is used for other condition, if a join has other condition, we only need to extract
	// first n columns from the RowTable to evaluate other condition
	columnCountNeededForOtherCondition int
	// total column numbers for build side chunk, this is used to construct the chunk if there is join other condition
	totalColumnNumber int
	// column index offset in null map, will be 1 when if there is usedFlag and 0 if there is no usedFlag
	colOffsetInNullMap int
	// keyMode is the key mode, it can be OneInt/FixedSerializedKey/VariableSerializedKey
	keyMode keyMode
	// offset to rowData, -1 for variable length, non-inlined key
	rowDataOffset int
	// fakeKeyByte is used as the fake key when current join need keep invalid key rows
	fakeKeyByte []byte
}

func (meta *joinTableMeta) getSerializedKeyLength(rowStart unsafe.Pointer) uint32 {
	return *(*uint32)(unsafe.Add(rowStart, sizeOfNextPtr+meta.nullMapLength))
}

func (meta *joinTableMeta) isReadNullMapThreadSafe(columnIndex int) bool {
	// Other goroutine will use `atomic.StoreUint32` to write to the first 32 bit in nullmap when it need to set usedFlag
	// so read from nullMap may meet concurrent write if meta.colOffsetInNullMap == 1 && (columnIndex + meta.colOffsetInNullMap < 32)
	mayConcurrentWrite := meta.colOffsetInNullMap == 1 && columnIndex < 31
	return !mayConcurrentWrite
}

// used in tests
func (meta *joinTableMeta) getKeyBytes(rowStart unsafe.Pointer) []byte {
	switch meta.keyMode {
	case OneInt64:
		return hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr), int(serialization.Uint64Len))
	case FixedSerializedKey:
		return hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr), meta.joinKeysLength)
	case VariableSerializedKey:
		return hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr+sizeOfElementSize), int(meta.getSerializedKeyLength(rowStart)))
	default:
		panic("unknown key match type")
	}
}

func (meta *joinTableMeta) advanceToRowData(matchedRowInfo *matchedRowInfo) {
	if meta.rowDataOffset == -1 {
		// variable length, non-inlined key
		matchedRowInfo.buildRowOffset = sizeOfNextPtr + meta.nullMapLength + sizeOfElementSize + int(meta.getSerializedKeyLength(*(*unsafe.Pointer)(unsafe.Pointer(&matchedRowInfo.buildRowStart))))
	} else {
		matchedRowInfo.buildRowOffset = meta.rowDataOffset
	}
}

func (meta *joinTableMeta) isColumnNull(rowStart unsafe.Pointer, columnIndex int) bool {
	byteIndex := (columnIndex + meta.colOffsetInNullMap) / 8
	bitIndex := (columnIndex + meta.colOffsetInNullMap) % 8
	return *(*uint8)(unsafe.Add(rowStart, sizeOfNextPtr+byteIndex))&(uint8(1)<<(7-bitIndex)) != uint8(0)
}

// for join that need to set UsedFlag during probe stage, read from nullMap is not thread safe for the first 32 bit of nullMap, atomic.LoadUint32 is used to avoid read-write conflict
func (*joinTableMeta) isColumnNullThreadSafe(rowStart unsafe.Pointer, columnIndex int) bool {
	return atomic.LoadUint32((*uint32)(unsafe.Add(rowStart, sizeOfNextPtr)))&bitMaskInUint32[columnIndex+1] != uint32(0)
}

func (*joinTableMeta) setUsedFlag(rowStart unsafe.Pointer) {
	addr := (*uint32)(unsafe.Add(rowStart, sizeOfNextPtr))
	value := atomic.LoadUint32(addr)
	value |= usedFlagMask
	atomic.StoreUint32(addr, value)
}

func (*joinTableMeta) isCurrentRowUsed(rowStart unsafe.Pointer) bool {
	return (*(*uint32)(unsafe.Add(rowStart, sizeOfNextPtr)) & usedFlagMask) == usedFlagMask
}

type keyProp struct {
	canBeInlined  bool
	keyLength     int
	isKeyInteger  bool
	isKeyUnsigned bool
}

func getKeyProp(tp *types.FieldType) *keyProp {
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear,
		mysql.TypeDuration:
		isKeyUnsigned := mysql.HasUnsignedFlag(tp.GetFlag())
		if tp.GetType() == mysql.TypeYear {
			// year type is always unsigned
			isKeyUnsigned = true
		} else if tp.GetType() == mysql.TypeDuration {
			// duration type is always signed
			isKeyUnsigned = false
		}
		return &keyProp{canBeInlined: true, keyLength: chunk.GetFixedLen(tp), isKeyInteger: true, isKeyUnsigned: isKeyUnsigned}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		collator := collate.GetCollator(tp.GetCollate())
		return &keyProp{canBeInlined: collate.CanUseRawMemAsKey(collator), keyLength: chunk.VarElemLen, isKeyInteger: false, isKeyUnsigned: false}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		// date related type will use uint64 as serialized key
		return &keyProp{canBeInlined: false, keyLength: int(serialization.Uint64Len), isKeyInteger: true, isKeyUnsigned: true}
	case mysql.TypeFloat:
		// float will use float64 as serialized key
		return &keyProp{canBeInlined: false, keyLength: int(serialization.Float64Len), isKeyInteger: false, isKeyUnsigned: false}
	case mysql.TypeNewDecimal:
		// Although decimal is fixed length, but its key is not fixed length
		return &keyProp{canBeInlined: false, keyLength: chunk.VarElemLen, isKeyInteger: false, isKeyUnsigned: false}
	case mysql.TypeEnum:
		if mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
			// enum int type is always unsigned
			return &keyProp{canBeInlined: false, keyLength: int(serialization.Uint64Len), isKeyInteger: true, isKeyUnsigned: true}
		}
		return &keyProp{canBeInlined: false, keyLength: chunk.VarElemLen, isKeyInteger: false, isKeyUnsigned: false}
	case mysql.TypeBit:
		// bit type is always unsigned
		return &keyProp{canBeInlined: false, keyLength: int(serialization.Uint64Len), isKeyInteger: true, isKeyUnsigned: true}
	default:
		keyLength := chunk.GetFixedLen(tp)
		return &keyProp{canBeInlined: false, keyLength: keyLength, isKeyInteger: false, isKeyUnsigned: false}
	}
}

// buildKeyIndex is the build key column index based on buildSchema, should not be nil
// otherConditionColIndex is the column index that will be used in other condition, if no other condition, will be nil
// columnsNeedConvertToRow is the column index that need to be converted to row, should not be nil
// needUsedFlag is true for outer/semi join that use outer to build
func newTableMeta(buildKeyIndex []int, buildTypes, buildKeyTypes, probeKeyTypes []*types.FieldType, columnsUsedByOtherCondition []int, outputColumns []int, needUsedFlag bool) *joinTableMeta {
	meta := &joinTableMeta{}
	meta.isFixedLength = true
	meta.rowLength = 0
	meta.totalColumnNumber = len(buildTypes)

	columnsNeedToBeSaved := make(map[int]struct{}, len(buildTypes))
	updateMeta := func(index int) {
		if _, ok := columnsNeedToBeSaved[index]; !ok {
			columnsNeedToBeSaved[index] = struct{}{}
			length := chunk.GetFixedLen(buildTypes[index])
			if length == chunk.VarElemLen {
				meta.isFixedLength = false
			} else {
				meta.rowLength += length
			}
		}
	}
	if outputColumns == nil {
		// outputColumns = nil means all the column is needed
		for index := range buildTypes {
			updateMeta(index)
		}
	} else {
		for _, index := range outputColumns {
			updateMeta(index)
		}
		for _, index := range columnsUsedByOtherCondition {
			updateMeta(index)
		}
	}

	meta.isJoinKeysFixedLength = true
	meta.joinKeysLength = 0
	meta.isJoinKeysInlined = true
	keyIndexMap := make(map[int]struct{})
	meta.serializeModes = make([]codec.SerializeMode, 0, len(buildKeyIndex))
	isAllKeyInteger := true
	varLengthKeyNumber := 0
	for index, keyIndex := range buildKeyIndex {
		keyType := buildKeyTypes[index]
		prop := getKeyProp(keyType)
		if prop.keyLength != chunk.VarElemLen {
			meta.joinKeysLength += prop.keyLength
		} else {
			meta.isJoinKeysFixedLength = false
			varLengthKeyNumber++
		}
		if !prop.canBeInlined {
			meta.isJoinKeysInlined = false
		}
		if prop.isKeyInteger {
			buildUnsigned := prop.isKeyUnsigned
			probeKeyProp := getKeyProp(probeKeyTypes[index])
			if !probeKeyProp.isKeyInteger {
				panic("build key is integer but probe key is not integer, should not happens")
			}
			probeUnsigned := probeKeyProp.isKeyUnsigned
			if (buildUnsigned && !probeUnsigned) || (probeUnsigned && !buildUnsigned) {
				meta.serializeModes = append(meta.serializeModes, codec.NeedSignFlag)
				meta.isJoinKeysInlined = false
				if meta.isJoinKeysFixedLength {
					// an extra sign flag is needed in this case
					meta.joinKeysLength++
				}
			} else {
				meta.serializeModes = append(meta.serializeModes, codec.Normal)
			}
		} else {
			if !prop.isKeyInteger {
				isAllKeyInteger = false
			}
			if prop.keyLength == chunk.VarElemLen {
				// keep var column by default for var length column
				meta.serializeModes = append(meta.serializeModes, codec.KeepVarColumnLength)
			} else {
				meta.serializeModes = append(meta.serializeModes, codec.Normal)
			}
		}
		keyIndexMap[keyIndex] = struct{}{}
	}
	if !meta.isJoinKeysFixedLength {
		meta.joinKeysLength = -1
	}
	if len(buildKeyIndex) != len(keyIndexMap) {
		// has duplicated key, can not be inlined
		meta.isJoinKeysInlined = false
	}
	if !meta.isJoinKeysInlined {
		if varLengthKeyNumber == 1 {
			// if key is not inlined and there is only one var-length key, then don't need to record the var length
			for i := 0; i < len(buildKeyIndex); i++ {
				if meta.serializeModes[i] == codec.KeepVarColumnLength {
					meta.serializeModes[i] = codec.Normal
				}
			}
		}
	} else {
		for _, index := range buildKeyIndex {
			updateMeta(index)
		}
	}
	if !meta.isFixedLength {
		meta.rowLength = 0
	}
	// construct the column order
	meta.rowColumnsOrder = make([]int, 0, len(columnsNeedToBeSaved))
	meta.columnsSize = make([]int, 0, len(columnsNeedToBeSaved))
	usedColumnMap := make(map[int]struct{}, len(columnsNeedToBeSaved))

	updateColumnOrder := func(index int) {
		if _, ok := usedColumnMap[index]; !ok {
			meta.rowColumnsOrder = append(meta.rowColumnsOrder, index)
			meta.columnsSize = append(meta.columnsSize, chunk.GetFixedLen(buildTypes[index]))
			usedColumnMap[index] = struct{}{}
		}
	}
	if meta.isJoinKeysInlined {
		// if join key is inlined, the join key will be the first columns
		for _, index := range buildKeyIndex {
			updateColumnOrder(index)
		}
	}
	meta.columnCountNeededForOtherCondition = 0
	if len(columnsUsedByOtherCondition) > 0 {
		// if join has other condition, the columns used by other condition is appended to row layout after the key
		for _, index := range columnsUsedByOtherCondition {
			updateColumnOrder(index)
		}
		meta.columnCountNeededForOtherCondition = len(usedColumnMap)
	}
	if outputColumns == nil {
		// outputColumns = nil means all the column is needed
		for index := range buildTypes {
			updateColumnOrder(index)
		}
	} else {
		for _, index := range outputColumns {
			updateColumnOrder(index)
		}
	}
	if isAllKeyInteger && len(buildKeyIndex) == 1 && meta.serializeModes[0] != codec.NeedSignFlag {
		meta.keyMode = OneInt64
	} else {
		if meta.isJoinKeysFixedLength {
			meta.keyMode = FixedSerializedKey
		} else {
			meta.keyMode = VariableSerializedKey
		}
	}
	if needUsedFlag {
		meta.colOffsetInNullMap = 1
		// If needUsedFlag == true, during probe stage, the usedFlag will be accessed by both read/write operator,
		// so atomic read/write is required. We want to keep this atomic operator inside the access of nullmap,
		// then the nullMapLength should be 4 bytes alignment since the smallest unit of atomic.LoadUint32 is UInt32
		meta.nullMapLength = ((len(columnsNeedToBeSaved) + 1 + 31) / 32) * 4
	} else {
		meta.colOffsetInNullMap = 0
		meta.nullMapLength = (len(columnsNeedToBeSaved) + 7) / 8
	}
	meta.rowDataOffset = -1
	if meta.isJoinKeysInlined {
		if meta.isJoinKeysFixedLength {
			meta.rowDataOffset = sizeOfNextPtr + meta.nullMapLength
		} else {
			meta.rowDataOffset = sizeOfNextPtr + meta.nullMapLength + sizeOfElementSize
		}
	} else {
		if meta.isJoinKeysFixedLength {
			meta.rowDataOffset = sizeOfNextPtr + meta.nullMapLength + meta.joinKeysLength
		}
	}
	if meta.isJoinKeysFixedLength && !meta.isJoinKeysInlined {
		meta.fakeKeyByte = make([]byte, meta.joinKeysLength)
	}
	return meta
}
