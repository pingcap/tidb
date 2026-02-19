// Copyright 2016 PingCAP, Inc.
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

package tablecodec

import (
	"encoding/binary"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

func EncodeIndexSeekKey(tableID int64, idxID int64, encodedValue []byte) kv.Key {
	key := make([]byte, 0, RecordRowKeyLen+len(encodedValue))
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	key = append(key, encodedValue...)
	return key
}

// CutIndexKey cuts encoded index key into colIDs to bytes slices map.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKey(key kv.Key, colIDs []int64) (values map[int64][]byte, b []byte, err error) {
	b = key[prefixLen+idLen:]
	values = make(map[int64][]byte, len(colIDs))
	for _, id := range colIDs {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values[id] = val
	}
	return
}

// CutIndexPrefix cuts the index prefix.
func CutIndexPrefix(key kv.Key) []byte {
	return key[prefixLen+idLen:]
}

// CutIndexKeyTo cuts encoded index key into colIDs to bytes slices.
// The caller should prepare the memory of the result values.
func CutIndexKeyTo(key kv.Key, values [][]byte) (b []byte, err error) {
	b = key[prefixLen+idLen:]
	length := len(values)
	for i := range length {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values[i] = val
	}
	return
}

// CutIndexKeyNew cuts encoded index key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKeyNew(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	values = make([][]byte, length)
	b, err = CutIndexKeyTo(key, values)
	return
}

// CutCommonHandle cuts encoded common handle key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutCommonHandle(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	b = key[prefixLen:]
	values = make([][]byte, 0, length)
	for range length {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values = append(values, val)
	}
	return
}

// HandleStatus is the handle status in index.
type HandleStatus int

const (
	// HandleDefault means decode handle value as int64 or bytes when DecodeIndexKV.
	HandleDefault HandleStatus = iota
	// HandleIsUnsigned means decode handle value as uint64 when DecodeIndexKV.
	HandleIsUnsigned
	// HandleNotNeeded means no need to decode handle value when DecodeIndexKV.
	HandleNotNeeded
)

// reEncodeHandle encodes the handle as a Datum so it can be properly decoded later.
// If it is common handle, it returns the encoded column values.
// If it is int handle, it is encoded as int Datum or uint Datum decided by the unsigned.
func reEncodeHandle(handle kv.Handle, unsigned bool) ([][]byte, error) {
	handleColLen := 1
	if !handle.IsInt() {
		handleColLen = handle.NumCols()
	}
	result := make([][]byte, 0, handleColLen)
	return reEncodeHandleTo(handle, unsigned, nil, result)
}

func reEncodeHandleTo(handle kv.Handle, unsigned bool, buf []byte, result [][]byte) ([][]byte, error) {
	if !handle.IsInt() {
		handleColLen := handle.NumCols()
		for i := range handleColLen {
			result = append(result, handle.EncodedCol(i))
		}
		return result, nil
	}
	handleDatum := types.NewIntDatum(handle.IntValue())
	if unsigned {
		handleDatum.SetUint64(handleDatum.GetUint64())
	}
	intHandleBytes, err := codec.EncodeValue(time.UTC, buf, handleDatum)
	result = append(result, intHandleBytes)
	return result, err
}

// reEncodeHandleConsiderNewCollation encodes the handle as a Datum so it can be properly decoded later.
func reEncodeHandleConsiderNewCollation(handle kv.Handle, columns []rowcodec.ColInfo, restoreData []byte) ([][]byte, error) {
	handleColLen := handle.NumCols()
	cHandleBytes := make([][]byte, 0, handleColLen)
	for i := range handleColLen {
		cHandleBytes = append(cHandleBytes, handle.EncodedCol(i))
	}
	if len(restoreData) == 0 {
		return cHandleBytes, nil
	}
	// Remove some extra columns(ID < 0), such like `model.ExtraPhysTblID`.
	// They are not belong to common handle and no need to restore data.
	idx := len(columns)
	for idx > 0 && columns[idx-1].ID < 0 {
		idx--
	}
	return decodeRestoredValuesV5(columns[:idx], cHandleBytes, restoreData)
}

func decodeRestoredValues(columns []rowcodec.ColInfo, restoredVal []byte) ([][]byte, error) {
	colIDs := make(map[int64]int, len(columns))
	for i, col := range columns {
		colIDs[col.ID] = i
	}
	// We don't need to decode handle here, and colIDs >= 0 always.
	rd := rowcodec.NewByteDecoder(columns, []int64{-1}, nil, nil)
	resultValues, err := rd.DecodeToBytesNoHandle(colIDs, restoredVal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resultValues, nil
}

// decodeRestoredValuesV5 decodes index values whose format is introduced in TiDB 5.0.
// Unlike the format in TiDB 4.0, the new format is optimized for storage space:
// 1. If the index is a composed index, only the non-binary string column's value need to write to value, not all.
// 2. If a string column's collation is _bin, then we only write the number of the truncated spaces to value.
// 3. If a string column is char, not varchar, then we use the sortKey directly.
func decodeRestoredValuesV5(columns []rowcodec.ColInfo, results [][]byte, restoredVal []byte) ([][]byte, error) {
	colIDOffsets := buildColumnIDOffsets(columns)
	colInfosNeedRestore := buildRestoredColumn(columns)
	rd := rowcodec.NewByteDecoder(colInfosNeedRestore, nil, nil, nil)
	newResults, err := rd.DecodeToBytesNoHandle(colIDOffsets, restoredVal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i := range newResults {
		noRestoreData := len(newResults[i]) == 0
		if noRestoreData {
			newResults[i] = results[i]
			continue
		}
		if collate.IsBinCollation(columns[i].Ft.GetCollate()) {
			noPaddingDatum, err := DecodeColumnValue(results[i], columns[i].Ft, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			paddingCountDatum, err := DecodeColumnValue(newResults[i], types.NewFieldType(mysql.TypeLonglong), nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			noPaddingStr, paddingCount := noPaddingDatum.GetString(), int(paddingCountDatum.GetInt64())
			// Skip if padding count is 0.
			if paddingCount == 0 {
				newResults[i] = results[i]
				continue
			}
			newDatum := &noPaddingDatum
			newDatum.SetString(noPaddingStr+strings.Repeat(" ", paddingCount), newDatum.Collation())
			newResults[i] = newResults[i][:0]
			newResults[i] = append(newResults[i], rowcodec.BytesFlag)
			newResults[i] = codec.EncodeBytes(newResults[i], newDatum.GetBytes())
		}
	}
	return newResults, nil
}

func buildColumnIDOffsets(allCols []rowcodec.ColInfo) map[int64]int {
	colIDOffsets := make(map[int64]int, len(allCols))
	for i, col := range allCols {
		colIDOffsets[col.ID] = i
	}
	return colIDOffsets
}

func buildRestoredColumn(allCols []rowcodec.ColInfo) []rowcodec.ColInfo {
	restoredColumns := make([]rowcodec.ColInfo, 0, len(allCols))
	for i, col := range allCols {
		if !types.NeedRestoredData(col.Ft) {
			continue
		}
		copyColInfo := rowcodec.ColInfo{
			ID: col.ID,
		}
		if collate.IsBinCollation(col.Ft.GetCollate()) {
			// Change the fieldType from string to uint since we store the number of the truncated spaces.
			// NOTE: the corresponding datum is generated as `types.NewUintDatum(paddingSize)`, and the raw data is
			// encoded via `encodeUint`. Thus we should mark the field type as unsigened here so that the BytesDecoder
			// can decode it correctly later. Otherwise there might be issues like #47115.
			copyColInfo.Ft = types.NewFieldType(mysql.TypeLonglong)
			copyColInfo.Ft.AddFlag(mysql.UnsignedFlag)
		} else {
			copyColInfo.Ft = allCols[i].Ft
		}
		restoredColumns = append(restoredColumns, copyColInfo)
	}
	return restoredColumns
}

func decodeIndexKvOldCollation(key, value []byte, hdStatus HandleStatus, buf []byte, resultValues [][]byte) ([][]byte, error) {
	b, err := CutIndexKeyTo(key, resultValues)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if hdStatus == HandleNotNeeded {
		return resultValues, nil
	}
	var handle kv.Handle
	if len(b) > 0 {
		// non-unique index
		handle, err = decodeHandleInIndexKey(b)
		if err != nil {
			return nil, err
		}
		resultValues, err = reEncodeHandleTo(handle, hdStatus == HandleIsUnsigned, buf, resultValues)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		// In unique int handle index.
		handle = DecodeIntHandleInIndexValue(value)
		resultValues, err = reEncodeHandleTo(handle, hdStatus == HandleIsUnsigned, buf, resultValues)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return resultValues, nil
}

func getIndexVersion(value []byte) int {
	if len(value) <= MaxOldEncodeValueLen {
		return 0
	}
	tailLen := int(value[0])
	if (tailLen == 0 || tailLen == 1) && value[1] == IndexVersionFlag {
		return int(value[2])
	}
	return 0
}

// DecodeIndexKVEx looks like DecodeIndexKV, the difference is that it tries to reduce allocations.
func DecodeIndexKVEx(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo, buf []byte, preAlloc [][]byte) ([][]byte, error) {
	if len(value) <= MaxOldEncodeValueLen {
		return decodeIndexKvOldCollation(key, value, hdStatus, buf, preAlloc)
	}
	if getIndexVersion(value) == 1 {
		return decodeIndexKvForClusteredIndexVersion1(key, value, colsLen, hdStatus, columns)
	}
	return decodeIndexKvGeneral(key, value, colsLen, hdStatus, columns)
}

// DecodeIndexKV uses to decode index key values.
//
//	`colsLen` is expected to be index columns count.
//	`columns` is expected to be index columns + handle columns(if hdStatus is not HandleNotNeeded).
func DecodeIndexKV(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo) ([][]byte, error) {
	if len(value) <= MaxOldEncodeValueLen {
		preAlloc := make([][]byte, colsLen, colsLen+len(columns))
		return decodeIndexKvOldCollation(key, value, hdStatus, nil, preAlloc)
	}
	if getIndexVersion(value) == 1 {
		return decodeIndexKvForClusteredIndexVersion1(key, value, colsLen, hdStatus, columns)
	}
	return decodeIndexKvGeneral(key, value, colsLen, hdStatus, columns)
}

// DecodeIndexHandle uses to decode the handle from index key/value.
func DecodeIndexHandle(key, value []byte, colsLen int) (kv.Handle, error) {
	var err error
	b := key[prefixLen+idLen:]
	for range colsLen {
		_, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if len(b) > 0 {
		handle, err := decodeHandleInIndexKey(b)
		if err != nil {
			return nil, err
		}
		// If len(value) >= 9, it may contain partition id.
		// We should decode it and return a partition handle.
		if len(value) >= 9 {
			seg := SplitIndexValue(value)
			if len(seg.PartitionID) != 0 {
				_, pid, err := codec.DecodeInt(seg.PartitionID)
				if err != nil {
					return nil, err
				}
				// For GlobalIndexVersionV1+, the handle from the key may already be a
				// PartitionHandle (partition ID encoded in key). To avoid creating a
				// nested PartitionHandle, extract the inner handle first.
				// For V1: use partition ID from value (authoritative source).
				// TODO: For V2+, use partition ID from key (PartitionHandle) instead.
				if ph, ok := handle.(kv.PartitionHandle); ok {
					handle = ph.Handle
				}
				handle = kv.NewPartitionHandle(pid, handle)
			}
		}
		return handle, nil
	} else if len(value) >= 8 {
		return DecodeHandleInIndexValue(value)
	}
	// Should never execute to here.
	return nil, errors.Errorf("no handle in index key: %v, value: %v", key, value)
}

func decodeHandleInIndexKey(keySuffix []byte) (kv.Handle, error) {
	// Check if this is a PartitionHandle (for global non-unique indexes V1+)
	if len(keySuffix) > 0 && keySuffix[0] == PartitionIDFlag {
		// Format: PartitionIDFlag + partition_id (8 bytes) + inner_handle
		keySuffix = keySuffix[1:] // Skip the flag
		remain, partID, err := codec.DecodeInt(keySuffix)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Decode the inner handle
		innerHandle, err := decodeHandleInIndexKey(remain)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return kv.NewPartitionHandle(partID, innerHandle), nil
	}

	remain, d, err := codec.DecodeOne(keySuffix)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(remain) == 0 && d.Kind() == types.KindInt64 {
		return kv.IntHandle(d.GetInt64()), nil
	}
	return kv.NewCommonHandle(keySuffix)
}

// DecodeHandleInIndexValue decodes handle in unqiue index value.
func DecodeHandleInIndexValue(value []byte) (handle kv.Handle, err error) {
	if len(value) <= MaxOldEncodeValueLen {
		return DecodeIntHandleInIndexValue(value), nil
	}
	seg := SplitIndexValue(value)
	if len(seg.IntHandle) != 0 {
		handle = DecodeIntHandleInIndexValue(seg.IntHandle)
	}
	if len(seg.CommonHandle) != 0 {
		handle, err = kv.NewCommonHandle(seg.CommonHandle)
		if err != nil {
			return nil, err
		}
	}
	if len(seg.PartitionID) != 0 {
		_, pid, err := codec.DecodeInt(seg.PartitionID)
		if err != nil {
			return nil, err
		}
		handle = kv.NewPartitionHandle(pid, handle)
	}
	return handle, nil
}

// DecodeIntHandleInIndexValue uses to decode index value as int handle id.
func DecodeIntHandleInIndexValue(data []byte) kv.Handle {
	return kv.IntHandle(binary.BigEndian.Uint64(data))
}

// EncodeTableIndexPrefix encodes index prefix with tableID and idxID.
func EncodeTableIndexPrefix(tableID, idxID int64) kv.Key {
	key := make([]byte, 0, prefixLen+idLen)
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	return key
}

// EncodeTablePrefix encodes the table prefix to generate a key
func EncodeTablePrefix(tableID int64) kv.Key {
	key := make([]byte, 0, tablePrefixLength+idLen)
	key = append(key, tablePrefix...)
	key = codec.EncodeInt(key, tableID)
	return key
}

// appendTableRecordPrefix appends table record prefix  "t[tableID]_r".
func appendTableRecordPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// appendTableIndexPrefix appends table index prefix  "t[tableID]_i".
func appendTableIndexPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

// GenTableRecordPrefix composes record prefix with tableID: "t[tableID]_r".
func GenTableRecordPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(recordPrefixSep))
	return appendTableRecordPrefix(buf, tableID)
}

// GenTableIndexPrefix composes index prefix with tableID: "t[tableID]_i".
func GenTableIndexPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(indexPrefixSep))
	return appendTableIndexPrefix(buf, tableID)
}

// IsRecordKey is used to check whether the key is an record key.
func IsRecordKey(k []byte) bool {
	return len(k) > 11 && k[0] == 't' && k[10] == 'r'
}

// IsIndexKey is used to check whether the key is an index key.
func IsIndexKey(k []byte) bool {
	return len(k) > 11 && k[0] == 't' && k[10] == 'i'
}

// IsTableKey is used to check whether the key is a table key.
func IsTableKey(k []byte) bool {
	return len(k) == 9 && k[0] == 't'
}

// IsUntouchedIndexKValue uses to check whether the key is index key, and the value is untouched,
// since the untouched index key/value is no need to commit.
func IsUntouchedIndexKValue(k, v []byte) bool {
	if !IsIndexKey(k) {
		return false
	}
	vLen := len(v)
	if IsTempIndexKey(k) {
		return vLen > 0 && v[vLen-1] == kv.UnCommitIndexKVFlag
	}
	if vLen <= MaxOldEncodeValueLen {
		return (vLen == 1 || vLen == 9) && v[vLen-1] == kv.UnCommitIndexKVFlag
	}
	// New index value format
	tailLen := int(v[0])
	if tailLen < 8 {
		// Non-unique index.
		return tailLen >= 1 && v[vLen-1] == kv.UnCommitIndexKVFlag
	}
	// Unique index
	return tailLen == 9
}

// GenTablePrefix composes table record and index prefix: "t[tableID]".
func GenTablePrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8)
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	return buf
}

// TruncateToRowKeyLen truncates the key to row key length if the key is longer than row key.
func TruncateToRowKeyLen(key kv.Key) kv.Key {
	if len(key) > RecordRowKeyLen {
		return key[:RecordRowKeyLen]
	}
	return key
}

// GetTableHandleKeyRange returns table handle's key range with tableID.
func GetTableHandleKeyRange(tableID int64) (startKey, endKey []byte) {
	startKey = EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MinInt64))
	endKey = EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MaxInt64))
	return
}

// GetTableIndexKeyRange returns table index's key range with tableID and indexID.
func GetTableIndexKeyRange(tableID, indexID int64) (startKey, endKey []byte) {
	startKey = EncodeIndexSeekKey(tableID, indexID, nil)
	endKey = EncodeIndexSeekKey(tableID, indexID, []byte{255})
	return
}

// GetIndexKeyBuf reuse or allocate buffer
func GetIndexKeyBuf(buf []byte, defaultCap int) []byte {
	if buf != nil {
		return buf[:0]
	}
	return make([]byte, 0, defaultCap)
}
