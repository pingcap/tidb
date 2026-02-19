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
	"bytes"
	"encoding/binary"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

func GenIndexKey(loc *time.Location, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	phyTblID int64, indexedValues []types.Datum, h kv.Handle, buf []byte) (key []byte, distinct bool, err error) {
	if idxInfo.Unique {
		// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
		// A UNIQUE index creates a constraint such that all values in the index must be distinct.
		// An error occurs if you try to add a new row with a key value that matches an existing row.
		// For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
		distinct = true
		for _, cv := range indexedValues {
			if cv.IsNull() {
				distinct = false
				break
			}
		}
	}
	// For string columns, indexes can be created using only the leading part of column values,
	// using col_name(length) syntax to specify an index prefix length.
	TruncateIndexValues(tblInfo, idxInfo, indexedValues)
	key = GetIndexKeyBuf(buf, RecordRowKeyLen+len(indexedValues)*9+9)
	key = appendTableIndexPrefix(key, phyTblID)
	key = codec.EncodeInt(key, idxInfo.ID)
	key, err = codec.EncodeKey(loc, key, indexedValues...)
	if err != nil {
		return nil, false, err
	}
	if !distinct && h != nil {
		// For PartitionHandle on global indexes V1+, we must encode BOTH partition ID and inner handle
		// in the key to prevent collisions when different partitions have duplicate handles.
		// This is critical after EXCHANGE PARTITION, which can create duplicate _tidb_rowid values.
		// Only use the new format for version >= V1. Legacy indexes (version 0) use the old format.
		if idxInfo.GlobalIndexVersion >= model.GlobalIndexVersionV1 {
			if tblInfo.HasClusteredIndex() {
				return nil, false, errors.New("clustered index is not supported in GlobalIndexVersionV1+")
			}
			ph, ok := h.(kv.PartitionHandle)
			if !ok {
				return nil, false, errors.New("handle is not a PartitionHandle in GlobalIndexVersionV1+")
			}
			// Encode as: PartitionIDFlag + partition_id (8 bytes) + inner_handle_encoded
			key = append(key, PartitionIDFlag)
			key = codec.EncodeInt(key, ph.PartitionID)
		}

		if h.IsInt() {
			// We choose the efficient path here instead of calling `codec.EncodeKey`
			// because the int handle must be an int64, and it must be comparable.
			// This remains correct until codec.encodeSignedInt is changed.
			key = append(key, codec.IntHandleFlag)
			key = codec.EncodeInt(key, h.IntValue())
		} else {
			key = append(key, h.Encoded()...)
		}
	}
	return
}

// TempIndexPrefix used to generate temporary index ID from index ID.
const TempIndexPrefix = 0x7fff000000000000

// IndexIDMask used to get index id from index ID/temp index ID.
const IndexIDMask = 0xffffffffffff

// IndexKey2TempIndexKey generates a temporary index key.
func IndexKey2TempIndexKey(key []byte) {
	idxIDBytes := key[prefixLen : prefixLen+idLen]
	idxID := codec.DecodeCmpUintToInt(binary.BigEndian.Uint64(idxIDBytes))
	eid := codec.EncodeIntToCmpUint(TempIndexPrefix | idxID)
	binary.BigEndian.PutUint64(key[prefixLen:], eid)
}

// TempIndexKey2IndexKey generates an index key from temporary index key.
func TempIndexKey2IndexKey(tempIdxKey []byte) {
	tmpIdxIDBytes := tempIdxKey[prefixLen : prefixLen+idLen]
	tempIdxID := codec.DecodeCmpUintToInt(binary.BigEndian.Uint64(tmpIdxIDBytes))
	eid := codec.EncodeIntToCmpUint(tempIdxID & IndexIDMask)
	binary.BigEndian.PutUint64(tempIdxKey[prefixLen:], eid)
}

// IsTempIndexKey checks whether the input key is for a temp index.
func IsTempIndexKey(indexKey []byte) (isTemp bool) {
	indexIDKey := indexKey[prefixLen : prefixLen+8]
	indexID := codec.DecodeCmpUintToInt(binary.BigEndian.Uint64(indexIDKey))
	tempIndexID := int64(TempIndexPrefix) | indexID
	return tempIndexID == indexID
}

// TempIndexValueFlag is the flag of temporary index value.
type TempIndexValueFlag byte

const (
	// TempIndexValueFlagNormal means the following value is a distinct the normal index value.
	TempIndexValueFlagNormal TempIndexValueFlag = iota
	// TempIndexValueFlagNonDistinctNormal means the following value is the non-distinct normal index value.
	TempIndexValueFlagNonDistinctNormal
	// TempIndexValueFlagDeleted means the following value is the distinct and deleted index value.
	TempIndexValueFlagDeleted
	// TempIndexValueFlagNonDistinctDeleted means the following value is the non-distinct deleted index value.
	TempIndexValueFlagNonDistinctDeleted
)

// TempIndexValue is the value of temporary index.
// It contains one or more element, each element represents a history index operations on the original index.
// A temp index value element is encoded as one of:
//   - [flag 1 byte][value_length 2 bytes ] [value value_len bytes]   [key_version 1 byte] {distinct normal}
//   - [flag 1 byte][value value_len bytes]                           [key_version 1 byte] {non-distinct normal}
//   - [flag 1 byte][handle_length 2 bytes] [handle handle_len bytes] [key_version 1 byte] {distinct deleted}
//   - [flag 1 byte]                                                  [key_version 1 byte] {non-distinct deleted}
//
// The temp index value is encoded as:
//   - [element 1][element 2]...[element n] {for distinct values}
//   - [element 1]                          {for non-distinct values}
type TempIndexValue []*TempIndexValueElem

// IsEmpty checks whether the value is empty.
func (v TempIndexValue) IsEmpty() bool {
	return len(v) == 0
}

// Current returns the current latest temp index value.
func (v TempIndexValue) Current() *TempIndexValueElem {
	return v[len(v)-1]
}

// FilterOverwritten is used by the temp index merge process to remove the overwritten index operations.
// For example, the value {temp_idx_key -> [h2, h2d, h3, h1d]} recorded four operations on the original index.
// Since 'h2d' overwrites 'h2', we can remove 'h2' from the value.
func (v TempIndexValue) FilterOverwritten() TempIndexValue {
	if len(v) <= 1 || !v[0].Distinct {
		return v
	}
	occurred := kv.NewHandleMap()
	for i := len(v) - 1; i >= 0; i-- {
		if _, ok := occurred.Get(v[i].Handle); !ok {
			occurred.Set(v[i].Handle, struct{}{})
		} else {
			v[i] = nil
		}
	}
	ret := v[:0]
	for _, elem := range v {
		if elem != nil {
			ret = append(ret, elem)
		}
	}
	return ret
}

// TempIndexValueElem represents a history index operations on the original index.
// A temp index value element is encoded as one of:
//   - [flag 1 byte][value_length 2 bytes ] [value value_len bytes]   [key_version 1 byte] {distinct normal}
//   - [flag 1 byte][value value_len bytes]                           [key_version 1 byte] {non-distinct normal}
//   - [flag 1 byte][handle_length 2 bytes] [handle handle_len bytes] [partitionIdFlag 1 byte] [partitionID 8 bytes] [key_version 1 byte] {distinct deleted}
//   - [flag 1 byte]                                                  [key_version 1 byte] {non-distinct deleted}
type TempIndexValueElem struct {
	Value    []byte
	Handle   kv.Handle
	KeyVer   byte
	Delete   bool
	Distinct bool

	// Global means it's a global Index, for partitioned tables. Currently only used in `distinct` + `deleted` scenarios.
	Global bool
}

const (
	// TempIndexKeyTypeNone means the key is not a temporary index key.
	TempIndexKeyTypeNone byte = 0
	// TempIndexKeyTypeDelete indicates this value is written in the delete-only stage.
	TempIndexKeyTypeDelete byte = 'd'
	// TempIndexKeyTypeBackfill indicates this value is written in the backfill stage.
	TempIndexKeyTypeBackfill byte = 'b'
	// TempIndexKeyTypeMerge indicates this value is written in the merge stage.
	TempIndexKeyTypeMerge byte = 'm'
	// TempIndexKeyTypePartitionIDFlag indicates the following value is partition id.
	TempIndexKeyTypePartitionIDFlag byte = 'p'
)

// Encode encodes the temp index value.
func (v *TempIndexValueElem) Encode(buf []byte) []byte {
	if v.Delete {
		if v.Distinct {
			handle := v.Handle
			var hEncoded []byte
			var hLen uint16
			if handle.IsInt() {
				hEncoded = codec.EncodeUint(hEncoded, uint64(handle.IntValue()))
				hLen = 8
			} else {
				hEncoded = handle.Encoded()
				hLen = uint16(len(hEncoded))
			}
			// flag + handle length + handle + [partition id] + temp key version
			if buf == nil {
				l := hLen + 4
				if v.Global {
					l += 9
				}
				buf = make([]byte, 0, l)
			}
			buf = append(buf, byte(TempIndexValueFlagDeleted))
			buf = append(buf, byte(hLen>>8), byte(hLen))
			buf = append(buf, hEncoded...)
			if v.Global {
				buf = append(buf, TempIndexKeyTypePartitionIDFlag)
				buf = append(buf, codec.EncodeInt(nil, v.Handle.(kv.PartitionHandle).PartitionID)...)
			}
			buf = append(buf, v.KeyVer)
			return buf
		}
		// flag + temp key version
		if buf == nil {
			buf = make([]byte, 0, 2)
		}
		buf = append(buf, byte(TempIndexValueFlagNonDistinctDeleted))
		buf = append(buf, v.KeyVer)
		return buf
	}
	if v.Distinct {
		// flag + value length + value + temp key version
		if buf == nil {
			buf = make([]byte, 0, len(v.Value)+4)
		}
		buf = append(buf, byte(TempIndexValueFlagNormal))
		vLen := uint16(len(v.Value))
		buf = append(buf, byte(vLen>>8), byte(vLen))
		buf = append(buf, v.Value...)
		buf = append(buf, v.KeyVer)
		return buf
	}
	// flag + value + temp key version
	if buf == nil {
		buf = make([]byte, 0, len(v.Value)+2)
	}
	buf = append(buf, byte(TempIndexValueFlagNonDistinctNormal))
	buf = append(buf, v.Value...)
	buf = append(buf, v.KeyVer)
	return buf
}

// DecodeTempIndexValue decodes the temp index value.
func DecodeTempIndexValue(value []byte) (TempIndexValue, error) {
	var (
		values []*TempIndexValueElem
		err    error
	)
	for len(value) > 0 {
		v := &TempIndexValueElem{}
		value, err = v.DecodeOne(value)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

// DecodeOne decodes one temp index value element.
func (v *TempIndexValueElem) DecodeOne(b []byte) (remain []byte, err error) {
	flag := TempIndexValueFlag(b[0])
	b = b[1:]
	switch flag {
	case TempIndexValueFlagNormal:
		vLen := (uint16(b[0]) << 8) + uint16(b[1])
		b = b[2:]
		v.Value = b[:vLen]
		b = b[vLen:]
		v.KeyVer = b[0]
		b = b[1:]
		v.Distinct = true
		return b, err
	case TempIndexValueFlagNonDistinctNormal:
		v.Value = b[:len(b)-1]
		v.KeyVer = b[len(b)-1]
		return nil, nil
	case TempIndexValueFlagDeleted:
		hLen := (uint16(b[0]) << 8) + uint16(b[1])
		b = b[2:]
		if hLen == idLen {
			v.Handle = DecodeIntHandleInIndexValue(b[:idLen])
		} else {
			v.Handle, _ = kv.NewCommonHandle(b[:hLen])
		}
		b = b[hLen:]
		if b[0] == TempIndexKeyTypePartitionIDFlag {
			v.Global = true
			var pid int64
			_, pid, err = codec.DecodeInt(b[1:9])
			if err != nil {
				return nil, err
			}
			v.Handle = kv.NewPartitionHandle(pid, v.Handle)
			b = b[9:]
		}
		v.KeyVer = b[0]
		b = b[1:]
		v.Distinct = true
		v.Delete = true
		return b, nil
	case TempIndexValueFlagNonDistinctDeleted:
		v.KeyVer = b[0]
		b = b[1:]
		v.Delete = true
		return b, nil
	default:
		return nil, errors.New("invalid temp index value")
	}
}

// TempIndexValueIsUntouched returns true if the value is untouched.
// All the temp index value has the suffix of temp key version.
// All the temp key versions differ from the uncommitted KV flag.
func TempIndexValueIsUntouched(b []byte) bool {
	if len(b) > 0 && b[len(b)-1] == kv.UnCommitIndexKVFlag {
		return true
	}
	return false
}

// GenIndexValuePortal is the portal for generating index value.
// Value layout:
//
//	+-- IndexValueVersion0  (with restore data, or common handle, or index is global)
//	|
//	|  Layout: TailLen | Options      | Padding      | [IntHandle] | [UntouchedFlag]
//	|  Length:   1     | len(options) | len(padding) |    8        |     1
//	|
//	|  TailLen:       len(padding) + len(IntHandle) + len(UntouchedFlag)
//	|  Options:       Encode some value for new features, such as common handle, new collations or global index.
//	|                 See below for more information.
//	|  Padding:       Ensure length of value always >= 10. (or >= 11 if UntouchedFlag exists.)
//	|  IntHandle:     Only exists when table use int handles and index is unique.
//	|  UntouchedFlag: Only exists when index is untouched.
//	|
//	+-- Old Encoding (without restore data, integer handle, local)
//	|
//	|  Layout: [Handle] | [UntouchedFlag]
//	|  Length:   8      |     1
//	|
//	|  Handle:        Only exists in unique index.
//	|  UntouchedFlag: Only exists when index is untouched.
//	|
//	|  If neither Handle nor UntouchedFlag exists, value will be one single byte '0' (i.e. []byte{'0'}).
//	|  Length of value <= 9, use to distinguish from the new encoding.
//	|
//	+-- IndexValueForClusteredIndexVersion1
//	|
//	|  Layout: TailLen |    VersionFlag  |    Version     ｜ Options      |   [UntouchedFlag]
//	|  Length:   1     |        1        |      1         |  len(options) |         1
//	|
//	|  TailLen:       len(UntouchedFlag)
//	|  Options:       Encode some value for new features, such as common handle, new collations or global index.
//	|                 See below for more information.
//	|  UntouchedFlag: Only exists when index is untouched.
//	|
//	|  Layout of Options:
//	|
//	|     Segment:             Common Handle                 |     Global Index      |   New Collation
//	|     Layout:  CHandle flag | CHandle Len | CHandle      | PidFlag | PartitionID |    restoreData
//	|     Length:     1         | 2           | len(CHandle) |    1    |    8        |   len(restoreData)
//	|
//	|     Common Handle Segment: Exists when unique index used common handles.
//	|     Global Index Segment:  Exists when index is global.
//	|     New Collation Segment: Exists when new collation is used and index or handle contains non-binary string.
//	|     In v4.0, restored data contains all the index values. For example, (a int, b char(10)) and index (a, b).
//	|     The restored data contains both the values of a and b.
//	|     In v5.0, restored data contains only non-binary data(except for char and _bin). In the above example, the restored data contains only the value of b.
//	|     Besides, if the collation of b is _bin, then restored data is an integer indicate the spaces are truncated. Then we use sortKey
//	|     and the restored data together to restore original data.
func GenIndexValuePortal(loc *time.Location, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	needRestoredData bool, distinct bool, untouched bool, indexedValues []types.Datum, h kv.Handle,
	partitionID int64, restoredData []types.Datum, buf []byte) ([]byte, error) {
	if tblInfo.IsCommonHandle && tblInfo.CommonHandleVersion == 1 {
		return GenIndexValueForClusteredIndexVersion1(loc, tblInfo, idxInfo, needRestoredData, distinct, untouched, indexedValues, h, partitionID, restoredData, buf)
	}
	return genIndexValueVersion0(loc, tblInfo, idxInfo, needRestoredData, distinct, untouched, indexedValues, h, partitionID, buf)
}

// TryGetCommonPkColumnRestoredIds get the IDs of primary key columns which need restored data if the table has common handle.
// Caller need to make sure the table has common handle.
func TryGetCommonPkColumnRestoredIds(tbl *model.TableInfo) []int64 {
	var pkColIDs []int64
	var pkIdx *model.IndexInfo
	for _, idx := range tbl.Indices {
		if idx.Primary {
			pkIdx = idx
			break
		}
	}
	if pkIdx == nil {
		return pkColIDs
	}
	for _, idxCol := range pkIdx.Columns {
		if types.NeedRestoredData(&tbl.Columns[idxCol.Offset].FieldType) {
			pkColIDs = append(pkColIDs, tbl.Columns[idxCol.Offset].ID)
		}
	}
	return pkColIDs
}

// GenIndexValueForClusteredIndexVersion1 generates the index value for the clustered index with version 1(New in v5.0.0).
func GenIndexValueForClusteredIndexVersion1(loc *time.Location, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	idxValNeedRestoredData bool, distinct bool, untouched bool, indexedValues []types.Datum, h kv.Handle,
	partitionID int64, handleRestoredData []types.Datum, buf []byte) ([]byte, error) {
	var idxVal []byte
	if buf == nil {
		idxVal = make([]byte, 0)
	} else {
		idxVal = buf[:0]
	}
	idxVal = append(idxVal, 0)
	tailLen := 0
	// Version info.
	idxVal = append(idxVal, IndexVersionFlag)
	idxVal = append(idxVal, byte(1))

	if distinct {
		idxVal = encodeCommonHandle(idxVal, h)
	}
	if idxInfo.Global {
		idxVal = encodePartitionID(idxVal, partitionID)
	}
	if idxValNeedRestoredData || len(handleRestoredData) > 0 {
		colIds := make([]int64, 0, len(idxInfo.Columns))
		allRestoredData := make([]types.Datum, 0, len(handleRestoredData)+len(idxInfo.Columns))
		for i, idxCol := range idxInfo.Columns {
			col := tblInfo.Columns[idxCol.Offset]
			// If the column is the primary key's column,
			// the restored data will be written later. Skip writing it here to avoid redundancy.
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				continue
			}
			if model.ColumnNeedRestoredData(idxCol, tblInfo.Columns) {
				colIds = append(colIds, col.ID)
				if collate.IsBinCollation(model.GetIdxChangingFieldType(idxCol, col).GetCollate()) {
					allRestoredData = append(allRestoredData, types.NewUintDatum(uint64(stringutil.GetTailSpaceCount(indexedValues[i].GetString()))))
				} else {
					allRestoredData = append(allRestoredData, indexedValues[i])
				}
			}
		}

		if len(handleRestoredData) > 0 {
			pkColIDs := TryGetCommonPkColumnRestoredIds(tblInfo)
			colIds = append(colIds, pkColIDs...)
			allRestoredData = append(allRestoredData, handleRestoredData...)
		}

		rd := rowcodec.Encoder{Enable: true}
		var err error
		idxVal, err = rd.Encode(loc, colIds, allRestoredData, nil, idxVal)
		if err != nil {
			return nil, err
		}
	}

	if untouched {
		tailLen = 1
		idxVal = append(idxVal, kv.UnCommitIndexKVFlag)
	}
	idxVal[0] = byte(tailLen)

	return idxVal, nil
}

// genIndexValueVersion0 create index value for both local and global index.
func genIndexValueVersion0(loc *time.Location, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	idxValNeedRestoredData bool, distinct bool, untouched bool, indexedValues []types.Datum, h kv.Handle,
	partitionID int64, buf []byte) ([]byte, error) {
	var idxVal []byte
	if buf == nil {
		idxVal = make([]byte, 0)
	} else {
		idxVal = buf[:0]
	}
	idxVal = append(idxVal, 0)
	newEncode := false
	tailLen := 0
	if !h.IsInt() && distinct {
		idxVal = encodeCommonHandle(idxVal, h)
		newEncode = true
	}
	if idxInfo.Global {
		idxVal = encodePartitionID(idxVal, partitionID)
		newEncode = true
	}
	if idxValNeedRestoredData {
		colIds := make([]int64, len(idxInfo.Columns))
		for i, col := range idxInfo.Columns {
			colIds[i] = tblInfo.Columns[col.Offset].ID
		}
		rd := rowcodec.Encoder{Enable: true}
		// Encode row restored value.
		var err error
		idxVal, err = rd.Encode(loc, colIds, indexedValues, nil, idxVal)
		if err != nil {
			return nil, err
		}
		newEncode = true
	}

	if newEncode {
		if h.IsInt() && distinct {
			// The len of the idxVal is always >= 10 since len (restoredValue) > 0.
			tailLen += 8
			idxVal = append(idxVal, EncodeHandleInUniqueIndexValue(h, false)...)
		} else if len(idxVal) < 10 {
			// Padding the len to 10
			paddingLen := 10 - len(idxVal)
			tailLen += paddingLen
			idxVal = append(idxVal, bytes.Repeat([]byte{0x0}, paddingLen)...)
		}
		if untouched {
			// If index is untouched and fetch here means the key is exists in TiKV, but not in txn mem-buffer,
			// then should also write the untouched index key/value to mem-buffer to make sure the data
			// is consistent with the index in txn mem-buffer.
			tailLen++
			idxVal = append(idxVal, kv.UnCommitIndexKVFlag)
		}
		idxVal[0] = byte(tailLen)
	} else {
		// Old index value encoding.
		if buf == nil {
			idxVal = make([]byte, 0)
		} else {
			idxVal = buf[:0]
		}
		if distinct {
			idxVal = EncodeHandleInUniqueIndexValue(h, untouched)
		}
		if untouched {
			// If index is untouched and fetch here means the key is exists in TiKV, but not in txn mem-buffer,
			// then should also write the untouched index key/value to mem-buffer to make sure the data
			// is consistent with the index in txn mem-buffer.
			idxVal = append(idxVal, kv.UnCommitIndexKVFlag)
		}
		if len(idxVal) == 0 {
			idxVal = append(idxVal, byte('0'))
		}
	}
	return idxVal, nil
}

// TruncateIndexValues truncates the index values created using only the leading part of column values.
func TruncateIndexValues(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, indexedValues []types.Datum) {
	for i := range indexedValues {
		idxCol := idxInfo.Columns[i]
		tblCol := tblInfo.Columns[idxCol.Offset]
		TruncateIndexValue(&indexedValues[i], idxCol, tblCol)
	}
}

// TruncateIndexValue truncate one value in the index.
func TruncateIndexValue(v *types.Datum, idxCol *model.IndexColumn, tblCol *model.ColumnInfo) {
	noPrefixIndex := idxCol.Length == types.UnspecifiedLength
	if noPrefixIndex {
		return
	}
	notStringType := v.Kind() != types.KindString && v.Kind() != types.KindBytes
	if notStringType {
		return
	}
	colValue := v.GetBytes()
	if tblCol.GetCharset() == charset.CharsetBin || tblCol.GetCharset() == charset.CharsetASCII {
		// Count character length by bytes if charset is binary or ascii.
		if len(colValue) > idxCol.Length {
			// truncate value and limit its length
			if v.Kind() == types.KindBytes {
				v.SetBytes(colValue[:idxCol.Length])
			} else {
				v.SetString(v.GetString()[:idxCol.Length], tblCol.GetCollate())
			}
		}
	} else if utf8.RuneCount(colValue) > idxCol.Length {
		// Count character length by characters for other rune-based charsets, they are all internally encoded as UTF-8.
		rs := bytes.Runes(colValue)
		truncateStr := string(rs[:idxCol.Length])
		// truncate value and limit its length
		v.SetString(truncateStr, tblCol.GetCollate())
	}
}

// EncodeHandleInUniqueIndexValue encodes handle in data.
func EncodeHandleInUniqueIndexValue(h kv.Handle, isUntouched bool) []byte {
	if h.IsInt() {
		var data [8]byte
		binary.BigEndian.PutUint64(data[:], uint64(h.IntValue()))
		return data[:]
	}
	var untouchedFlag byte
	if isUntouched {
		untouchedFlag = 1
	}
	return encodeCommonHandle([]byte{untouchedFlag}, h)
}

func encodeCommonHandle(idxVal []byte, h kv.Handle) []byte {
	idxVal = append(idxVal, CommonHandleFlag)
	hLen := uint16(len(h.Encoded()))
	idxVal = append(idxVal, byte(hLen>>8), byte(hLen))
	idxVal = append(idxVal, h.Encoded()...)
	return idxVal
}

func encodePartitionID(idxVal []byte, partitionID int64) []byte {
	idxVal = append(idxVal, PartitionIDFlag)
	idxVal = codec.EncodeInt(idxVal, partitionID)
	return idxVal
}

// IndexValueSegments use to store result of SplitIndexValue.
type IndexValueSegments struct {
	CommonHandle   []byte
	PartitionID    []byte
	RestoredValues []byte
	IntHandle      []byte
}

// SplitIndexValue decodes segments in index value for both non-clustered and clustered table.
func SplitIndexValue(value []byte) (segs IndexValueSegments) {
	if getIndexVersion(value) == 0 {
		// For Old Encoding (IntHandle without any others options)
		if len(value) <= MaxOldEncodeValueLen {
			segs.IntHandle = value
			return segs
		}
		// For IndexValueVersion0
		return splitIndexValueForIndexValueVersion0(value)
	}
	// For IndexValueForClusteredIndexVersion1
	return splitIndexValueForClusteredIndexVersion1(value)
}

// splitIndexValueForIndexValueVersion0 splits index value into segments.
func splitIndexValueForIndexValueVersion0(value []byte) (segs IndexValueSegments) {
	tailLen := int(value[0])
	tail := value[len(value)-tailLen:]
	value = value[1 : len(value)-tailLen]
	if len(tail) >= 8 {
		segs.IntHandle = tail[:8]
	}
	if len(value) > 0 && value[0] == CommonHandleFlag {
		handleLen := uint16(value[1])<<8 + uint16(value[2])
		handleEndOff := 3 + handleLen
		segs.CommonHandle = value[3:handleEndOff]
		value = value[handleEndOff:]
	}
	if len(value) > 0 && value[0] == PartitionIDFlag {
		segs.PartitionID = value[1:9]
		value = value[9:]
	}
	if len(value) > 0 && value[0] == RestoreDataFlag {
		segs.RestoredValues = value
	}
	return
}

// splitIndexValueForClusteredIndexVersion1 splits index value into segments.
func splitIndexValueForClusteredIndexVersion1(value []byte) (segs IndexValueSegments) {
	tailLen := int(value[0])
	// Skip the tailLen and version info.
	value = value[3 : len(value)-tailLen]
	if len(value) > 0 && value[0] == CommonHandleFlag {
		handleLen := uint16(value[1])<<8 + uint16(value[2])
		handleEndOff := 3 + handleLen
		segs.CommonHandle = value[3:handleEndOff]
		value = value[handleEndOff:]
	}
	if len(value) > 0 && value[0] == PartitionIDFlag {
		segs.PartitionID = value[1:9]
		value = value[9:]
	}
	if len(value) > 0 && value[0] == RestoreDataFlag {
		segs.RestoredValues = value
	}
	return
}

func decodeIndexKvForClusteredIndexVersion1(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo) ([][]byte, error) {
	var resultValues [][]byte
	var keySuffix []byte
	var handle kv.Handle
	var err error
	segs := splitIndexValueForClusteredIndexVersion1(value)
	resultValues, keySuffix, err = CutIndexKeyNew(key, colsLen)
	if err != nil {
		return nil, err
	}
	if segs.RestoredValues != nil {
		resultValues, err = decodeRestoredValuesV5(columns[:colsLen], resultValues, segs.RestoredValues)
		if err != nil {
			return nil, err
		}
	}
	if hdStatus == HandleNotNeeded {
		return resultValues, nil
	}
	if segs.CommonHandle != nil {
		// In unique common handle index.
		handle, err = kv.NewCommonHandle(segs.CommonHandle)
	} else {
		// In non-unique index, decode handle in keySuffix.
		handle, err = kv.NewCommonHandle(keySuffix)
	}
	if err != nil {
		return nil, err
	}
	handleBytes, err := reEncodeHandleConsiderNewCollation(handle, columns[colsLen:], segs.RestoredValues)
	if err != nil {
		return nil, err
	}
	resultValues = append(resultValues, handleBytes...)
	if segs.PartitionID != nil {
		_, pid, err := codec.DecodeInt(segs.PartitionID)
		if err != nil {
			return nil, err
		}
		datum := types.NewIntDatum(pid)
		pidBytes, err := codec.EncodeValue(time.UTC, nil, datum)
		if err != nil {
			return nil, err
		}
		resultValues = append(resultValues, pidBytes)
	}
	return resultValues, nil
}

// decodeIndexKvGeneral decodes index key value pair of new layout in an extensible way.
func decodeIndexKvGeneral(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo) ([][]byte, error) {
	var resultValues [][]byte
	var keySuffix []byte
	var handle kv.Handle
	var err error
	segs := splitIndexValueForIndexValueVersion0(value)
	resultValues, keySuffix, err = CutIndexKeyNew(key, colsLen)
	if err != nil {
		return nil, err
	}
	if segs.RestoredValues != nil { // new collation
		resultValues, err = decodeRestoredValues(columns[:colsLen], segs.RestoredValues)
		if err != nil {
			return nil, err
		}
	}
	if hdStatus == HandleNotNeeded {
		return resultValues, nil
	}

	if segs.IntHandle != nil {
		// In unique int handle index.
		handle = DecodeIntHandleInIndexValue(segs.IntHandle)
	} else if segs.CommonHandle != nil {
		// In unique common handle index.
		handle, err = decodeHandleInIndexKey(segs.CommonHandle)
		if err != nil {
			return nil, err
		}
	} else {
		// In non-unique index, decode handle in keySuffix
		handle, err = decodeHandleInIndexKey(keySuffix)
		if err != nil {
			return nil, err
		}
	}
	handleBytes, err := reEncodeHandle(handle, hdStatus == HandleIsUnsigned)
	if err != nil {
		return nil, err
	}
	resultValues = append(resultValues, handleBytes...)
	if segs.PartitionID != nil {
		_, pid, err := codec.DecodeInt(segs.PartitionID)
		if err != nil {
			return nil, err
		}
		datum := types.NewIntDatum(pid)
		pidBytes, err := codec.EncodeValue(time.UTC, nil, datum)
		if err != nil {
			return nil, err
		}
		resultValues = append(resultValues, pidBytes)
	}
	return resultValues, nil
}

// IndexKVIsUnique uses to judge if an index is unique, it can handle the KV committed by txn already, it doesn't consider the untouched flag.
func IndexKVIsUnique(value []byte) bool {
	if len(value) <= MaxOldEncodeValueLen {
		return len(value) == 8
	}
	if getIndexVersion(value) == 1 {
		segs := splitIndexValueForClusteredIndexVersion1(value)
		return segs.CommonHandle != nil
	}
	segs := splitIndexValueForIndexValueVersion0(value)
	return segs.IntHandle != nil || segs.CommonHandle != nil
}

// VerifyTableIDForRanges verifies that all given ranges are valid to decode the table id.
func VerifyTableIDForRanges(keyRanges *kv.KeyRanges) ([]int64, error) {
	tids := make([]int64, 0, keyRanges.PartitionNum())
	collectFunc := func(ranges []kv.KeyRange, _ []int) error {
		if len(ranges) == 0 {
			return nil
		}
		tid := DecodeTableID(ranges[0].StartKey)
		if tid <= 0 {
			return errors.New("Incorrect keyRange is constrcuted")
		}
		tids = append(tids, tid)
		for i := 1; i < len(ranges); i++ {
			tmpTID := DecodeTableID(ranges[i].StartKey)
			if tmpTID <= 0 {
				return errors.New("Incorrect keyRange is constrcuted")
			}
			if tid != tmpTID {
				return errors.Errorf("Using multi partition's ranges as single table's")
			}
		}
		return nil
	}
	err := keyRanges.ForEachPartitionWithErr(collectFunc)
	return tids, err
}
