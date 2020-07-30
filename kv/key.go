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
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// Key represents high-level Key type.
type Key []byte

// Next returns the next key in byte-order.
func (k Key) Next() Key {
	// add 0x0 to the end of key
	buf := make([]byte, len(k)+1)
	copy(buf, k)
	return buf
}

// PrefixNext returns the next prefix key.
//
// Assume there are keys like:
//
//   rowkey1
//   rowkey1_column1
//   rowkey1_column2
//   rowKey2
//
// If we seek 'rowkey1' Next, we will get 'rowkey1_column1'.
// If we seek 'rowkey1' PrefixNext, we will get 'rowkey2'.
func (k Key) PrefixNext() Key {
	buf := make([]byte, len(k))
	copy(buf, k)
	var i int
	for i = len(k) - 1; i >= 0; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}
	if i == -1 {
		copy(buf, k)
		buf = append(buf, 0)
	}
	return buf
}

// Cmp returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (k Key) Cmp(another Key) int {
	return bytes.Compare(k, another)
}

// HasPrefix tests whether the Key begins with prefix.
func (k Key) HasPrefix(prefix Key) bool {
	return bytes.HasPrefix(k, prefix)
}

// Clone returns a deep copy of the Key.
func (k Key) Clone() Key {
	ck := make([]byte, len(k))
	copy(ck, k)
	return ck
}

// String implements fmt.Stringer interface.
func (k Key) String() string {
	return hex.EncodeToString(k)
}

// KeyRange represents a range where StartKey <= key < EndKey.
type KeyRange struct {
	StartKey Key
	EndKey   Key
}

// IsPoint checks if the key range represents a point.
func (r *KeyRange) IsPoint() bool {
	if len(r.StartKey) != len(r.EndKey) {
		// Works like
		//   return bytes.Equal(r.StartKey.Next(), r.EndKey)

		startLen := len(r.StartKey)
		return startLen+1 == len(r.EndKey) &&
			r.EndKey[startLen] == 0 &&
			bytes.Equal(r.StartKey, r.EndKey[:startLen])
	}
	// Works like
	//   return bytes.Equal(r.StartKey.PrefixNext(), r.EndKey)

	i := len(r.StartKey) - 1
	for ; i >= 0; i-- {
		if r.StartKey[i] != 255 {
			break
		}
		if r.EndKey[i] != 0 {
			return false
		}
	}
	if i < 0 {
		// In case all bytes in StartKey are 255.
		return false
	}
	// The byte at diffIdx in StartKey should be one less than the byte at diffIdx in EndKey.
	// And bytes in StartKey and EndKey before diffIdx should be equal.
	diffOneIdx := i
	return r.StartKey[diffOneIdx]+1 == r.EndKey[diffOneIdx] &&
		bytes.Equal(r.StartKey[:diffOneIdx], r.EndKey[:diffOneIdx])
}

// Handle is the ID of a row.
type Handle interface {
	// IsInt returns if the handle type is int64.
	IsInt() bool
	// IntValue returns the int64 value if IsInt is true, it panics if IsInt returns false.
	IntValue() int64
	// Next returns the minimum handle that is greater than this handle.
	Next() Handle
	// Equal returns if the handle equals to another handle, it panics if the types are different.
	Equal(h Handle) bool
	// Compare returns the comparison result of the two handles, it panics if the types are different.
	Compare(h Handle) int
	// Encoded returns the encoded bytes.
	Encoded() []byte
	// Len returns the length of the encoded bytes.
	Len() int
	// NumCols returns the number of columns of the handle,
	NumCols() int
	// EncodedCol returns the encoded column value at the given column index.
	EncodedCol(idx int) []byte
	// String implements the fmt.Stringer interface.
	String() string
}

// IntHandle implement the Handle interface for int64 type handle.
type IntHandle int64

// IsInt implements the Handle interface.
func (ih IntHandle) IsInt() bool {
	return true
}

// IntValue implements the Handle interface.
func (ih IntHandle) IntValue() int64 {
	return int64(ih)
}

// Next implements the Handle interface.
func (ih IntHandle) Next() Handle {
	return IntHandle(int64(ih) + 1)
}

// Equal implements the Handle interface.
func (ih IntHandle) Equal(h Handle) bool {
	return h.IsInt() && int64(ih) == h.IntValue()
}

// Compare implements the Handle interface.
func (ih IntHandle) Compare(h Handle) int {
	if !h.IsInt() {
		panic("IntHandle compares to CommonHandle")
	}
	ihVal := ih.IntValue()
	hVal := h.IntValue()
	if ihVal > hVal {
		return 1
	}
	if ihVal < hVal {
		return -1
	}
	return 0
}

// Encoded implements the Handle interface.
func (ih IntHandle) Encoded() []byte {
	return codec.EncodeInt(nil, int64(ih))
}

// Len implements the Handle interface.
func (ih IntHandle) Len() int {
	return 8
}

// NumCols implements the Handle interface, not supported for IntHandle type.
func (ih IntHandle) NumCols() int {
	panic("not supported in IntHandle")
}

// EncodedCol implements the Handle interface., not supported for IntHandle type.
func (ih IntHandle) EncodedCol(idx int) []byte {
	panic("not supported in IntHandle")
}

// String implements the Handle interface.
func (ih IntHandle) String() string {
	return strconv.FormatInt(int64(ih), 10)
}

// CommonHandle implements the Handle interface for non-int64 type handle.
type CommonHandle struct {
	encoded       []byte
	colEndOffsets []uint16
}

// NewCommonHandle creates a CommonHandle from a encoded bytes which is encoded by code.EncodeKey.
func NewCommonHandle(encoded []byte) (*CommonHandle, error) {
	ch := &CommonHandle{encoded: encoded}
	if len(encoded) < 9 {
		padded := make([]byte, 9)
		copy(padded, encoded)
		ch.encoded = padded
	}
	remain := encoded
	endOff := uint16(0)
	for len(remain) > 0 {
		if remain[0] == 0 {
			// padded data
			break
		}
		var err error
		var col []byte
		col, remain, err = codec.CutOne(remain)
		if err != nil {
			return nil, err
		}
		endOff += uint16(len(col))
		ch.colEndOffsets = append(ch.colEndOffsets, endOff)
	}
	return ch, nil
}

// IsInt implements the Handle interface.
func (ch *CommonHandle) IsInt() bool {
	return false
}

// IntValue implements the Handle interface, not supported for CommonHandle type.
func (ch *CommonHandle) IntValue() int64 {
	panic("not supported in CommonHandle")
}

// Next implements the Handle interface.
func (ch *CommonHandle) Next() Handle {
	return &CommonHandle{
		encoded:       Key(ch.encoded).PrefixNext(),
		colEndOffsets: ch.colEndOffsets,
	}
}

// Equal implements the Handle interface.
func (ch *CommonHandle) Equal(h Handle) bool {
	return !h.IsInt() && bytes.Equal(ch.encoded, h.Encoded())
}

// Compare implements the Handle interface.
func (ch *CommonHandle) Compare(h Handle) int {
	if h.IsInt() {
		panic("CommonHandle compares to IntHandle")
	}
	return bytes.Compare(ch.encoded, h.Encoded())
}

// Encoded implements the Handle interface.
func (ch *CommonHandle) Encoded() []byte {
	return ch.encoded
}

// Len implements the Handle interface.
func (ch *CommonHandle) Len() int {
	return len(ch.encoded)
}

// NumCols implements the Handle interface.
func (ch *CommonHandle) NumCols() int {
	return len(ch.colEndOffsets)
}

// EncodedCol implements the Handle interface.
func (ch *CommonHandle) EncodedCol(idx int) []byte {
	colStartOffset := uint16(0)
	if idx > 0 {
		colStartOffset = ch.colEndOffsets[idx-1]
	}
	return ch.encoded[colStartOffset:ch.colEndOffsets[idx]]
}

// String implements the Handle interface.
func (ch *CommonHandle) String() string {
	strs := make([]string, 0, ch.NumCols())
	for i := 0; i < ch.NumCols(); i++ {
		encodedCol := ch.EncodedCol(i)
		_, d, err := codec.DecodeOne(encodedCol)
		if err != nil {
			return err.Error()
		}
		str, err := d.ToString()
		if err != nil {
			return err.Error()
		}
		strs = append(strs, str)
	}
	return fmt.Sprintf("{%s}", strings.Join(strs, ", "))
}

// HandleMap is the map for Handle.
type HandleMap struct {
	ints map[int64]interface{}
	strs map[string]strHandleVal
}

type strHandleVal struct {
	h   Handle
	val interface{}
}

// NewHandleMap creates a new map for handle.
func NewHandleMap() *HandleMap {
	// Initialize the two maps to avoid checking nil.
	return &HandleMap{
		ints: map[int64]interface{}{},
		strs: map[string]strHandleVal{},
	}
}

// Get gets a value by a Handle.
func (m *HandleMap) Get(h Handle) (v interface{}, ok bool) {
	if h.IsInt() {
		v, ok = m.ints[h.IntValue()]
	} else {
		var strVal strHandleVal
		strVal, ok = m.strs[string(h.Encoded())]
		v = strVal.val
	}
	return
}

// Set sets a value with a Handle.
func (m *HandleMap) Set(h Handle, val interface{}) {
	if h.IsInt() {
		m.ints[h.IntValue()] = val
	} else {
		m.strs[string(h.Encoded())] = strHandleVal{
			h:   h,
			val: val,
		}
	}
}

// Delete deletes a entry from the map.
func (m *HandleMap) Delete(h Handle) {
	if h.IsInt() {
		delete(m.ints, h.IntValue())
	} else {
		delete(m.strs, string(h.Encoded()))
	}
}

// Len returns the length of the map.
func (m *HandleMap) Len() int {
	return len(m.ints) + len(m.strs)
}

// Range iterates the HandleMap with fn, the fn returns true to continue, returns false to stop.
func (m *HandleMap) Range(fn func(h Handle, val interface{}) bool) {
	for h, val := range m.ints {
		if !fn(IntHandle(h), val) {
			return
		}
	}
	for _, strVal := range m.strs {
		if !fn(strVal.h, strVal.val) {
			return
		}
	}
}

// BuildHandleFromDatumRow builds kv.Handle from cols in row.
func BuildHandleFromDatumRow(sctx *stmtctx.StatementContext, row []types.Datum, handleOrdinals []int) (Handle, error) {
	pkDts := make([]types.Datum, 0, len(handleOrdinals))
	for _, ordinal := range handleOrdinals {
		pkDts = append(pkDts, row[ordinal])
	}
	handleBytes, err := codec.EncodeKey(sctx, nil, pkDts...)
	if err != nil {
		return nil, err
	}
	handle, err := NewCommonHandle(handleBytes)
	if err != nil {
		return nil, err
	}
	return handle, nil
}
