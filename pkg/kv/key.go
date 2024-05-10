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

package kv

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/size"
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
//	rowkey1
//	rowkey1_column1
//	rowkey1_column2
//	rowKey2
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
		buf = append(buf, 0) // nozero
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
// Hack: make the layout exactly the same with github.com/pingcap/kvproto/pkg/coprocessor.KeyRange
// So we can avoid allocation of converting kv.KeyRange to coprocessor.KeyRange
// Not defined as "type KeyRange = coprocessor.KeyRange" because their field name are different.
// kv.KeyRange use StartKey,EndKey while coprocessor.KeyRange use Start,End
type KeyRange struct {
	StartKey Key
	EndKey   Key

	XXXNoUnkeyedLiteral struct{}
	XXXunrecognized     []byte
	XXXsizecache        int32
}

// KeyRangeSliceMemUsage return the memory usage of []KeyRange
func KeyRangeSliceMemUsage(k []KeyRange) int64 {
	const sizeofKeyRange = int64(unsafe.Sizeof(*(*KeyRange)(nil)))

	res := sizeofKeyRange * int64(cap(k))
	for _, m := range k {
		res += int64(cap(m.StartKey)) + int64(cap(m.EndKey)) + int64(cap(m.XXXunrecognized))
	}

	return res
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

// Entry is the entry for key and value
type Entry struct {
	Key   Key
	Value []byte
}

// Handle is the ID of a row.
type Handle interface {
	// IsInt returns if the handle type is int64.
	IsInt() bool
	// IntValue returns the int64 value if IsInt is true, it panics if IsInt returns false.
	IntValue() int64
	// Next returns the minimum handle that is greater than this handle.
	// The returned handle is not guaranteed to be able to decode.
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
	// Data returns the data of all columns of a handle.
	Data() ([]types.Datum, error)
	// String implements the fmt.Stringer interface.
	String() string
	// MemUsage returns the memory usage of a handle.
	MemUsage() uint64
	// ExtraMemSize returns the memory usage of objects that are pointed to by the Handle.
	ExtraMemSize() uint64
}

var _ Handle = IntHandle(0)
var _ Handle = &CommonHandle{}
var _ Handle = PartitionHandle{}

// IntHandle implement the Handle interface for int64 type handle.
type IntHandle int64

// IsInt implements the Handle interface.
func (IntHandle) IsInt() bool {
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
func (IntHandle) Len() int {
	return 8
}

// NumCols implements the Handle interface, not supported for IntHandle type.
func (IntHandle) NumCols() int {
	panic("not supported in IntHandle")
}

// EncodedCol implements the Handle interface., not supported for IntHandle type.
func (IntHandle) EncodedCol(_ int) []byte {
	panic("not supported in IntHandle")
}

// Data implements the Handle interface.
func (ih IntHandle) Data() ([]types.Datum, error) {
	return []types.Datum{types.NewIntDatum(int64(ih))}, nil
}

// String implements the Handle interface.
func (ih IntHandle) String() string {
	return strconv.FormatInt(int64(ih), 10)
}

// MemUsage implements the Handle interface.
func (IntHandle) MemUsage() uint64 {
	return 8
}

// ExtraMemSize implements the Handle interface.
func (IntHandle) ExtraMemSize() uint64 {
	return 0
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
func (*CommonHandle) IsInt() bool {
	return false
}

// IntValue implements the Handle interface, not supported for CommonHandle type.
func (*CommonHandle) IntValue() int64 {
	panic("not supported in CommonHandle")
}

// Next implements the Handle interface.
// Note that the returned encoded field is not guaranteed to be able to decode.
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

// Data implements the Handle interface.
func (ch *CommonHandle) Data() ([]types.Datum, error) {
	data := make([]types.Datum, 0, ch.NumCols())
	for i := 0; i < ch.NumCols(); i++ {
		encodedCol := ch.EncodedCol(i)
		_, d, err := codec.DecodeOne(encodedCol)
		if err != nil {
			return nil, err
		}
		data = append(data, d)
	}
	return data, nil
}

// String implements the Handle interface.
func (ch *CommonHandle) String() string {
	data, err := ch.Data()
	if err != nil {
		return err.Error()
	}
	strs := make([]string, 0, ch.NumCols())
	for _, datum := range data {
		str, err := datum.ToString()
		if err != nil {
			return err.Error()
		}
		strs = append(strs, str)
	}
	return fmt.Sprintf("{%s}", strings.Join(strs, ", "))
}

// MemUsage implements the Handle interface.
func (ch *CommonHandle) MemUsage() uint64 {
	// 48 is used by the 2 slice fields.
	return 48 + ch.ExtraMemSize()
}

// ExtraMemSize implements the Handle interface.
func (ch *CommonHandle) ExtraMemSize() uint64 {
	// colEndOffsets is a slice of uint16.
	return uint64(cap(ch.encoded) + cap(ch.colEndOffsets)*2)
}

// HandleMap is the map for Handle.
type HandleMap struct {
	ints map[int64]any
	strs map[string]strHandleVal

	// Use two two-dimensional map to fit partitionHandle.
	// The first int64 is for partitionID.
	partitionInts map[int64]map[int64]any
	partitionStrs map[int64]map[string]strHandleVal
}

type strHandleVal struct {
	h   Handle
	val any
}

// SizeofHandleMap presents the memory size of struct HandleMap
const SizeofHandleMap = int64(unsafe.Sizeof(*(*HandleMap)(nil)))

// SizeofStrHandleVal presents the memory size of struct strHandleVal
const SizeofStrHandleVal = int64(unsafe.Sizeof(*(*strHandleVal)(nil)))

// NewHandleMap creates a new map for handle.
func NewHandleMap() *HandleMap {
	return &HandleMap{
		ints: map[int64]any{},
		strs: map[string]strHandleVal{},

		partitionInts: map[int64]map[int64]any{},
		partitionStrs: map[int64]map[string]strHandleVal{},
	}
}

// Get gets a value by a Handle.
func (m *HandleMap) Get(h Handle) (v any, ok bool) {
	ints, strs := m.ints, m.strs
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if (h.IsInt() && m.partitionInts[idx] == nil) ||
			(!h.IsInt() && m.partitionStrs[idx] == nil) {
			return nil, false
		}
		ints, strs = m.partitionInts[idx], m.partitionStrs[idx]
	}
	if h.IsInt() {
		v, ok = ints[h.IntValue()]
	} else {
		var strVal strHandleVal
		strVal, ok = strs[string(h.Encoded())]
		v = strVal.val
	}
	return
}

func calcStrsMemUsage(strs map[string]strHandleVal) int64 {
	res := int64(0)
	for key := range strs {
		res += size.SizeOfString + int64(len(key)) + SizeofStrHandleVal
	}
	return res
}

func calcIntsMemUsage(ints map[int64]any) int64 {
	return int64(len(ints)) * (size.SizeOfInt64 + size.SizeOfInterface)
}

// MemUsage gets the memory usage.
func (m *HandleMap) MemUsage() int64 {
	res := SizeofHandleMap
	res += int64(len(m.partitionInts)) * (size.SizeOfInt64 + size.SizeOfMap)
	for _, v := range m.partitionInts {
		res += calcIntsMemUsage(v)
	}
	res += int64(len(m.partitionStrs)) * (size.SizeOfInt64 + size.SizeOfMap)
	for _, v := range m.partitionStrs {
		res += calcStrsMemUsage(v)
	}
	res += calcIntsMemUsage(m.ints)
	res += calcStrsMemUsage(m.strs)
	return res
}

// Set sets a value with a Handle.
func (m *HandleMap) Set(h Handle, val any) {
	ints, strs := m.ints, m.strs
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if h.IsInt() {
			if m.partitionInts[idx] == nil {
				m.partitionInts[idx] = make(map[int64]any)
			}
			ints = m.partitionInts[idx]
		} else {
			if m.partitionStrs[idx] == nil {
				m.partitionStrs[idx] = make(map[string]strHandleVal)
			}
			strs = m.partitionStrs[idx]
		}
	}
	if h.IsInt() {
		ints[h.IntValue()] = val
	} else {
		strs[string(h.Encoded())] = strHandleVal{
			h:   h,
			val: val,
		}
	}
}

// Delete deletes a entry from the map.
func (m *HandleMap) Delete(h Handle) {
	ints, strs := m.ints, m.strs
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if (h.IsInt() && m.partitionInts[idx] == nil) ||
			(!h.IsInt() && m.partitionStrs[idx] == nil) {
			return
		}
		ints, strs = m.partitionInts[idx], m.partitionStrs[idx]
	}
	if h.IsInt() {
		delete(ints, h.IntValue())
	} else {
		delete(strs, string(h.Encoded()))
	}
}

// Len returns the length of the map.
func (m *HandleMap) Len() int {
	l := len(m.ints) + len(m.strs)
	for _, v := range m.partitionInts {
		l += len(v)
	}
	for _, v := range m.partitionStrs {
		l += len(v)
	}
	return l
}

// Range iterates the HandleMap with fn, the fn returns true to continue, returns false to stop.
func (m *HandleMap) Range(fn func(h Handle, val any) bool) {
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
	for pid, v := range m.partitionInts {
		for h, val := range v {
			if !fn(NewPartitionHandle(pid, IntHandle(h)), val) {
				return
			}
		}
	}
	for _, v := range m.partitionStrs {
		for _, strVal := range v {
			if !fn(strVal.h, strVal.val) {
				return
			}
		}
	}
}

// MemAwareHandleMap is similar to HandleMap, but it's aware of its memory usage and doesn't support delete.
// It only tracks the actual sizes. Objects that are pointed to by the key or value are not tracked.
// Those should be tracked by the caller.
type MemAwareHandleMap[V any] struct {
	ints set.MemAwareMap[int64, V]
	strs set.MemAwareMap[string, strHandleValue[V]]

	partitionInts map[int64]set.MemAwareMap[int64, V]
	partitionStrs map[int64]set.MemAwareMap[string, strHandleValue[V]]
}

type strHandleValue[V any] struct {
	h   Handle
	val V
}

// NewMemAwareHandleMap creates a new map for handle.
func NewMemAwareHandleMap[V any]() *MemAwareHandleMap[V] {
	return &MemAwareHandleMap[V]{
		ints: set.NewMemAwareMap[int64, V](),
		strs: set.NewMemAwareMap[string, strHandleValue[V]](),

		partitionInts: map[int64]set.MemAwareMap[int64, V]{},
		partitionStrs: map[int64]set.MemAwareMap[string, strHandleValue[V]]{},
	}
}

// Get gets a value by a Handle.
func (m *MemAwareHandleMap[V]) Get(h Handle) (v V, ok bool) {
	ints, strs := m.ints, m.strs
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if h.IsInt() {
			if m.partitionInts[idx].M == nil {
				return v, false
			}
			ints = m.partitionInts[idx]
		} else {
			if m.partitionStrs[idx].M == nil {
				return v, false
			}
			strs = m.partitionStrs[idx]
		}
	}
	if h.IsInt() {
		v, ok = ints.Get(h.IntValue())
	} else {
		var strVal strHandleValue[V]
		strVal, ok = strs.Get(string(h.Encoded()))
		v = strVal.val
	}
	return
}

// Set sets a value with a Handle.
func (m *MemAwareHandleMap[V]) Set(h Handle, val V) int64 {
	ints, strs := m.ints, m.strs
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if h.IsInt() {
			if m.partitionInts[idx].M == nil {
				m.partitionInts[idx] = set.NewMemAwareMap[int64, V]()
			}
			ints = m.partitionInts[idx]
		} else {
			if m.partitionStrs[idx].M == nil {
				m.partitionStrs[idx] = set.NewMemAwareMap[string, strHandleValue[V]]()
			}
			strs = m.partitionStrs[idx]
		}
	}
	if h.IsInt() {
		return ints.Set(h.IntValue(), val)
	}
	return strs.Set(string(h.Encoded()), strHandleValue[V]{
		h:   h,
		val: val,
	})
}

// Range iterates the MemAwareHandleMap with fn, the fn returns true to continue, returns false to stop.
func (m *MemAwareHandleMap[V]) Range(fn func(h Handle, val V) bool) {
	for h, val := range m.ints.M {
		if !fn(IntHandle(h), val) {
			return
		}
	}
	for _, strVal := range m.strs.M {
		if !fn(strVal.h, strVal.val) {
			return
		}
	}
	for _, v := range m.partitionInts {
		for h, val := range v.M {
			if !fn(IntHandle(h), val) {
				return
			}
		}
	}
	for _, v := range m.partitionStrs {
		for _, strVal := range v.M {
			if !fn(strVal.h, strVal.val) {
				return
			}
		}
	}
}

// PartitionHandle combines a handle and a PartitionID, used to location a row in partitioned table.
type PartitionHandle struct {
	Handle
	PartitionID int64
}

// NewPartitionHandle creates a PartitionHandle from a normal handle and a pid.
func NewPartitionHandle(pid int64, h Handle) PartitionHandle {
	return PartitionHandle{
		Handle:      h,
		PartitionID: pid,
	}
}

// Equal implements the Handle interface.
func (ph PartitionHandle) Equal(h Handle) bool {
	if ph2, ok := h.(PartitionHandle); ok {
		return ph.PartitionID == ph2.PartitionID && ph.Handle.Equal(ph2.Handle)
	}
	return false
}

// Compare implements the Handle interface.
func (ph PartitionHandle) Compare(h Handle) int {
	if ph2, ok := h.(PartitionHandle); ok {
		if ph.PartitionID < ph2.PartitionID {
			return -1
		}
		if ph.PartitionID > ph2.PartitionID {
			return 1
		}
		return ph.Handle.Compare(ph2.Handle)
	}
	panic("PartitonHandle compares to non-parition Handle")
}

// MemUsage implements the Handle interface.
func (ph PartitionHandle) MemUsage() uint64 {
	return ph.Handle.MemUsage() + 8
}

// ExtraMemSize implements the Handle interface.
func (ph PartitionHandle) ExtraMemSize() uint64 {
	return ph.Handle.ExtraMemSize()
}
