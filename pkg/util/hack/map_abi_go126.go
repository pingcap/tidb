// Copyright 2025 PingCAP, Inc.
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

//go:build go1.26 && !go1.27

package hack

import (
	"runtime"
	"strings"
	"unsafe"
)

// Maximum size of a table before it is split at the directory level.
const maxTableCapacity = 1024

// Number of bits in the group.slot count.
const swissMapGroupSlotsBits = 3

// Number of slots in a group.
const swissMapGroupSlots = 1 << swissMapGroupSlotsBits // 8

// $GOROOT/src/internal/runtime/maps/table.go:`type table struct`
type swissMapTable struct {
	// The number of filled slots (i.e. the number of elements in the table).
	used uint16

	// The total number of slots (always 2^N). Equal to
	// `(groups.lengthMask+1)*abi.SwissMapGroupSlots`.
	capacity uint16

	// The number of slots we can still fill without needing to rehash.
	//
	// We rehash when used + tombstones > loadFactor*capacity, including
	// tombstones so the table doesn't overfill with tombstones. This field
	// counts down remaining empty slots before the next rehash.
	growthLeft uint16

	// The number of bits used by directory lookups above this table. Note
	// that this may be less then globalDepth, if the directory has grown
	// but this table has not yet been split.
	localDepth uint8

	// Index of this table in the Map directory. This is the index of the
	// _first_ location in the directory. The table may occur in multiple
	// sequential indicies.
	//
	// index is -1 if the table is stale (no longer installed in the
	// directory).
	index int

	// groups is an array of slot groups. Each group holds abi.SwissMapGroupSlots
	// key/elem slots and their control bytes. A table has a fixed size
	// groups array. The table is replaced (in rehash) when more space is
	// required.
	//
	// TODO(prattmic): keys and elements are interleaved to maximize
	// locality, but it comes at the expense of wasted space for some types
	// (consider uint8 key, uint64 element). Consider placing all keys
	// together in these cases to save space.
	groups groupsReference
}

// groupsReference is a wrapper type describing an array of groups stored at
// data.
type groupsReference struct {
	// data points to an array of groups. See groupReference above for the
	// definition of group.
	data unsafe.Pointer // data *[length]typ.Group

	// lengthMask is the number of groups in data minus one (note that
	// length must be a power of two). This allows computing i%length
	// quickly using bitwise AND.
	lengthMask uint64
}

// $GOROOT/src/internal/runtime/maps/map.go:`type Map struct`
type swissMap struct {
	// The number of filled slots (i.e. the number of elements in all
	// tables). Excludes deleted slots.
	// Must be first (known by the compiler, for len() builtin).
	Used uint64

	// seed is the hash seed, computed as a unique random number per map.
	seed uintptr

	// The directory of tables.
	//
	// Normally dirPtr points to an array of table pointers
	//
	// dirPtr *[dirLen]*table
	//
	// The length (dirLen) of this array is `1 << globalDepth`. Multiple
	// entries may point to the same table. See top-level comment for more
	// details.
	//
	// Small map optimization: if the map always contained
	// abi.SwissMapGroupSlots or fewer entries, it fits entirely in a
	// single group. In that case dirPtr points directly to a single group.
	//
	// dirPtr *group
	//
	// In this case, dirLen is 0. used counts the number of used slots in
	// the group. Note that small maps never have deleted slots (as there
	// is no probe sequence to maintain).
	dirPtr unsafe.Pointer
	dirLen int

	// The number of bits to use in table directory lookups.
	globalDepth uint8

	// The number of bits to shift out of the hash for directory lookups.
	// On 64-bit systems, this is 64 - globalDepth.
	globalShift uint8

	// writing is a flag that is toggled (XOR 1) while the map is being
	// written. Normally it is set to 1 when writing, but if there are
	// multiple concurrent writers, then toggling increases the probability
	// that both sides will detect the race.
	writing uint8

	// tombstonePossible is false if we know that no table in this map
	// contains a tombstone.
	tombstonePossible bool

	// clearSeq is a sequence counter of calls to Clear. It is used to
	// detect map clears during iteration.
	clearSeq uint64
}

func (m *swissMap) directoryAt(i uintptr) *swissMapTable {
	return *(**swissMapTable)(unsafe.Pointer(uintptr(m.dirPtr) + uintptr(sizeofPtr)*i))
}

// Size returns the accurate memory size of the swissMap including all its tables.
func (m *swissMap) Size(groupSize uint64) (sz uint64) {
	sz += swissMapSize
	sz += sizeofPtr * uint64(m.dirLen)
	if m.dirLen == 0 {
		sz += groupSize
		return
	}

	var lastTab *swissMapTable
	for i := range m.dirLen {
		t := m.directoryAt(uintptr(i))
		if t == lastTab {
			continue
		}
		lastTab = t
		sz += swissTableSize
		sz += groupSize * (t.groups.lengthMask + 1)
	}
	return
}

// Cap returns the total capacity of the swissMap.
func (m *swissMap) Cap() uint64 {
	if m.dirLen == 0 {
		return swissMapGroupSlots
	}
	var capacity uint64
	var lastTab *swissMapTable
	for i := range m.dirLen {
		t := m.directoryAt(uintptr(i))
		if t == lastTab {
			continue
		}
		lastTab = t
		capacity += uint64(t.capacity)
	}
	return capacity
}

// Size returns the accurate memory size
func (m *SwissMapWrap) Size() uint64 {
	return m.Data.Size(uint64(m.Type.GroupSize))
}

const (
	swissMapSize   = uint64(unsafe.Sizeof(swissMap{}))
	swissTableSize = uint64(unsafe.Sizeof(swissMapTable{}))
	sizeofPtr      = uint64(unsafe.Sizeof(uintptr(0)))
)

// TODO: use a more accurate size calculation if necessary
func approxSize(groupSize uint64, maxLen uint64) (size uint64) {
	// 204 can fit the `split`/`rehash` behavior of different kinds of swisstable
	const ratio = 204
	return groupSize * maxLen * ratio / 1000
}

type ctrlGroup uint64

type groupReference struct {
	// data points to the group, which is described by typ.Group and has
	// layout:
	//
	// type group struct {
	// 	ctrls ctrlGroup
	// 	slots [abi.SwissMapGroupSlots]slot
	// }
	//
	// type slot struct {
	// 	key  typ.Key
	// 	elem typ.Elem
	// }
	data unsafe.Pointer // data *typ.Group
}

func (g *groupsReference) group(typ *swissMapType, i uint64) groupReference {
	// TODO(prattmic): Do something here about truncation on cast to
	// uintptr on 32-bit systems?
	offset := uintptr(i) * typ.GroupSize

	return groupReference{
		data: unsafe.Pointer(uintptr(g.data) + offset),
	}
}

// $GOROOT/src/internal/abi/type.go:`type Type struct`
type abiType struct {
	Size       uintptr
	PtrBytes   uintptr // number of (prefix) bytes in the type that can contain pointers
	Hash       uint32  // hash of type; avoids computation in hash tables
	TFlag      uint8   // extra type information flags
	Align      uint8   // alignment of variable with this type
	FieldAlign uint8   // alignment of struct field with this type
	Kind       uint8   // enumeration for C
	// function for comparing objects of this type
	// (ptr to object A, ptr to object B) -> ==?
	Equal func(unsafe.Pointer, unsafe.Pointer) bool
	// GCData stores the GC type data for the garbage collector.
	// Normally, GCData points to a bitmask that describes the
	// ptr/nonptr fields of the type. The bitmask will have at
	// least PtrBytes/ptrSize bits.
	// If the TFlagGCMaskOnDemand bit is set, GCData is instead a
	// **byte and the pointer to the bitmask is one dereference away.
	// The runtime will build the bitmask if needed.
	// (See runtime/type.go:getGCMask.)
	// Note: multiple types may have the same value of GCData,
	// including when TFlagGCMaskOnDemand is set. The types will, of course,
	// have the same pointer layout (but not necessarily the same size).
	GCData    *byte
	Str       int32 // string form
	PtrToThis int32 // type for pointer to this type, may be zero
}

// $GOROOT/src/internal/abi/map_swiss.go:`type SwissMapType struct`
type swissMapType struct {
	abiType
	Key   *abiType
	Elem  *abiType
	Group *abiType // internal type representing a slot group
	// function for hashing keys (ptr to key, seed) -> hash
	Hasher    func(unsafe.Pointer, uintptr) uintptr
	GroupSize uintptr // == Group.Size_
	SlotSize  uintptr // size of key/elem slot
	ElemOff   uintptr // offset of elem in key/elem slot; aka key size; elem size: SlotSize - ElemOff;
	Flags     uint32
}

// SwissMapWrap is a wrapper of map to access its internal structure.
type SwissMapWrap struct {
	Type *swissMapType
	Data *swissMap
}

// ToSwissMap converts a map to SwissMapWrap.
func ToSwissMap[K comparable, V any](m map[K]V) (sm SwissMapWrap) {
	ref := any(m)
	sm = *(*SwissMapWrap)(unsafe.Pointer(&ref))
	return
}

const (
	ctrlGroupsSize   = unsafe.Sizeof(ctrlGroup(0))
	groupSlotsOffset = ctrlGroupsSize
)

func (g *groupReference) cap(typ *swissMapType) uint64 {
	_ = g
	return groupCap(uint64(typ.GroupSize), uint64(typ.SlotSize))
}

func groupCap(groupSize, slotSize uint64) uint64 {
	return (groupSize - uint64(groupSlotsOffset)) / slotSize
}

// key returns a pointer to the key at index i.
func (g *groupReference) key(typ *swissMapType, i uintptr) unsafe.Pointer {
	offset := groupSlotsOffset + i*typ.SlotSize
	return unsafe.Pointer(uintptr(g.data) + offset)
}

// elem returns a pointer to the element at index i.
func (g *groupReference) elem(typ *swissMapType, i uintptr) unsafe.Pointer {
	offset := groupSlotsOffset + i*typ.SlotSize + typ.ElemOff
	return unsafe.Pointer(uintptr(g.data) + offset)
}

// MemAwareMap is a map with memory usage tracking.
type MemAwareMap[K comparable, V any] struct {
	M              map[K]V
	groupSize      uint64
	nextCheckpoint uint64 // every `maxTableCapacity` increase in Used
	Bytes          uint64
}

// MockSeedForTest sets the seed of the swissMap inside MemAwareMap
func (m *MemAwareMap[K, V]) MockSeedForTest(seed uint64) (oriSeed uint64) {
	return m.unwrap().MockSeedForTest(seed)
}

// MockSeedForTest sets the seed of the swissMap
func (m *swissMap) MockSeedForTest(seed uint64) (oriSeed uint64) {
	if m.Used != 0 {
		panic("MockSeedForTest can only be called on empty map")
	}
	oriSeed = uint64(m.seed)
	m.seed = uintptr(seed)
	return
}

// Count returns the number of elements in the map.
func (m *MemAwareMap[K, V]) Count() int {
	return len(m.M)
}

// Empty returns true if the map is empty.
func (m *MemAwareMap[K, V]) Empty() bool {
	return len(m.M) == 0
}

// Exist returns true if the key exists in the map.
func (m *MemAwareMap[K, V]) Exist(val K) bool {
	_, ok := m.M[val]
	return ok
}

func (m *MemAwareMap[K, V]) unwrap() *swissMap {
	return *(**swissMap)(unsafe.Pointer(&m.M))
}

// Set sets the value for the key in the map and returns the memory delta.
func (m *MemAwareMap[K, V]) Set(key K, value V) (deltaBytes int64) {
	sm := m.unwrap()
	m.M[key] = value
	if sm.Used >= m.nextCheckpoint {
		newBytes := max(m.Bytes, approxSize(m.groupSize, sm.Used))
		deltaBytes = int64(newBytes) - int64(m.Bytes)
		m.Bytes = newBytes
		m.nextCheckpoint = min(sm.Used, maxTableCapacity) + sm.Used
	}
	return
}

// SetExt sets the value for the key in the map and returns the memory delta and whether it's an insert.
func (m *MemAwareMap[K, V]) SetExt(key K, value V) (deltaBytes int64, insert bool) {
	sm := m.unwrap()
	oriUsed := sm.Used
	deltaBytes = m.Set(key, value)
	insert = oriUsed != sm.Used
	return
}

// Init initializes the MemAwareMap with the given map and returns the initial memory size.
// The input map should NOT be nil.
func (m *MemAwareMap[K, V]) Init(v map[K]V) int64 {
	if v == nil {
		panic("MemAwareMap.Init: input map should NOT be nil")
	}
	m.M = v
	sm := m.unwrap()

	m.groupSize = uint64(ToSwissMap(m.M).Type.GroupSize)
	m.Bytes = sm.Size(m.groupSize)
	if sm.Used <= swissMapGroupSlots {
		m.nextCheckpoint = swissMapGroupSlots * 2
	} else {
		m.nextCheckpoint = min(sm.Used, maxTableCapacity) + sm.Used
	}
	return int64(m.Bytes)
}

// NewMemAwareMap creates a new MemAwareMap with the given initial capacity.
func NewMemAwareMap[K comparable, V any](capacity int) *MemAwareMap[K, V] {
	m := new(MemAwareMap[K, V])
	m.Init(make(map[K]V, capacity))
	return m
}

// RealBytes returns the real memory size of the map.
// Compute the real size is expensive, so do not call it frequently.
// Make sure the `seed` is same when testing the memory size.
func (m *MemAwareMap[K, V]) RealBytes() uint64 {
	return m.unwrap().Size(m.groupSize)
}

func checkMapABI() {
	if !strings.Contains(runtime.Version(), `go1.26`) {
		panic("The hack package only supports go1.26, please confirm the correctness of the ABI before upgrading")
	}
}

// Get the value of the key.
func (m *MemAwareMap[K, V]) Get(k K) (v V, ok bool) {
	v, ok = m.M[k]
	return
}

// Len returns the number of elements in the map.
func (m *MemAwareMap[K, V]) Len() int {
	return len(m.M)
}
