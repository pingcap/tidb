package bloom

import (
	"fmt"
)

// Filter a simple abstraction of bloom filter
type Filter struct {
	BitSet   []uint64
	length   uint64
	unitSize uint64

	numbers []uint64
}

// NewFilter returns a filter with a given size
func NewFilter(length int) *Filter {
	if length <= 1000 {
		length = 1000
	}
	bitset := make([]uint64, length)
	bits := uint64(64)
	return &Filter{
		BitSet:   bitset,
		length:   bits * uint64(length),
		unitSize: bits,
	}
}

// NewFilterWithoutLength returns a filter without length.
// Must call Init(length).
func NewFilterWithoutLength() *Filter {
	return &Filter{}
}

// Init reset the length
func (bf *Filter) Init(length int) {
	if length <= 1000 {
		length = 1000
	}
	bitset := make([]uint64, length)
	bits := uint64(64)
	bf.BitSet = bitset
	bf.length = bits * uint64(length)
	bf.unitSize = bits
}

// NewFilterBySlice create a bloom filter by the given slice
func NewFilterBySlice(bs []uint64) (*Filter, error) {
	if len(bs) == 0 {
		return nil, fmt.Errorf("len(bs) == 0")
	}

	bits := uint64(64)
	return &Filter{
		BitSet:   bs,
		length:   bits * uint64(len(bs)),
		unitSize: bits,
	}, nil
}

// InsertU64 a key into the filter
func (bf *Filter) InsertU64(key uint64) {
	hash := key % uint64(bf.length)
	idx := hash / bf.unitSize
	shift := hash % bf.unitSize
	bf.BitSet[idx] |= 1 << shift
}

// ProbeU64 check whether the given key is in the filter
func (bf *Filter) ProbeU64(key uint64) bool {
	hash := key % uint64(bf.length)
	idx := hash / bf.unitSize
	shift := hash % bf.unitSize
	return bf.BitSet[idx]&(1<<shift) != 0
}

// LazyInsertU64 saves the hash value
func (bf *Filter) LazyInsertU64(key uint64) {
	bf.numbers = append(bf.numbers, key)
}

// Build builds the bloom filter
func (bf *Filter) Build() {
	bf.Init(len(bf.numbers) * 8 / 64)
	for _, val := range bf.numbers {
		bf.InsertU64(val)
	}
}
