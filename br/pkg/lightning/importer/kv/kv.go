// Copyright 2022 PingCAP, Inc.
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
	"container/heap"
	"sort"
)

// KeyRange represents a range of keys.
// StartKey is inclusive, while EndKey is exclusive.
// If EndKey is empty, it means the range is unbounded.
type KeyRange struct {
	StartKey []byte `json:"startKey"`
	EndKey   []byte `json:"endKey"`
}

func (kr KeyRange) Less(other KeyRange) bool {
	cmp := bytes.Compare(kr.StartKey, other.StartKey)
	if cmp != 0 {
		return cmp < 0
	}
	return len(kr.EndKey) > 0 && (len(other.EndKey) == 0 || bytes.Compare(kr.EndKey, other.EndKey) < 0)
}

// Overlaps returns true if the two ranges overlap.
func (kr KeyRange) Overlaps(other KeyRange) bool {
	if len(kr.EndKey) > 0 && bytes.Compare(kr.EndKey, other.StartKey) <= 0 {
		return false
	}
	if len(other.EndKey) > 0 && bytes.Compare(other.EndKey, kr.StartKey) <= 0 {
		return false
	}
	return true
}

// OverlapsOrTouches returns true if the two ranges overlap or touch.
func (kr KeyRange) OverlapsOrTouches(other KeyRange) bool {
	if len(kr.EndKey) > 0 && bytes.Compare(kr.EndKey, other.StartKey) < 0 {
		return false
	}
	if len(other.EndKey) > 0 && bytes.Compare(other.EndKey, kr.StartKey) < 0 {
		return false
	}
	return true
}

// Union returns the smallest range that contains both ranges.
func (kr KeyRange) Union(other KeyRange) KeyRange {
	startKey := kr.StartKey
	if bytes.Compare(startKey, other.StartKey) > 0 {
		startKey = other.StartKey
	}
	endKey := kr.EndKey
	if len(other.EndKey) == 0 || (len(endKey) > 0 && bytes.Compare(endKey, other.EndKey) < 0) {
		endKey = other.EndKey
	}
	return KeyRange{StartKey: startKey, EndKey: endKey}
}

// Intersect returns the intersection of the two ranges.
func (kr KeyRange) Intersect(other KeyRange) KeyRange {
	startKey := kr.StartKey
	if bytes.Compare(startKey, other.StartKey) < 0 {
		startKey = other.StartKey
	}
	endKey := kr.EndKey
	if len(other.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(endKey, other.EndKey) > 0) {
		endKey = other.EndKey
	}
	return KeyRange{StartKey: startKey, EndKey: endKey}
}

// IsEmpty returns true if the range is empty.
func (kr KeyRange) IsEmpty() bool {
	return len(kr.EndKey) > 0 && bytes.Compare(kr.StartKey, kr.EndKey) >= 0
}

// KeyRanges represents a set of key ranges that are not overlapping.
type KeyRanges struct {
	ranges    []KeyRange
	compacted bool
}

func (krs *KeyRanges) Add(kr KeyRange) {
	krs.ranges = append(krs.ranges, kr)
	krs.compacted = false
}

func (krs *KeyRanges) Ranges() []KeyRange {
	krs.compact()
	return append([]KeyRange(nil), krs.ranges...)
}

// AbsentFromSelf returns the key ranges that are present in other but not in self.
func (krs *KeyRanges) AbsentFromSelf(other KeyRange) []KeyRange {
	krs.compact()

	firstIdx := sort.Search(len(krs.ranges), func(i int) bool {
		return bytes.Compare(krs.ranges[i].EndKey, other.StartKey) > 0
	})
	if firstIdx == len(krs.ranges) {
		return []KeyRange{other}
	}

	var result []KeyRange
	for i := firstIdx; i < len(krs.ranges) && !other.IsEmpty(); i++ {
		kr := krs.ranges[i]
		if bytes.Compare(kr.StartKey, other.StartKey) > 0 {
			result = append(result, KeyRange{StartKey: other.StartKey, EndKey: kr.StartKey})
		}
		if bytes.Compare(kr.EndKey, other.StartKey) > 0 {
			other.StartKey = kr.EndKey
		}
	}

	if !other.IsEmpty() {
		result = append(result, other)
	}
	return result
}

func (krs *KeyRanges) compact() {
	if krs.compacted {
		return
	}
	krs.compacted = true
	if len(krs.ranges) == 0 {
		return
	}
	sort.Slice(krs.ranges, func(i, j int) bool {
		return krs.ranges[i].Less(krs.ranges[j])
	})
	// Merge overlapping ranges.
	merged := make([]KeyRange, 0, len(krs.ranges))
	for i := 0; i < len(krs.ranges); {
		j := i + 1
		for j < len(krs.ranges) && krs.ranges[i].OverlapsOrTouches(krs.ranges[j]) {
			krs.ranges[i] = krs.ranges[i].Union(krs.ranges[j])
			j++
		}
		merged = append(merged, krs.ranges[i])
		i = j
	}
	krs.ranges = merged
}

// Reader is the interface for reading key-value pairs.
type Reader interface {
	// Read reads the next key-value pair. If there is no more pair, both key and value will be nil.
	Read() (key, val []byte, err error)
	// Close closes the reader.
	Close() error
}

// Writer is an interface for writing key-value pairs.
type Writer interface {
	Write(key, val []byte) error
	Close() error
}

// Copy copies all key-value pairs from reader to writer.
func Copy(w Writer, r Reader) error {
	for {
		key, val, err := r.Read()
		if err != nil {
			return err
		}
		if key == nil {
			return nil
		}
		if err := w.Write(key, val); err != nil {
			return err
		}
	}
}

type mergedReader struct {
	h mergedReaderHeap
}

// MergeReaders merges multiple ordered readers into one ordered reader.
// The input readers must be ordered by key. Otherwise, the result is undefined.
func MergeReaders(readers ...Reader) (Reader, error) {
	h := make(mergedReaderHeap, 0, len(readers))
	for _, reader := range readers {
		key, val, err := reader.Read()
		if err != nil {
			return nil, err
		}
		if key == nil {
			if err := reader.Close(); err != nil {
				return nil, err
			}
			continue
		}
		h = append(h, mergedReaderItem{reader: reader, firstKey: key, firstVal: val, firstValid: true})
	}
	heap.Init(&h)
	return &mergedReader{h: h}, nil
}

func (mr *mergedReader) Read() (key, val []byte, err error) {
	for mr.h.Len() > 0 {
		if mr.h[0].firstValid {
			mr.h[0].firstValid = false
			return mr.h[0].firstKey, mr.h[0].firstVal, nil
		}
		key, val, err := mr.h[0].reader.Read()
		if err != nil {
			return nil, nil, err
		}
		if key == nil {
			if err := mr.h[0].reader.Close(); err != nil {
				return nil, nil, err
			}
			heap.Pop(&mr.h)
		} else {
			mr.h[0].firstKey = key
			mr.h[0].firstVal = val
			mr.h[0].firstValid = true
			heap.Fix(&mr.h, 0)
		}
	}
	return nil, nil, nil
}

func (mr *mergedReader) Close() error {
	var firstErr error
	for _, item := range mr.h {
		if err := item.reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type mergedReaderItem struct {
	firstKey   []byte
	firstVal   []byte
	firstValid bool
	reader     Reader
}

type mergedReaderHeap []mergedReaderItem

func (h mergedReaderHeap) Len() int {
	return len(h)
}

func (h mergedReaderHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].firstKey, h[j].firstKey) < 0
}

func (h mergedReaderHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergedReaderHeap) Push(x interface{}) {
	*h = append(*h, x.(mergedReaderItem))
}

func (h *mergedReaderHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
