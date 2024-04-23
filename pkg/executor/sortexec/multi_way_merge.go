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

package sortexec

import (
	"container/heap"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type multiWayMergeSource interface {
	init(*multiWayMergeImpl) error
	next(int) (chunk.Row, error)
	getPartitionNum() int
}

type memorySource struct {
	sortedRowsIters []*chunk.Iterator4Slice
}

func (m *memorySource) init(multiWayMerge *multiWayMergeImpl) error {
	for i := range m.sortedRowsIters {
		row := m.sortedRowsIters[i].Begin()
		if row.IsEmpty() {
			continue
		}
		multiWayMerge.elements = append(multiWayMerge.elements, rowWithPartition{row: row, partitionID: i})
	}
	heap.Init(multiWayMerge)
	return nil
}

func (m *memorySource) next(partitionID int) (chunk.Row, error) {
	return m.sortedRowsIters[partitionID].Next(), nil
}

func (m *memorySource) getPartitionNum() int {
	return len(m.sortedRowsIters)
}

type diskSource struct {
	sortedRowsInDisk []*chunk.DataInDiskByChunks
	cursors          []*dataCursor
}

func (d *diskSource) init(multiWayMerge *multiWayMergeImpl) error {
	inDiskNum := len(d.sortedRowsInDisk)
	d.cursors = make([]*dataCursor, 0, inDiskNum)
	for i := 0; i < inDiskNum; i++ {
		chk, err := d.sortedRowsInDisk[i].GetChunk(0)
		if err != nil {
			return err
		}
		cursor := NewDataCursor()
		cursor.setChunk(chk, 0)
		d.cursors = append(d.cursors, cursor)
		row := cursor.begin()
		if row.IsEmpty() {
			continue
		}
		multiWayMerge.elements = append(multiWayMerge.elements, rowWithPartition{row: row, partitionID: i})
	}
	heap.Init(multiWayMerge)
	return nil
}

func (d *diskSource) next(partitionID int) (chunk.Row, error) {
	newRow := d.cursors[partitionID].next()
	if newRow.IsEmpty() {
		// Try to fetch more data from the disk
		success, err := reloadCursor(d.cursors[partitionID], d.sortedRowsInDisk[partitionID])
		if err != nil {
			return chunk.Row{}, err
		}

		if !success {
			return chunk.Row{}, nil
		}

		// Get new row
		newRow = d.cursors[partitionID].begin()
		if newRow.IsEmpty() {
			return chunk.Row{}, errors.New("Get an empty row")
		}
	}
	return newRow, nil
}

func (d *diskSource) getPartitionNum() int {
	return len(d.sortedRowsInDisk)
}

type sortPartitionSource struct {
	sortPartitions []*sortPartition
}

func (s *sortPartitionSource) init(multiWayMerge *multiWayMergeImpl) error {
	partitionNum := s.getPartitionNum()
	for i := 0; i < partitionNum; i++ {
		row, err := s.sortPartitions[i].getNextSortedRow()
		if err != nil {
			return err
		}

		multiWayMerge.elements = append(multiWayMerge.elements, rowWithPartition{row: row, partitionID: i})
	}
	heap.Init(multiWayMerge)
	return nil
}

func (s *sortPartitionSource) next(partitionID int) (chunk.Row, error) {
	return s.sortPartitions[partitionID].getNextSortedRow()
}

func (s *sortPartitionSource) getPartitionNum() int {
	return len(s.sortPartitions)
}

type multiWayMergeImpl struct {
	lessRowFunction func(rowI chunk.Row, rowJ chunk.Row) int
	elements        []rowWithPartition
}

func (h *multiWayMergeImpl) Less(i, j int) bool {
	rowI := h.elements[i].row
	rowJ := h.elements[j].row
	ret := h.lessRowFunction(rowI, rowJ)
	return ret < 0
}

func (h *multiWayMergeImpl) Len() int {
	return len(h.elements)
}

func (*multiWayMergeImpl) Push(any) {
	// Should never be called.
}

func (h *multiWayMergeImpl) Pop() any {
	h.elements = h.elements[:len(h.elements)-1]
	return nil
}

func (h *multiWayMergeImpl) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}

type multiWayMerger struct {
	source        multiWayMergeSource
	multiWayMerge *multiWayMergeImpl
}

func newMultiWayMerger(
	source multiWayMergeSource,
	lessRowFunction func(rowI chunk.Row, rowJ chunk.Row) int,
) *multiWayMerger {
	return &multiWayMerger{
		source: source,
		multiWayMerge: &multiWayMergeImpl{
			lessRowFunction: lessRowFunction,
			elements:        make([]rowWithPartition, 0, source.getPartitionNum()),
		},
	}
}

func (m *multiWayMerger) init() error {
	return m.source.init(m.multiWayMerge)
}

func (m *multiWayMerger) next() (chunk.Row, error) {
	if m.multiWayMerge.Len() > 0 {
		elem := m.multiWayMerge.elements[0]
		newRow, err := m.source.next(elem.partitionID)
		if err != nil {
			return chunk.Row{}, err
		}

		if newRow.IsEmpty() {
			heap.Remove(m.multiWayMerge, 0)
			return elem.row, nil
		}
		m.multiWayMerge.elements[0].row = newRow
		heap.Fix(m.multiWayMerge, 0)
		return elem.row, nil
	}
	return chunk.Row{}, nil
}
