// Copyright 2023 PingCAP, Inc.
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

import "github.com/pingcap/tidb/pkg/util/chunk"

type rowWithPartition struct {
	row         chunk.Row
	partitionID int
}

type multiWayMerge struct {
	lessRowFunction     func(rowI chunk.Row, rowJ chunk.Row) bool
	compressRowFunction func(rowI chunk.Row, rowJ chunk.Row) int
	elements            []rowWithPartition
}

func (h *multiWayMerge) Less(i, j int) bool {
	rowI := h.elements[i]
	rowJ := h.elements[j]
	return h.lessRowFunction(rowI.row, rowJ.row)
}

func (h *multiWayMerge) Len() int {
	return len(h.elements)
}

func (*multiWayMerge) Push(interface{}) {
	// Should never be called.
}

func (h *multiWayMerge) Pop() interface{} {
	h.elements = h.elements[:len(h.elements)-1]
	return nil
}

func (h *multiWayMerge) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}
