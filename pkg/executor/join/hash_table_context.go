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
	"github.com/pingcap/tidb/pkg/util/memory"
)

type hashTableContext struct {
	// rowTables is used during split partition stage, each buildWorker has
	// its own rowTable
	rowTables     [][]*rowTable
	hashTable     *hashTableV2
	tagHelper     *tagPtrHelper
	memoryTracker *memory.Tracker
}

func (htc *hashTableContext) reset() {
	htc.rowTables = nil
	htc.hashTable = nil
	htc.tagHelper = nil
	htc.memoryTracker.Detach()
}

func (htc *hashTableContext) getAllMemoryUsageInHashTable() int64 {
	partNum := len(htc.hashTable.tables)
	totalMemoryUsage := int64(0)
	for i := range partNum {
		mem := htc.hashTable.getPartitionMemoryUsage(i)
		totalMemoryUsage += mem
	}
	return totalMemoryUsage
}

func (htc *hashTableContext) clearHashTable() {
	partNum := len(htc.hashTable.tables)
	for i := range partNum {
		htc.hashTable.clearPartitionSegments(i)
	}
}

func (htc *hashTableContext) getPartitionMemoryUsage(partID int) int64 {
	totalMemoryUsage := int64(0)
	for _, tables := range htc.rowTables {
		if tables != nil && tables[partID] != nil {
			totalMemoryUsage += tables[partID].getTotalMemoryUsage()
		}
	}

	return totalMemoryUsage
}

func (htc *hashTableContext) getSegmentsInRowTable(workerID, partitionID int) []*rowTableSegment {
	if htc.rowTables[workerID] != nil && htc.rowTables[workerID][partitionID] != nil {
		return htc.rowTables[workerID][partitionID].getSegments()
	}

	return nil
}

func (htc *hashTableContext) getAllSegmentsMemoryUsageInRowTable() int64 {
	totalMemoryUsage := int64(0)
	for _, tables := range htc.rowTables {
		for _, table := range tables {
			if table != nil {
				totalMemoryUsage += table.getTotalMemoryUsage()
			}
		}
	}
	return totalMemoryUsage
}

func (htc *hashTableContext) clearAllSegmentsInRowTable() {
	for _, tables := range htc.rowTables {
		for _, table := range tables {
			if table != nil {
				table.clearSegments()
			}
		}
	}
}

func (htc *hashTableContext) clearSegmentsInRowTable(workerID, partitionID int) {
	if htc.rowTables[workerID] != nil && htc.rowTables[workerID][partitionID] != nil {
		htc.rowTables[workerID][partitionID].clearSegments()
	}
}

func (htc *hashTableContext) build(task *buildTask) {
	htc.hashTable.tables[task.partitionIdx].build(task.segStartIdx, task.segEndIdx, htc.tagHelper)
}

func (htc *hashTableContext) lookup(partitionIndex int, hashValue uint64) taggedPtr {
	return htc.hashTable.tables[partitionIndex].lookup(hashValue, htc.tagHelper)
}

func (htc *hashTableContext) appendRowSegment(workerID, partitionID int, seg *rowTableSegment) {
	if len(seg.hashValues) == 0 {
		return
	}

	if htc.rowTables[workerID][partitionID] == nil {
		htc.rowTables[workerID][partitionID] = newRowTable()
	}

	seg.initTaggedBits()
	htc.rowTables[workerID][partitionID].segments = append(htc.rowTables[workerID][partitionID].segments, seg)
}

func (*hashTableContext) calculateHashTableMemoryUsage(rowTables []*rowTable) (int64, []int64) {
	totalMemoryUsage := int64(0)
	partitionsMemoryUsage := make([]int64, 0)
	for _, table := range rowTables {
		hashTableLength := getHashTableLengthByRowTable(table)
		memoryUsage := getHashTableMemoryUsage(hashTableLength)
		partitionsMemoryUsage = append(partitionsMemoryUsage, memoryUsage)
		totalMemoryUsage += memoryUsage
	}
	return totalMemoryUsage, partitionsMemoryUsage
}

// In order to avoid the allocation of hash table, we pre-calculate the memory usage in advance
// to know which hash tables need to be created.
func (htc *hashTableContext) tryToSpill(rowTables []*rowTable, spillHelper *hashJoinSpillHelper) ([]*rowTable, error) {
	totalMemoryUsage, hashTableMemoryUsage := htc.calculateHashTableMemoryUsage(rowTables)

	// Pre-consume the memory usage
	htc.memoryTracker.Consume(totalMemoryUsage)

	if spillHelper != nil && spillHelper.isSpillNeeded() {
		spillHelper.spillTriggeredBeforeBuildingHashTableForTest = true
		err := spillHelper.spillRowTable(hashTableMemoryUsage)
		if err != nil {
			return nil, err
		}

		spilledPartition := spillHelper.getSpilledPartitions()
		for _, partID := range spilledPartition {
			// Clear spilled row tables
			rowTables[partID].clearSegments()
		}

		// Though some partitions have been spilled or are empty, their hash tables are still be created
		// because probe rows in these partitions may access their hash tables.
		// We need to consider these memory usage.
		totalDefaultMemUsage := getHashTableMemoryUsage(minimalHashTableLen) * int64(len(spilledPartition))

		// Hash table memory usage has already been released in spill operation.
		// So it's unnecessary to release them again.
		htc.memoryTracker.Consume(totalDefaultMemUsage)
	}

	return rowTables, nil
}

func (htc *hashTableContext) mergeRowTablesToHashTable(partitionNumber uint, spillHelper *hashJoinSpillHelper) (int, error) {
	rowTables := make([]*rowTable, partitionNumber)
	for i := range partitionNumber {
		rowTables[i] = newRowTable()
	}

	totalSegmentCnt := 0
	for _, rowTablesPerWorker := range htc.rowTables {
		for partIdx, rt := range rowTablesPerWorker {
			if rt == nil {
				continue
			}
			rowTables[partIdx].merge(rt)
			totalSegmentCnt += len(rt.segments)
		}
	}

	var err error

	// spillHelper may be nil in ut
	if spillHelper != nil {
		rowTables, err = htc.tryToSpill(rowTables, spillHelper)
		if err != nil {
			return 0, err
		}

		spillHelper.setCanSpillFlag(false)
	}

	taggedBits := uint8(maxTaggedBits)
	for i := range partitionNumber {
		for _, seg := range rowTables[i].segments {
			taggedBits = min(taggedBits, seg.taggedBits)
		}
		htc.hashTable.tables[i] = newSubTable(rowTables[i])
	}

	htc.tagHelper = &tagPtrHelper{}
	htc.tagHelper.init(taggedBits)

	htc.clearAllSegmentsInRowTable()
	return totalSegmentCnt, nil
}
