// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/metautil"
	prealloctableid "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_table_id"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

type testAllocator int64

func (t *testAllocator) GetGlobalID() (int64, error) {
	return int64(*t), nil
}

func (t *testAllocator) AdvanceGlobalIDs(n int) (int64, error) {
	old := int64(*t)
	*t = testAllocator(int64(*t) + int64(n))
	return old, nil
}

func TestAllocator(t *testing.T) {
	type Case struct {
		tableIDs              []int64
		partitions            map[int64][]int64
		hasAllocatedTo        int64
		successfullyAllocated []int64
		shouldAllocatedTo     int64
		msg                   string
	}

	cases := []Case{
		{
			tableIDs:              []int64{1, 2, 5, 6, 7},
			hasAllocatedTo:        6,
			successfullyAllocated: []int64{7, 8, 9, 10, 11},
			shouldAllocatedTo:     12,
			msg:                   "ID:[7,8), unreusedCount: 4/5, insaneTableIDCount: 0",
		},
		{
			tableIDs:              []int64{4, 6, 9, 2},
			hasAllocatedTo:        1,
			successfullyAllocated: []int64{2, 4, 6, 9},
			shouldAllocatedTo:     10,
			msg:                   "ID:[2,10), unreusedCount: 0/4, insaneTableIDCount: 0",
		},
		{
			tableIDs:              []int64{1, 2, 3, 4},
			hasAllocatedTo:        5,
			successfullyAllocated: []int64{6, 7, 8, 9},
			shouldAllocatedTo:     10,
			msg:                   "ID:empty(end=5), unreusedCount: 4/4, insaneTableIDCount: 0",
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 1 << 50, 1<<50 + 2479},
			hasAllocatedTo:        3,
			successfullyAllocated: []int64{5, 6, 7, 8, 9, 10},
			shouldAllocatedTo:     11,
			msg:                   "ID:[4,7), unreusedCount: 2/6, insaneTableIDCount: 2",
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 7},
			hasAllocatedTo:        6,
			successfullyAllocated: []int64{7, 10, 11, 12, 13, 14, 15, 16, 17, 18},
			shouldAllocatedTo:     19,
			partitions: map[int64][]int64{
				7: {3, 4, 10, 11, 12},
			},
			msg: "ID:[7,13), unreusedCount: 6/10, insaneTableIDCount: 0",
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 7, 13},
			hasAllocatedTo:        9,
			successfullyAllocated: []int64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			shouldAllocatedTo:     21,
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
			msg: "ID:[10,14), unreusedCount: 7/11, insaneTableIDCount: 0",
		},
	}

	run := func(t *testing.T, c Case) {
		tables := make([]*metautil.Table, 0, len(c.tableIDs))
		for _, id := range c.tableIDs {
			table := metautil.Table{
				Info: &model.TableInfo{
					ID:        id,
					Partition: &model.PartitionInfo{},
				},
			}
			if c.partitions != nil {
				for _, part := range c.partitions[id] {
					table.Info.Partition.Definitions = append(table.Info.Partition.Definitions, model.PartitionDefinition{ID: part})
				}
			}
			tables = append(tables, &table)
		}

		ids := prealloctableid.New(tables)
		allocator := testAllocator(c.hasAllocatedTo)
		require.NoError(t, ids.Alloc(&allocator, []metautil.TableIDSpan{}))
		require.Equal(t, c.msg, ids.String())

		allocated := make([]int64, 0, len(c.successfullyAllocated))
		for _, t := range tables {
			newTableInfo := ids.TryRewriteTableID(t.Info.Clone())
			allocated = append(allocated, newTableInfo.ID)
			if _, exists := c.partitions[t.Info.ID]; exists {
				for _, def := range newTableInfo.Partition.Definitions {
					allocated = append(allocated, def.ID)
				}
			}
		}
		require.ElementsMatch(t, allocated, c.successfullyAllocated)
		require.Equal(t, int64(allocator), c.shouldAllocatedTo)
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			run(t, c)
		})
	}
}

func span(start, end int64) metautil.TableIDSpan {
	return metautil.TableIDSpan{
		StartTableID: start,
		EndTableID:   end,
	}
}

func TestAllocator2(t *testing.T) {
	type Case struct {
		tableIDs              []int64
		partitions            map[int64][]int64
		tableIDSpans          []metautil.TableIDSpan
		hasAllocatedTo        int64
		successfullyAllocated []int64
		shouldAllocatedTo     int64
		msg                   string
	}

	cases := []Case{
		{
			tableIDs:              []int64{1, 2, 5, 6, 7},
			tableIDSpans:          []metautil.TableIDSpan{span(6, 7)},
			hasAllocatedTo:        6,
			successfullyAllocated: []int64{8, 9, 10, 11, 12},
			shouldAllocatedTo:     13,
			msg:                   "ID:empty(end=8), unreusedCount: 5/5, insaneTableIDCount: 0",
		},
		{
			tableIDs:              []int64{4, 6, 9, 2},
			tableIDSpans:          []metautil.TableIDSpan{span(2, 4), span(6, 9)},
			hasAllocatedTo:        1,
			successfullyAllocated: []int64{2, 4, 6, 9},
			shouldAllocatedTo:     10,
			msg:                   "ID:[2,10), unreusedCount: 0/4, insaneTableIDCount: 0",
		},
		{
			tableIDs:              []int64{1, 2, 3, 4},
			tableIDSpans:          []metautil.TableIDSpan{span(1, 3)},
			hasAllocatedTo:        5,
			successfullyAllocated: []int64{6, 7, 8, 9},
			shouldAllocatedTo:     10,
			msg:                   "ID:empty(end=5), unreusedCount: 4/4, insaneTableIDCount: 0",
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 1 << 50, 1<<50 + 2479},
			tableIDSpans:          []metautil.TableIDSpan{span(2, 6), span(1<<50, 1<<50+2479)},
			hasAllocatedTo:        3,
			successfullyAllocated: []int64{7, 8, 9, 10, 11, 12},
			shouldAllocatedTo:     13,
			msg:                   "ID:empty(end=7), unreusedCount: 4/6, insaneTableIDCount: 2",
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 7},
			tableIDSpans:          []metautil.TableIDSpan{span(3, 7)},
			hasAllocatedTo:        6,
			successfullyAllocated: []int64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
			shouldAllocatedTo:     20,
			partitions: map[int64][]int64{
				7: {3, 4, 10, 11, 12},
			},
			msg: "ID:[8,13), unreusedCount: 7/10, insaneTableIDCount: 0",
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 7},
			tableIDSpans:          []metautil.TableIDSpan{span(3, 6)},
			hasAllocatedTo:        5,
			successfullyAllocated: []int64{7, 10, 11, 12, 13, 14, 15, 16, 17, 18},
			shouldAllocatedTo:     19,
			partitions: map[int64][]int64{
				7: {3, 4, 10, 11, 12},
			},
			msg: "ID:[7,13), unreusedCount: 6/10, insaneTableIDCount: 0",
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 7, 13},
			tableIDSpans:          []metautil.TableIDSpan{span(8, 11), span(12, 13)},
			hasAllocatedTo:        9,
			successfullyAllocated: []int64{12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
			shouldAllocatedTo:     23,
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
			msg: "ID:[12,14), unreusedCount: 9/11, insaneTableIDCount: 0",
		},
	}

	run := func(t *testing.T, c Case) {
		tables := make([]*metautil.Table, 0, len(c.tableIDs))
		for _, id := range c.tableIDs {
			table := metautil.Table{
				Info: &model.TableInfo{
					ID:        id,
					Partition: &model.PartitionInfo{},
				},
			}
			if c.partitions != nil {
				for _, part := range c.partitions[id] {
					table.Info.Partition.Definitions = append(table.Info.Partition.Definitions, model.PartitionDefinition{ID: part})
				}
			}
			tables = append(tables, &table)
		}

		ids := prealloctableid.New(tables)
		allocator := testAllocator(c.hasAllocatedTo)
		require.NoError(t, ids.Alloc(&allocator, c.tableIDSpans))
		require.Equal(t, c.msg, ids.String())

		allocated := make([]int64, 0, len(c.successfullyAllocated))
		for _, t := range tables {
			newTableInfo := ids.TryRewriteTableID(t.Info.Clone())
			allocated = append(allocated, newTableInfo.ID)
			if _, exists := c.partitions[t.Info.ID]; exists {
				for _, def := range newTableInfo.Partition.Definitions {
					allocated = append(allocated, def.ID)
				}
			}
		}
		require.ElementsMatch(t, allocated, c.successfullyAllocated)
		require.Equal(t, int64(allocator), c.shouldAllocatedTo)
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			run(t, c)
		})
	}
}
