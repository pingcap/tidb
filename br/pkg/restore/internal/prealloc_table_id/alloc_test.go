// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/metautil"
	prealloctableid "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_table_id"
	"github.com/pingcap/tidb/pkg/parser/model"
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
	}

	cases := []Case{
		{
			tableIDs:              []int64{1, 2, 5, 6, 7},
			hasAllocatedTo:        6,
			successfullyAllocated: []int64{6, 7},
			shouldAllocatedTo:     8,
		},
		{
			tableIDs:              []int64{4, 6, 9, 2},
			hasAllocatedTo:        1,
			successfullyAllocated: []int64{2, 4, 6, 9},
			shouldAllocatedTo:     10,
		},
		{
			tableIDs:              []int64{1, 2, 3, 4},
			hasAllocatedTo:        5,
			successfullyAllocated: []int64{},
			shouldAllocatedTo:     5,
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 1 << 50, 1<<50 + 2479},
			hasAllocatedTo:        3,
			successfullyAllocated: []int64{5, 6},
			shouldAllocatedTo:     7,
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 7},
			hasAllocatedTo:        6,
			successfullyAllocated: []int64{6, 7},
			shouldAllocatedTo:     13,
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 7, 13},
			hasAllocatedTo:        9,
			successfullyAllocated: []int64{13},
			shouldAllocatedTo:     14,
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
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
		require.NoError(t, ids.Alloc(&allocator))

		allocated := make([]int64, 0, len(c.successfullyAllocated))
		for _, t := range tables {
			if ids.PreallocedFor(t.Info) {
				allocated = append(allocated, t.Info.ID)
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
