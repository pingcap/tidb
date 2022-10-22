// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/metautil"
	prealloctableid "github.com/pingcap/tidb/br/pkg/restore/prealloc_table_id"
	"github.com/pingcap/tidb/parser/model"
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
	}

	run := func(t *testing.T, c Case) {
		tables := make([]*metautil.Table, 0, len(c.tableIDs))
		for _, id := range c.tableIDs {
			tables = append(tables, &metautil.Table{
				Info: &model.TableInfo{
					ID: id,
				},
			})
		}

		ids := prealloctableid.New(tables)
		allocator := testAllocator(c.hasAllocatedTo)
		require.NoError(t, ids.Alloc(&allocator))

		allocated := make([]int64, 0, len(c.successfullyAllocated))
		for _, t := range c.tableIDs {
			if ids.Prealloced(t) {
				allocated = append(allocated, t)
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
