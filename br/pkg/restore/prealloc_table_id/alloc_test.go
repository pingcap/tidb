// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/metautil"
<<<<<<< HEAD:br/pkg/restore/prealloc_table_id/alloc_test.go
	prealloctableid "github.com/pingcap/tidb/br/pkg/restore/prealloc_table_id"
	"github.com/pingcap/tidb/pkg/parser/model"
=======
	prealloctableid "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_table_id"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
>>>>>>> c08679bccfa (br: fix pre allocate id exceeds bound (#59719)):br/pkg/restore/internal/prealloc_table_id/alloc_test.go
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
			successfullyAllocated: []int64{7},
			shouldAllocatedTo:     8,
<<<<<<< HEAD:br/pkg/restore/prealloc_table_id/alloc_test.go
=======
			msg:                   "ID:[7,8)",
>>>>>>> c08679bccfa (br: fix pre allocate id exceeds bound (#59719)):br/pkg/restore/internal/prealloc_table_id/alloc_test.go
		},
		{
			tableIDs:              []int64{4, 6, 9, 2},
			hasAllocatedTo:        1,
			successfullyAllocated: []int64{2, 4, 6, 9},
			shouldAllocatedTo:     10,
<<<<<<< HEAD:br/pkg/restore/prealloc_table_id/alloc_test.go
=======
			msg:                   "ID:[2,10)",
>>>>>>> c08679bccfa (br: fix pre allocate id exceeds bound (#59719)):br/pkg/restore/internal/prealloc_table_id/alloc_test.go
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
<<<<<<< HEAD:br/pkg/restore/prealloc_table_id/alloc_test.go
=======
			msg:                   "ID:[4,7)",
>>>>>>> c08679bccfa (br: fix pre allocate id exceeds bound (#59719)):br/pkg/restore/internal/prealloc_table_id/alloc_test.go
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 7},
			hasAllocatedTo:        6,
			successfullyAllocated: []int64{7},
			shouldAllocatedTo:     13,
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
<<<<<<< HEAD:br/pkg/restore/prealloc_table_id/alloc_test.go
=======
			msg: "ID:[7,13)",
>>>>>>> c08679bccfa (br: fix pre allocate id exceeds bound (#59719)):br/pkg/restore/internal/prealloc_table_id/alloc_test.go
		},
		{
			tableIDs:              []int64{1, 2, 5, 6, 7, 13},
			hasAllocatedTo:        9,
			successfullyAllocated: []int64{13},
			shouldAllocatedTo:     14,
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
<<<<<<< HEAD:br/pkg/restore/prealloc_table_id/alloc_test.go
=======
			msg: "ID:[10,14)",
>>>>>>> c08679bccfa (br: fix pre allocate id exceeds bound (#59719)):br/pkg/restore/internal/prealloc_table_id/alloc_test.go
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

func TestAllocatorBound(t *testing.T) {
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)
	tk.MustExec("CREATE TABLE test.t1 (id int);")
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR)
	currentGlobalID := int64(0)
	err := kv.RunInNewTxn(ctx, s.Mock.Store(), true, func(_ context.Context, txn kv.Transaction) (err error) {
		allocator := meta.NewMutator(txn)
		currentGlobalID, err = allocator.GetGlobalID()
		return err
	})
	require.NoError(t, err)
	rows := tk.MustQuery("ADMIN SHOW DDL JOBS WHERE JOB_ID = ?", currentGlobalID).Rows()
	// The current global ID is used, so it cannot use anymore.
	require.Len(t, rows, 1)
	tableInfos := []*metautil.Table{
		{Info: &model.TableInfo{ID: currentGlobalID}},
		{Info: &model.TableInfo{ID: currentGlobalID + 2}},
		{Info: &model.TableInfo{ID: currentGlobalID + 4}},
	}
	ids := prealloctableid.New(tableInfos)
	lastGlobalID := currentGlobalID
	err = kv.RunInNewTxn(ctx, s.Mock.Store(), true, func(_ context.Context, txn kv.Transaction) error {
		allocator := meta.NewMutator(txn)
		if err := ids.Alloc(allocator); err != nil {
			return err
		}
		currentGlobalID, err = allocator.GetGlobalID()
		return err
	})
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("ID:[%d,%d)", lastGlobalID+1, currentGlobalID), ids.String())
	require.False(t, ids.Prealloced(tableInfos[0].Info.ID))
	require.True(t, ids.Prealloced(tableInfos[1].Info.ID))
	require.True(t, ids.Prealloced(tableInfos[2].Info.ID))
	require.True(t, ids.Prealloced(currentGlobalID-1))
}
