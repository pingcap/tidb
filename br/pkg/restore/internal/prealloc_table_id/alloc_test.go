// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/br/pkg/metautil"
	prealloctableid "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_table_id"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
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

func checkMap(m map[string][]*model.TableInfo, from, end int64) bool {
	used := make(map[int64]struct{})
	validate := func(id int64) bool {
		return id >= from && id < end
	}
	checkDup := func(id int64) bool {
		if _, ok := used[id]; ok {
			return true
		}
		used[id] = struct{}{}
		return false
	}

	for db, infos := range m {
		dbID, _ := strconv.ParseInt(db, 10, 64)
		for _, info := range infos {
			if !validate(info.ID) || checkDup(info.ID) ||
				(validate(dbID) && dbID != info.ID) {
				return false
			}
			for _, part := range info.Partition.Definitions {
				if !validate(part.ID) || checkDup(part.ID) {
					return false
				}
			}
		}
	}
	return true
}

func TestAllocator(t *testing.T) {
	type Case struct {
		tableIDs       []int64
		partitions     map[int64][]int64
		hasAllocatedTo int64
		allocedRange   [2]int64
		msg            string
	}

	cases := []Case{
		{
			tableIDs:       []int64{},
			hasAllocatedTo: 20,
			allocedRange:   [2]int64{21, 20},
			msg:            "ID:empty(end=0)",
		},
		{
			tableIDs:       []int64{1, 2, 5, 6, 7},
			hasAllocatedTo: 6,
			allocedRange:   [2]int64{7, 12},
			msg:            "ID:[7,12)",
		},
		{
			tableIDs:       []int64{4, 6, 9, 2},
			hasAllocatedTo: 1,
			allocedRange:   [2]int64{2, 6},
			msg:            "ID:[2,6)",
		},
		{
			tableIDs:       []int64{1, 2, 3, 4},
			hasAllocatedTo: 5,
			allocedRange:   [2]int64{6, 10},
			msg:            "ID:[6,10)",
		},
		{
			tableIDs:       []int64{1, 2, 5, 6, 1 << 50, 1<<50 + 2479},
			hasAllocatedTo: 3,
			allocedRange:   [2]int64{4, 10},
			msg:            "ID:[4,10)",
		},
		{
			tableIDs:       []int64{1, 2, 5, 6, 7},
			hasAllocatedTo: 6,
			allocedRange:   [2]int64{7, 17},
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
			msg: "ID:[7,17)",
		},
		{
			tableIDs:       []int64{1, 2, 5, 6, 7, 13},
			hasAllocatedTo: 9,
			allocedRange:   [2]int64{10, 21},
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
			msg: "ID:[10,21)",
		},
	}

	run := func(t *testing.T, c Case) {
		tables := make([]*metautil.Table, 0, len(c.tableIDs))
		for _, id := range c.tableIDs {
			table := metautil.Table{
				// Use the same ID for DB and table.
				// We will use DB ID to check if table ID is correctly allocated.
				DB: &model.DBInfo{
					ID:   id,
					Name: ast.CIStr{L: fmt.Sprintf("%d", id)},
				},
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
		ids.Alloc(&allocator)
		alloc, err := ids.BatchAlloc(tables)
		checkMap(alloc, c.allocedRange[0], c.allocedRange[1])
		require.NoError(t, err)
		require.Equal(t, c.msg, ids.String())
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
}
