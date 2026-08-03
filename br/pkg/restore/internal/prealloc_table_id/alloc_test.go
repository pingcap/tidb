// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/tidb/br/pkg/metautil"
	prealloctableid "github.com/pingcap/tidb/br/pkg/restore/internal/prealloc_table_id"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type testAllocator int64

const InsaneTableIDThreshold = math.MaxUint32

func (t *testAllocator) GetGlobalID() (int64, error) {
	return int64(*t), nil
}

func (t *testAllocator) AdvanceGlobalIDs(n int) (int64, error) {
	old := int64(*t)
	*t = testAllocator(int64(*t) + int64(n))
	return old, nil
}

func checkBatchAlloc(ret map[string]*model.TableInfo, tables []*metautil.Table, current, reusable int64) error {
	if len(ret) != len(tables) {
		return errors.Errorf("expect %d tables, but got %d", len(tables), len(ret))
	}

	for _, t := range tables {
		if _, ok := ret[t.Info.Name.L]; !ok {
			return errors.Errorf("table %s not found in the result", t.Info.Name)
		}

		retInfo := ret[t.Info.Name.L]
		if t.Info.ID > current && t.Info.ID < InsaneTableIDThreshold && retInfo.ID != t.Info.ID {
			return errors.Errorf("expect table %s ID to be %d, but got %d", t.Info.Name, t.Info.ID, retInfo.ID)
		}
		if (t.Info.ID <= current || t.Info.ID >= InsaneTableIDThreshold) && retInfo.ID < reusable {
			return errors.Errorf("expect table %s ID to be greater than %d, but got %d", t.Info.Name, current, retInfo.ID)
		}
	}
	return nil
}

func batchAlloc(tables []*metautil.Table, p *prealloctableid.PreallocIDs) (map[string]*model.TableInfo, error) {
	clonedInfos := make(map[string]*model.TableInfo, len(tables))
	if len(tables) == 0 {
		return clonedInfos, nil
	}

	for _, t := range tables {
		infoClone, err := p.RewriteTableInfo(t.Info)
		if err != nil {
			return nil, err
		}
		clonedInfos[t.Info.Name.L] = infoClone
	}

	return clonedInfos, nil
}

func TestAllocator(t *testing.T) {
	type Case struct {
		tableIDs       []int64
		partitions     map[int64][]int64
		hasAllocatedTo int64
		reusableBorder int64
		msg            string
	}

	msg := func(c *Case) string {
		if len(c.tableIDs) == 0 {
			return "ID:empty(end=0)"
		}
		rewriteCnt := int64(0)
		for _, id := range c.tableIDs {
			if id <= c.hasAllocatedTo || id >= InsaneTableIDThreshold {
				rewriteCnt++
			}
		}
		for _, part := range c.partitions {
			for _, id := range part {
				if id <= c.hasAllocatedTo || id >= InsaneTableIDThreshold {
					rewriteCnt++
				}
			}
		}
		return fmt.Sprintf("ID:[%d,%d)", c.hasAllocatedTo+1, c.reusableBorder+rewriteCnt)
	}

	cases := []Case{
		{
			tableIDs:       []int64{},
			hasAllocatedTo: 20,
			reusableBorder: 0,
		},
		{
			tableIDs:       []int64{1, 2, 15, 6, 7},
			hasAllocatedTo: 6,
			reusableBorder: 16,
		},
		{
			tableIDs:       []int64{4, 6, 9, 2},
			hasAllocatedTo: 1,
			reusableBorder: 10,
		},
		{
			tableIDs:       []int64{1, 2, 3, 4},
			hasAllocatedTo: 5,
			reusableBorder: 6,
		},
		{
			tableIDs:       []int64{2, 3, 4, 5},
			hasAllocatedTo: 5,
			reusableBorder: 6,
		},
		{
			tableIDs:       []int64{10, 7, 8, 9},
			hasAllocatedTo: 5,
			reusableBorder: 13,
			partitions: map[int64][]int64{
				7: {2, 3, 4, 11, 12},
			},
		},
		{
			tableIDs:       []int64{1, 2, 5, 6, 1 << 50, 1<<50 + 2479},
			hasAllocatedTo: 3,
			reusableBorder: 7,
		},
		{
			tableIDs:       []int64{11, 22, 5, 6, 7},
			hasAllocatedTo: 6,
			reusableBorder: 23,
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
		},
		{
			tableIDs:       []int64{1, 2, 9000005, 7, 17, 130},
			hasAllocatedTo: 9,
			reusableBorder: 9000006,
			partitions: map[int64][]int64{
				7: {8, 9, 10, 11, 12},
			},
		},
	}

	run := func(t *testing.T, c Case) {
		tables := make([]*metautil.Table, 0, len(c.tableIDs))
		for _, id := range c.tableIDs {
			table := metautil.Table{
				DB: &model.DBInfo{
					Name: pmodel.NewCIStr("test"),
				},
				Info: &model.TableInfo{
					Name:      pmodel.NewCIStr(fmt.Sprintf("t%d", id)),
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

		ids, err := prealloctableid.New(tables)
		require.NoError(t, err)
		allocator := testAllocator(c.hasAllocatedTo)
		ids.PreallocIDs(&allocator)
		alloc, err := batchAlloc(tables, ids)
		require.NoError(t, checkBatchAlloc(alloc, tables, c.hasAllocatedTo, c.reusableBorder))
		require.NoError(t, err)
		require.Equal(t, msg(&c), ids.String())
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
	ids, err := prealloctableid.New(tableInfos)
	require.NoError(t, err)
	lastGlobalID := currentGlobalID
	err = kv.RunInNewTxn(ctx, s.Mock.Store(), true, func(_ context.Context, txn kv.Transaction) error {
		allocator := meta.NewMutator(txn)
		if err := ids.PreallocIDs(allocator); err != nil {
			return err
		}
		currentGlobalID, err = allocator.GetGlobalID()
		return err
	})
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("ID:[%d,%d)", lastGlobalID+1, currentGlobalID+1), ids.String())
}
