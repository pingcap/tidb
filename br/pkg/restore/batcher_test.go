// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type drySender struct {
	mu *sync.Mutex

	rewriteRules *restore.RewriteRules
	ranges       []rtree.Range
	nBatch       int

	sink restore.TableSink
}

func (sender *drySender) PutSink(sink restore.TableSink) {
	sender.sink = sink
}

func (sender *drySender) RestoreBatch(ranges restore.DrainResult) {
	sender.mu.Lock()
	defer sender.mu.Unlock()
	log.Info("fake restore range", rtree.ZapRanges(ranges.Ranges))
	sender.nBatch++
	sender.rewriteRules.Append(*ranges.RewriteRules)
	sender.ranges = append(sender.ranges, ranges.Ranges...)
	sender.sink.EmitTables(ranges.BlankTablesAfterSend...)
}

func (sender *drySender) Close() {
	sender.sink.Close()
}

func waitForSend() {
	time.Sleep(10 * time.Millisecond)
}

func (sender *drySender) Ranges() []rtree.Range {
	return sender.ranges
}

func newDrySender() *drySender {
	return &drySender{
		rewriteRules: restore.EmptyRewriteRule(),
		ranges:       []rtree.Range{},
		mu:           new(sync.Mutex),
	}
}

type recordCurrentTableManager struct {
	lock sync.Mutex
	m    map[int64]bool
}

func (manager *recordCurrentTableManager) Close(ctx context.Context) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	if len(manager.m) > 0 {
		log.Panic("When closing, there are still some tables doesn't be sent",
			zap.Any("tables", manager.m))
	}
}

func newMockManager() *recordCurrentTableManager {
	return &recordCurrentTableManager{
		m: make(map[int64]bool),
	}
}

func (manager *recordCurrentTableManager) Enter(_ context.Context, tables []restore.CreatedTable) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	for _, t := range tables {
		log.Info("entering", zap.Int64("table ID", t.Table.ID))
		manager.m[t.Table.ID] = true
	}
	return nil
}

func (manager *recordCurrentTableManager) Leave(_ context.Context, tables []restore.CreatedTable) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	for _, t := range tables {
		if !manager.m[t.Table.ID] {
			return errors.Errorf("Table %d is removed before added", t.Table.ID)
		}
		log.Info("leaving", zap.Int64("table ID", t.Table.ID))
		delete(manager.m, t.Table.ID)
	}
	return nil
}

func (manager *recordCurrentTableManager) Has(tables ...restore.TableWithRange) bool {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	ids := make([]int64, 0, len(tables))
	currentIDs := make([]int64, 0, len(manager.m))
	for _, t := range tables {
		ids = append(ids, t.Table.ID)
	}
	for id, contains := range manager.m {
		if contains {
			currentIDs = append(currentIDs, id)
		}
	}
	log.Info("testing", zap.Int64s("should has ID", ids), zap.Int64s("has ID", currentIDs))
	for _, i := range ids {
		if !manager.m[i] {
			return false
		}
	}
	return true
}

func (sender *drySender) HasRewriteRuleOfKey(prefix string) bool {
	sender.mu.Lock()
	defer sender.mu.Unlock()
	for _, rule := range sender.rewriteRules.Data {
		if bytes.Equal([]byte(prefix), rule.OldKeyPrefix) {
			return true
		}
	}
	return false
}

func (sender *drySender) RangeLen() int {
	sender.mu.Lock()
	defer sender.mu.Unlock()
	return len(sender.ranges)
}

func (sender *drySender) BatchCount() int {
	return sender.nBatch
}

func fakeTableWithRange(id int64, rngs []rtree.Range) restore.TableWithRange {
	tbl := &metautil.Table{
		DB: &model.DBInfo{},
		Info: &model.TableInfo{
			ID: id,
		},
	}
	tblWithRng := restore.TableWithRange{
		CreatedTable: restore.CreatedTable{
			RewriteRule: restore.EmptyRewriteRule(),
			Table:       tbl.Info,
			OldTable:    tbl,
		},
		Range: rngs,
	}
	return tblWithRng
}

func fakeRewriteRules(oldPrefix string, newPrefix string) *restore.RewriteRules {
	return &restore.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: []byte(oldPrefix),
				NewKeyPrefix: []byte(newPrefix),
			},
		},
	}
}

func fakeRange(startKey, endKey string) rtree.Range {
	return rtree.Range{
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}

func join(nested [][]rtree.Range) (plain []rtree.Range) {
	for _, ranges := range nested {
		plain = append(plain, ranges...)
	}
	return plain
}

// TestBasic tests basic workflow of batcher.
func TestBasic(t *testing.T) {
	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := restore.NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(2)

	tableRanges := [][]rtree.Range{
		{fakeRange("aaa", "aab")},
		{fakeRange("baa", "bab"), fakeRange("bac", "bad")},
		{fakeRange("caa", "cab"), fakeRange("cac", "cad")},
	}

	simpleTables := []restore.TableWithRange{}
	for i, ranges := range tableRanges {
		simpleTables = append(simpleTables, fakeTableWithRange(int64(i), ranges))
	}
	for _, tbl := range simpleTables {
		batcher.Add(tbl)
	}

	batcher.Close()
	rngs := sender.Ranges()

	require.Equal(t, rngs, join(tableRanges))
	select {
	case err := <-errCh:
		t.Fatal(errors.Trace(err))
	default:
	}
}

func TestAutoSend(t *testing.T) {
	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := restore.NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(1024)

	simpleTable := fakeTableWithRange(1, []rtree.Range{fakeRange("caa", "cab"), fakeRange("cac", "cad")})

	batcher.Add(simpleTable)
	require.Greater(t, batcher.Len(), 0)

	// enable auto commit.
	batcher.EnableAutoCommit(ctx, 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	require.Greater(t, sender.RangeLen(), 0)
	require.Equal(t, 0, batcher.Len())

	batcher.Close()

	rngs := sender.Ranges()
	require.Equal(t, simpleTable.Range, rngs)
	select {
	case err := <-errCh:
		t.Fatal(errors.Trace(err))
	default:
	}
}

func TestSplitRangeOnSameTable(t *testing.T) {
	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := restore.NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(2)

	simpleTable := fakeTableWithRange(1, []rtree.Range{
		fakeRange("caa", "cab"), fakeRange("cac", "cad"),
		fakeRange("cae", "caf"), fakeRange("cag", "cai"),
		fakeRange("caj", "cak"), fakeRange("cal", "cam"),
		fakeRange("can", "cao"), fakeRange("cap", "caq"),
	})

	batcher.Add(simpleTable)
	batcher.Close()
	require.Equal(t, 4, sender.BatchCount())

	rngs := sender.Ranges()
	require.Equal(t, simpleTable.Range, rngs)
	select {
	case err := <-errCh:
		t.Fatal(errors.Trace(err))
	default:
	}
}

func TestRewriteRules(t *testing.T) {
	tableRanges := [][]rtree.Range{
		{fakeRange("aaa", "aab")},
		{fakeRange("baa", "bab"), fakeRange("bac", "bad")},
		{
			fakeRange("caa", "cab"), fakeRange("cac", "cad"),
			fakeRange("cae", "caf"), fakeRange("cag", "cai"),
			fakeRange("caj", "cak"), fakeRange("cal", "cam"),
			fakeRange("can", "cao"), fakeRange("cap", "caq"),
		},
	}
	rewriteRules := []*restore.RewriteRules{
		fakeRewriteRules("a", "ada"),
		fakeRewriteRules("b", "bob"),
		fakeRewriteRules("c", "cpp"),
	}

	tables := make([]restore.TableWithRange, 0, len(tableRanges))
	for i, ranges := range tableRanges {
		table := fakeTableWithRange(int64(i), ranges)
		table.RewriteRule = rewriteRules[i]
		tables = append(tables, table)
	}

	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := restore.NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(2)

	batcher.Add(tables[0])
	waitForSend()
	require.Equal(t, 0, sender.RangeLen())

	batcher.Add(tables[1])
	waitForSend()
	require.True(t, sender.HasRewriteRuleOfKey("a"))
	require.True(t, sender.HasRewriteRuleOfKey("b"))
	require.True(t, manager.Has(tables[1]))
	require.Equal(t, 2, sender.RangeLen())

	batcher.Add(tables[2])
	batcher.Close()
	require.True(t, sender.HasRewriteRuleOfKey("c"))
	require.Equal(t, join(tableRanges), sender.Ranges())

	select {
	case err := <-errCh:
		t.Fatal(errors.Trace(err))
	default:
	}
}

func TestBatcherLen(t *testing.T) {
	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := restore.NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(15)

	simpleTable := fakeTableWithRange(1, []rtree.Range{
		fakeRange("caa", "cab"), fakeRange("cac", "cad"),
		fakeRange("cae", "caf"), fakeRange("cag", "cai"),
		fakeRange("caj", "cak"), fakeRange("cal", "cam"),
		fakeRange("can", "cao"), fakeRange("cap", "caq"),
	})

	simpleTable2 := fakeTableWithRange(2, []rtree.Range{
		fakeRange("caa", "cab"), fakeRange("cac", "cad"),
		fakeRange("cae", "caf"), fakeRange("cag", "cai"),
		fakeRange("caj", "cak"), fakeRange("cal", "cam"),
		fakeRange("can", "cao"), fakeRange("cap", "caq"),
	})

	batcher.Add(simpleTable)
	waitForSend()
	require.Equal(t, 8, batcher.Len())
	require.False(t, manager.Has(simpleTable))
	require.False(t, manager.Has(simpleTable2))

	batcher.Add(simpleTable2)
	waitForSend()
	require.Equal(t, 1, batcher.Len())
	require.True(t, manager.Has(simpleTable2))
	require.False(t, manager.Has(simpleTable))
	batcher.Close()
	require.Equal(t, 0, batcher.Len())

	select {
	case err := <-errCh:
		t.Fatal(errors.Trace(err))
	default:
	}
}
