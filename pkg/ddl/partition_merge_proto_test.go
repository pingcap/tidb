// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"

	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
)

// PartitionProtoOpenStoreForTest is installed by the external test package to
// avoid the production driver's dependency back to ddl. It is not a product API.
var PartitionProtoOpenStoreForTest func(string) (kv.Storage, error)

type partitionProtoKV struct{ Key, Value []byte }
type partitionProtoRange struct {
	Adapter    []partitionProtoKV `json:"adapter"`
	PhysicalID int64              `json:"physical_id"`
	IndexID    int64              `json:"index_id"`
	Expected   []partitionProtoKV `json:"expected"`
}
type partitionProtoFixture struct {
	AdapterVerified bool                  `json:"adapter_verified"`
	Name            string                `json:"name"`
	Version         int                   `json:"version"`
	SnapshotTS      uint64                `json:"snapshot_ts"`
	Table           *model.TableInfo      `json:"table"`
	Rows            []partitionProtoKV    `json:"rows"`
	Input           []partitionProtoKV    `json:"input"`
	Ranges          []partitionProtoRange `json:"ranges"`
	Duplicate       bool                  `json:"duplicate,omitempty"`
}

// TestPartitionPrototypeProductionMerge executes the actual DXF step and worker,
// rather than the standalone prototype adapter. Optional real-store execution is
// restricted to a fresh loopback cluster whose exact ID is supplied by its owner.
func TestPartitionPrototypeProductionMerge(t *testing.T) {
	ctx := context.Background()
	store := partitionProtoStore(t)
	fixtures := partitionProtoStaticFixtures(t)
	if path := os.Getenv("PARTITION_PROTO_FIXTURES"); path != "" {
		var err error
		fixtures, err = readPartitionProtoFixtures(path)
		require.NoError(t, err)
		require.NotEmpty(t, fixtures)
		for _, fixture := range fixtures {
			require.True(t, fixture.AdapterVerified, "exported snapshot lacks a standalone adapter comparison")
		}

	}
	report := struct {
		Status   string   `json:"status"`
		Backend  string   `json:"backend"`
		Cases    int      `json:"cases"`
		Subtasks int      `json:"production_subtasks"`
		Checks   []string `json:"checks"`
	}{Backend: "unistore"}
	if os.Getenv("PARTITION_PROTO_PD") != "" {
		report.Backend = "tikv"
	}
	defer func() {
		report.Status = "PASS"
		if t.Failed() {
			report.Status = "FAIL"
		}
		if path := os.Getenv("PARTITION_PROTO_REPORT"); path != "" {
			raw, err := json.MarshalIndent(report, "", "  ")
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, raw, 0o600))
		}
	}()
	for _, f := range fixtures {
		require.Equal(t, 1, f.Version)
		require.NotNil(t, f.Table)
		require.NotEmpty(t, f.Ranges)
		for _, batch := range []int{1, 2, 16} {
			for _, concurrency := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/batch%d/cpu%d", f.Name, batch, concurrency), func(t *testing.T) {
					partitionProtoReset(t, store, f)
					if !f.Duplicate {
						partitionProtoExpectedFromRows(t, f)
					}
					tbl := tables.MockTableFromMeta(f.Table)
					require.NotNil(t, tbl)
					parent, ok := tbl.(table.PhysicalTable)
					require.True(t, ok)
					for _, rg := range f.Ranges {
						err := partitionProtoRunStep(ctx, store, parent, rg, batch, concurrency)
						report.Subtasks++
						if f.Duplicate {
							require.Error(t, err)
							require.True(t, kv.ErrKeyExists.Equal(err), "%v", err)
							partitionProtoRequireInput(t, store, f)
							break
						}
						require.NoError(t, err)
					}
					if !f.Duplicate {
						partitionProtoRequireIndexes(t, store, f)
						// Replaying a completed subtask must preserve normal indexes, including
						// ranges containing only m/d versions that were intentionally skipped.
						for _, rg := range f.Ranges {
							require.NoError(t, partitionProtoRunStep(ctx, store, parent, rg, batch, concurrency))
							report.Subtasks++
						}
						partitionProtoRequireIndexes(t, store, f)
					}
					partitionProtoRequireRows(t, store, f)
					report.Cases++
				})
			}
		}
	}
	report.Checks = append(report.Checks, "production DXF merge exact index equality, row immutability and idempotent replay across batch/CPU matrix")
	t.Run("commit_failure_and_retry", func(t *testing.T) {
		f := partitionProtoStaticFixtures(t)[0]
		partitionProtoReset(t, store, f)
		parent := tables.MockTableFromMeta(f.Table).(table.PhysicalTable)
		rg := f.Ranges[0]
		var staged atomic.Int32
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionMergingInTxn", func() { staged.Add(1) })
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/kv/mockCommitErrorInNewTxn", `1*return("no_retry")`)
		err := partitionProtoRunStep(ctx, store, parent, rg, 1, 1)
		require.ErrorContains(t, err, "mock commit error")
		require.Positive(t, staged.Load(), "the injected failure must follow actual index/temp mutations")
		partitionProtoRequireInput(t, store, f)
		partitionProtoRequireRows(t, store, f)
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/kv/mockCommitErrorInNewTxn")
		require.NoError(t, partitionProtoRunStep(ctx, store, parent, rg, 1, 1))
		partitionProtoRequireIndexes(t, store, f)
		partitionProtoRequireRows(t, store, f)
		report.Subtasks += 2
		report.Cases++
	})
	report.Checks = append(report.Checks, "nonretryable commit failure changes no row/index/temp bytes; fresh subtask replay succeeds")
	t.Run("concurrent_dml_conflict", func(t *testing.T) {
		f := partitionProtoStaticFixtures(t)[0]
		partitionProtoReset(t, store, f)
		parent := tables.MockTableFromMeta(f.Table).(table.PhysicalTable)
		rg := f.Ranges[0]
		item := rg.Expected[0]
		temp := bytes.Clone(item.Key)
		tablecodec.IndexKey2TempIndexKey(temp)
		var once sync.Once
		var injectedErr error
		calls := 0
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionMergingInTxn", func() {
			once.Do(func() {
				calls++
				elem := tablecodec.TempIndexValueElem{Value: item.Value, KeyVer: tablecodec.TempIndexKeyTypeMerge}
				injectedErr = kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
					if err := txn.Set(item.Key, item.Value); err != nil {
						return err
					}
					return txn.Set(temp, elem.Encode(nil))
				})
			})
		})
		require.NoError(t, partitionProtoRunStep(ctx, store, parent, rg, 1, 1))
		require.NoError(t, injectedErr)
		require.Equal(t, 1, calls)
		remaining := partitionProtoScan(t, store, temp)
		require.Len(t, remaining, 1, "the failed old merge must not delete the newer m event")
		history, err := tablecodec.DecodeTempIndexValue(remaining[0].Value)
		require.NoError(t, err)
		require.Equal(t, tablecodec.TempIndexKeyTypeMerge, history.Current().KeyVer)

		partitionProtoRequireIndexes(t, store, f)
		partitionProtoRequireRows(t, store, f)
		report.Subtasks++
		report.Cases++
	})
	report.Checks = append(report.Checks, "production merge retries after foreground commit overwrites its temp/index write set")
	t.Run("missing_partition_rejected", func(t *testing.T) {
		f := partitionProtoStaticFixtures(t)[0]
		partitionProtoReset(t, store, f)
		parent := tables.MockTableFromMeta(f.Table).(table.PhysicalTable)
		rg := f.Ranges[0]
		rg.PhysicalID = 999999
		require.ErrorContains(t, partitionProtoRunStep(ctx, store, parent, rg, 1, 1), "not found")
		partitionProtoRequireInput(t, store, f)
		partitionProtoRequireRows(t, store, f)
		report.Subtasks++
		report.Cases++
	})
	report.Checks = append(report.Checks, "missing target partition fails before mutation")
	t.Logf("production merge: backend=%s cases=%d subtasks=%d", report.Backend, report.Cases, report.Subtasks)
}

func partitionProtoStore(t *testing.T) kv.Storage {
	t.Helper()
	if addr := os.Getenv("PARTITION_PROTO_PD"); addr != "" {
		host, _, err := net.SplitHostPort(addr)
		require.NoError(t, err)
		ip := net.ParseIP(host)
		require.NotNil(t, ip)
		require.True(t, ip.IsLoopback())
		id, err := strconv.ParseUint(os.Getenv("PARTITION_PROTO_CLUSTER_ID"), 10, 64)
		require.NoError(t, err)
		require.NotZero(t, id)
		require.NotNil(t, PartitionProtoOpenStoreForTest)
		store, err := PartitionProtoOpenStoreForTest("tikv://" + addr + "?disableGC=true")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		require.Equal(t, fmt.Sprintf("tikv-%d/", id), store.UUID())
		txn, err := store.Begin()
		require.NoError(t, err)
		it, err := txn.Iter(nil, nil)
		require.NoError(t, err)
		pristine := !it.Valid()
		it.Close()
		require.NoError(t, txn.Rollback())
		require.True(t, pristine, "real replay requires an empty owned cluster; refusing to clear existing data")
		return store
	}
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func partitionProtoRunStep(ctx context.Context, store kv.Storage, parent table.PhysicalTable, rg partitionProtoRange, batch, cpu int) error {
	job := &model.Job{ID: 101, TableID: parent.Meta().ID, SchemaName: "prototype", ReorgMeta: &model.DDLReorgMeta{}}
	job.ReorgMeta.SetBatchSize(batch)
	task := &proto.Task{TaskBase: proto.TaskBase{ID: 101, Type: proto.Backfill, Step: proto.BackfillStepMergeTempIndex}}
	executor, err := newMergeTempIndexExecutor(&task.TaskBase, job, store, parent)
	if err != nil {
		return err
	}
	execute.SetFrameworkInfo(executor, task, &proto.StepResource{CPU: proto.NewAllocatable(int64(cpu)), Mem: proto.NewAllocatable(64 << 20)}, nil, nil)
	if err = executor.Init(ctx); err != nil {
		return err
	}
	defer func() { _ = executor.Cleanup(ctx) }()
	prefix := tablecodec.EncodeIndexSeekKey(rg.PhysicalID, tablecodec.TempIndexPrefix|rg.IndexID, nil)
	meta := BackfillSubTaskMeta{PhysicalTableID: rg.PhysicalID}
	meta.StartKey = prefix
	meta.EndKey = prefix.PrefixNext()
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return executor.RunSubtask(ctx, &proto.Subtask{SubtaskBase: proto.SubtaskBase{ID: 1}, Meta: raw})
}

func partitionProtoScan(t *testing.T, store kv.Storage, prefix kv.Key) []partitionProtoKV {
	t.Helper()
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() { require.NoError(t, txn.Rollback()) }()
	it, err := txn.Iter(prefix, prefix.PrefixNext())
	require.NoError(t, err)
	defer it.Close()
	var rows []partitionProtoKV
	for it.Valid() {
		rows = append(rows, partitionProtoKV{bytes.Clone(it.Key()), bytes.Clone(it.Value())})
		require.NoError(t, it.Next())
	}
	return rows
}

func partitionProtoReset(t *testing.T, store kv.Storage, f partitionProtoFixture) {
	t.Helper()
	ids := []int64{f.Table.ID}
	for _, def := range f.Table.Partition.Definitions {
		ids = append(ids, def.ID)
	}
	for _, id := range ids {
		keys := partitionProtoScan(t, store, tablecodec.EncodeTablePrefix(id))
		for start := 0; start < len(keys); start += 128 {
			require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
				for _, entry := range keys[start:min(start+128, len(keys))] {
					if err := txn.Delete(entry.Key); err != nil {
						return err
					}
				}
				return nil
			}))
		}
	}
	input := append(slices.Clone(f.Rows), f.Input...)
	for start := 0; start < len(input); start += 128 {
		require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
			for _, entry := range input[start:min(start+128, len(input))] {
				if err := txn.Set(entry.Key, entry.Value); err != nil {
					return err
				}
			}
			return nil
		}))
	}
}

func partitionProtoSorted(input []partitionProtoKV) []partitionProtoKV {
	out := slices.Clone(input)
	slices.SortFunc(out, func(a, b partitionProtoKV) int { return bytes.Compare(a.Key, b.Key) })
	return out
}
func partitionProtoRequireIndexes(t *testing.T, store kv.Storage, f partitionProtoFixture) {
	t.Helper()
	for _, rg := range f.Ranges {
		actual := partitionProtoScan(t, store, tablecodec.EncodeIndexSeekKey(rg.PhysicalID, rg.IndexID, nil))
		require.Equal(t, partitionProtoSorted(rg.Expected), partitionProtoSorted(actual))
	}
}
func partitionProtoRequireRows(t *testing.T, store kv.Storage, f partitionProtoFixture) {
	t.Helper()
	var actual []partitionProtoKV
	for _, def := range f.Table.Partition.Definitions {
		actual = append(actual, partitionProtoScan(t, store, tablecodec.GenTableRecordPrefix(def.ID))...)
	}
	require.Equal(t, partitionProtoSorted(f.Rows), partitionProtoSorted(actual))
}
func partitionProtoRequireInput(t *testing.T, store kv.Storage, f partitionProtoFixture) {
	t.Helper()
	var actual []partitionProtoKV
	for _, rg := range f.Ranges {
		actual = append(actual, partitionProtoScan(t, store, tablecodec.EncodeIndexSeekKey(rg.PhysicalID, rg.IndexID, nil))...)
		actual = append(actual, partitionProtoScan(t, store, tablecodec.EncodeIndexSeekKey(rg.PhysicalID, tablecodec.TempIndexPrefix|rg.IndexID, nil))...)
	}
	require.Equal(t, partitionProtoSorted(f.Input), partitionProtoSorted(actual))
}

// Independently decode physical nullable columns from row bytes. The prototype's
// logical row helper and its merge adapter are deliberately not used here.
func partitionProtoExpectedFromRows(t *testing.T, f partitionProtoFixture) {
	t.Helper()
	colTypes := make(map[int64]*types.FieldType)
	for _, col := range f.Table.Columns {
		colTypes[col.ID] = &col.FieldType
	}
	for _, rg := range f.Ranges {
		if f.AdapterVerified {
			require.Equal(t, partitionProtoSorted(rg.Expected), partitionProtoSorted(rg.Adapter), "production fixture is not a three-way comparison")
		}
		idx := model.FindIndexInfoByID(f.Table.Indices, rg.IndexID)
		require.NotNil(t, idx)
		var expected []partitionProtoKV
		for _, entry := range f.Rows {
			pid, h, err := tablecodec.DecodeRecordKey(entry.Key)
			require.NoError(t, err)
			if !idx.Global && pid != rg.PhysicalID {
				continue
			}
			decoded, err := tablecodec.DecodeRowToDatumMap(entry.Value, colTypes, time.UTC)
			require.NoError(t, err)
			datum := decoded[f.Table.Columns[idx.Columns[0].Offset].ID]
			var handle kv.Handle = h
			physical := pid
			if idx.Global {
				handle = kv.NewPartitionHandle(pid, h)
				physical = f.Table.ID
			}
			key, distinct, err := tablecodec.GenIndexKey(codec.NewEncoder(false), time.UTC, f.Table, idx, physical, []types.Datum{datum}, handle, nil)
			require.NoError(t, err)
			val, err := tablecodec.GenIndexValuePortal(false, time.UTC, f.Table, idx, false, distinct, false, []types.Datum{datum}, handle, pid, nil, nil)
			require.NoError(t, err)
			expected = append(expected, partitionProtoKV{key, val})
		}
		require.Equal(t, partitionProtoSorted(rg.Expected), partitionProtoSorted(expected), "fixture expected indexes disagree with physical row columns")
	}
}

func partitionProtoStaticFixtures(t *testing.T) []partitionProtoFixture {
	t.Helper()
	const tableID int64 = 9100
	pids := []int64{9101, 9102}
	type owner struct{ pid, h int64 }
	a := owner{pids[0], 7}
	var all []partitionProtoFixture
	for _, global := range []bool{false, true} {
		for _, unique := range []bool{false, true} {
			kinds := []string{"insert", "delete", "delete_only", "merging_delete"}
			if unique {
				kinds = append(kinds, "reuse", "merging_reuse", "null", "duplicate")
			}
			for _, kind := range kinds {
				tbl := &model.TableInfo{ID: tableID, Name: ast.NewCIStr("split_projection"), State: model.StatePublic, Charset: "utf8mb4", Collate: "utf8mb4_bin"}
				for offset, col := range []struct {
					id   int64
					name string
				}{{2, "value"}, {1, "route"}} {
					tbl.Columns = append(tbl.Columns, &model.ColumnInfo{ID: col.id, Name: ast.NewCIStr(col.name), Offset: offset, State: model.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLonglong)})
				}
				idx := &model.IndexInfo{ID: 1, Name: ast.NewCIStr("idx"), Global: global, Unique: unique,
					State: model.StateWriteReorganization, BackfillState: model.BackfillStateMerging,
					Columns: []*model.IndexColumn{{Name: ast.NewCIStr("value"), Offset: 0, Length: types.UnspecifiedLength}}}
				if global {
					idx.GlobalIndexVersion = model.GlobalIndexVersionV1
				}
				tbl.Indices = []*model.IndexInfo{idx}
				tbl.Partition = &model.PartitionInfo{Type: ast.PartitionTypeRange, Enable: true, Expr: "route",
					Definitions: []model.PartitionDefinition{{ID: pids[0], Name: ast.NewCIStr("p0"), LessThan: []string{"100"}},
						{ID: pids[1], Name: ast.NewCIStr("p1"), LessThan: []string{"200"}}}}
				f := partitionProtoFixture{Name: fmt.Sprintf("global%t/unique%t/%s", global, unique, kind), Version: 1, SnapshotTS: 1, Table: tbl}
				b := owner{pids[0], 8}
				if global || kind == "null" {
					b = owner{pids[1], 7}
				}
				value := types.NewIntDatum(10)
				if kind == "null" {
					value = types.Datum{}
				}
				entry := func(o owner) partitionProtoKV {
					var handle kv.Handle = kv.IntHandle(o.h)
					physical := o.pid
					if global {
						physical = tableID
						handle = kv.NewPartitionHandle(o.pid, handle)
					}
					key, distinct, err := tablecodec.GenIndexKey(codec.NewEncoder(false), time.UTC, tbl, idx, physical, []types.Datum{value}, handle, nil)
					require.NoError(t, err)
					raw, err := tablecodec.GenIndexValuePortal(false, time.UTC, tbl, idx, false, distinct, false, []types.Datum{value}, handle, o.pid, nil, nil)
					require.NoError(t, err)
					return partitionProtoKV{key, raw}
				}
				rowKV := func(o owner) partitionProtoKV {
					route := int64(10)
					if o.pid == pids[1] {
						route = 110
					}
					raw, err := tablecodec.EncodeRow(time.UTC, []types.Datum{types.NewIntDatum(route), value}, []int64{1, 2}, nil, nil, nil, &rowcodec.Encoder{Enable: true})
					require.NoError(t, err)
					return partitionProtoKV{tablecodec.EncodeRowKeyWithHandle(o.pid, kv.IntHandle(o.h)), raw}
				}
				events := make(map[string][]byte)
				event := func(o owner, deleted bool, version byte) {
					item := entry(o)
					temp := bytes.Clone(item.Key)
					tablecodec.IndexKey2TempIndexKey(temp)
					var handle kv.Handle = kv.IntHandle(o.h)
					if global {
						handle = kv.NewPartitionHandle(o.pid, handle)
					}
					elem := tablecodec.TempIndexValueElem{Value: item.Value, Handle: handle, Distinct: unique && !value.IsNull(), Delete: deleted, Global: global, KeyVer: version}
					var previous []byte
					if elem.Distinct {
						previous = events[string(temp)]
					}
					events[string(temp)] = elem.Encode(previous)
				}
				var expected []partitionProtoKV
				switch kind {
				case "insert":
					f.Rows = []partitionProtoKV{rowKV(a)}
					event(a, false, 'b')
					expected = []partitionProtoKV{entry(a)}
				case "delete":
					f.Input = []partitionProtoKV{entry(a)}
					event(a, true, 'b')
				case "delete_only":
					f.Input = []partitionProtoKV{entry(a)}
					f.Rows = []partitionProtoKV{rowKV(a)}
					event(a, true, 'd')
					expected = []partitionProtoKV{entry(a)}
				case "merging_delete":
					event(a, false, 'b')
					event(a, true, 'm')
				case "reuse", "null":
					f.Input = []partitionProtoKV{entry(a)}
					f.Rows = []partitionProtoKV{rowKV(b)}
					event(a, true, 'b')
					event(b, false, 'b')
					expected = []partitionProtoKV{entry(b)}
				case "merging_reuse":
					f.Input = []partitionProtoKV{entry(b)}
					f.Rows = []partitionProtoKV{rowKV(b)}
					event(a, true, 'b')
					event(b, false, 'b')
					event(b, false, 'm')
					expected = []partitionProtoKV{entry(b)}
				case "duplicate":
					f.Input = []partitionProtoKV{entry(a)}
					f.Rows = []partitionProtoKV{rowKV(a), rowKV(b)}
					event(b, false, 'b')
					f.Duplicate = true
				}
				for key, raw := range events {
					f.Input = append(f.Input, partitionProtoKV{[]byte(key), raw})
				}
				physicalIDs := pids
				if global {
					physicalIDs = []int64{tableID}
				}
				for _, pid := range physicalIDs {
					rg := partitionProtoRange{PhysicalID: pid, IndexID: idx.ID}
					for _, item := range expected {
						if tablecodec.DecodeTableID(item.Key) == pid {
							rg.Expected = append(rg.Expected, item)
						}
					}
					f.Ranges = append(f.Ranges, rg)
				}
				all = append(all, f)
			}
		}
	}
	return all
}

// The bound applies after decompression, including trailing whitespace/data.
// Plain JSON remains accepted for earlier prototype artifacts.
func readPartitionProtoFixtures(path string) ([]partitionProtoFixture, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var reader io.Reader = file
	if strings.HasSuffix(path, ".gz") {
		compressed, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		defer compressed.Close()
		reader = compressed
	}
	return decodePartitionProtoFixtures(reader, 1<<30)
}

func decodePartitionProtoFixtures(reader io.Reader, limit int64) ([]partitionProtoFixture, error) {
	bounded := &io.LimitedReader{R: reader, N: limit + 1}
	decoder := json.NewDecoder(bounded)
	var fixtures []partitionProtoFixture
	if err := decoder.Decode(&fixtures); err != nil {
		return nil, err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("unexpected trailing fixture JSON")
		}
		return nil, err
	}
	if bounded.N <= 0 {
		return nil, fmt.Errorf("fixture exceeds decoded size limit")
	}
	return fixtures, nil
}

func TestPartitionPrototypeFixtureCodec(t *testing.T) {
	fixture := partitionProtoStaticFixtures(t)[:1]
	raw, err := json.Marshal(fixture)
	require.NoError(t, err)
	for _, compressed := range []bool{false, true} {
		suffix := ".json"
		data := raw
		if compressed {
			suffix += ".gz"
			var buf bytes.Buffer
			writer := gzip.NewWriter(&buf)
			_, err = writer.Write(raw)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			data = buf.Bytes()
		}
		path := filepath.Join(t.TempDir(), "fixture"+suffix)
		require.NoError(t, os.WriteFile(path, data, 0o600))
		decoded, err := readPartitionProtoFixtures(path)
		require.NoError(t, err)
		require.Len(t, decoded, 1)
		require.Equal(t, fixture[0].Name, decoded[0].Name)
		require.Equal(t, fixture[0].Input, decoded[0].Input)
		if compressed {
			data = bytes.Clone(data)
			data[len(data)-1] ^= 0xff
			require.NoError(t, os.WriteFile(path, data, 0o600))
			_, err = readPartitionProtoFixtures(path)
			require.Error(t, err, "corrupt gzip must not become passing input")
		}
	}
	_, err = decodePartitionProtoFixtures(bytes.NewReader(raw), int64(len(raw)-1))
	require.Error(t, err)
	_, err = decodePartitionProtoFixtures(bytes.NewReader(append(bytes.Clone(raw), []byte(" {}")...)), int64(len(raw)+10))
	require.ErrorContains(t, err, "trailing")
	_, err = decodePartitionProtoFixtures(bytes.NewReader(append(bytes.Clone(raw), bytes.Repeat([]byte{' '}, 100)...)), int64(len(raw)+10))
	require.Error(t, err, "oversized trailing whitespace must count toward the limit")
}

// A baseline can already contain a row inserted after temp capture was armed.
// Its matching b entry must not survive successful merge and later deletion.
func TestAddIndexSkippedTempCleanup(t *testing.T) {
	ctx := context.Background()
	store := partitionProtoStore(t)
	for _, distributed := range []bool{false, true} {
		t.Run(fmt.Sprintf("distributed_%t", distributed), func(t *testing.T) {
			var f partitionProtoFixture
			for _, candidate := range partitionProtoStaticFixtures(t) {
				if candidate.Table.Indices[0].Unique && strings.HasSuffix(candidate.Name, "/insert") {
					f = candidate
					break
				}
			}
			require.NotNil(t, f.Table)
			rg := f.Ranges[0]
			item := rg.Expected[0]
			f.Input = append(f.Input, item)
			partitionProtoReset(t, store, f)
			parent := tables.MockTableFromMeta(f.Table).(table.PhysicalTable)
			run := func() {
				if distributed {
					require.NoError(t, partitionProtoRunStep(ctx, store, parent, rg, 1, 1))
					return
				}
				physical := parent.GetPartitionedTable().GetPartition(rg.PhysicalID)
				idx, err := tables.NewIndex(rg.PhysicalID, f.Table, f.Table.Indices[0])
				require.NoError(t, err)
				worker := &mergeIndexWorker{backfillCtx: &backfillCtx{ddlCtx: &ddlCtx{store: store}, table: physical, batchCnt: 1, jobContext: NewReorgContext()}, indexes: []table.Index{idx}, buffers: newTempIdxBuffers(1)}
				prefix := tablecodec.EncodeIndexSeekKey(rg.PhysicalID, tablecodec.TempIndexPrefix|rg.IndexID, nil)
				task := reorgBackfillTask{physicalTable: physical, startKey: prefix, endKey: prefix.PrefixNext()}
				for n := 0; n < 4; n++ {
					result, err := worker.BackfillData(ctx, task)
					require.NoError(t, err)
					if result.done {
						return
					}
					task.startKey = result.nextKey
				}
				t.Fatal("merge did not finish")
			}
			run()
			temp := bytes.Clone(item.Key)
			tablecodec.IndexKey2TempIndexKey(temp)
			require.Empty(t, partitionProtoScan(t, store, temp), "a skipped b record must be consumed transactionally")
			require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
				if err := txn.Delete(f.Rows[0].Key); err != nil {
					return err
				}
				return txn.Delete(item.Key)
			}))
			run()
			require.Empty(t, partitionProtoScan(t, store, item.Key), "late merge resurrected deleted index")
		})
	}
}

func TestPartitionBackfillReadProjection(t *testing.T) {
	info := partitionProtoStaticFixtures(t)[0].Table.Clone()
	pi := info.Partition
	pi.DDLAction = model.ActionReorganizePartition
	pi.DDLState = model.StateWriteReorganization
	pi.DroppingDefinitions = []model.PartitionDefinition{pi.Definitions[0]}
	pi.AddingDefinitions = []model.PartitionDefinition{
		{ID: 9201, Name: ast.NewCIStr("new0"), LessThan: []string{"50"}},
		{ID: 9202, Name: ast.NewCIStr("new1"), LessThan: []string{"100"}},
	}
	tbl := tables.MockTableFromMeta(info)
	require.NotNil(t, tbl)
	before, err := json.Marshal(tbl.Meta())
	require.NoError(t, err)
	projected, err := projectPartitionBackfillTable(tbl, []int64{9201, 9202, 9102}, []int64{1})
	require.NoError(t, err)
	require.Equal(t, []int64{9201, 9202, 9102}, []int64{projected.Meta().Partition.Definitions[0].ID, projected.Meta().Partition.Definitions[1].ID, projected.Meta().Partition.Definitions[2].ID})
	require.Nil(t, projected.GetPartitionedTable().GetPartition(9101))
	require.NotNil(t, projected.GetPartitionedTable().GetPartition(9201))
	// Run the production step against an actual AddingDefinition selected by
	// the read projection, rather than rewriting the fixture's public schema.
	store := partitionProtoStore(t)
	fixture := partitionProtoStaticFixtures(t)[0]
	entry := fixture.Ranges[0].Expected[0]
	entry.Key = tablecodec.EncodeIndexSeekKey(9201, 1, entry.Key[19:])
	temp := bytes.Clone(entry.Key)
	tablecodec.IndexKey2TempIndexKey(temp)
	value := (&tablecodec.TempIndexValueElem{Value: entry.Value, KeyVer: tablecodec.TempIndexKeyTypeBackfill}).Encode(nil)
	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
		if err := txn.Set(tablecodec.EncodeRowKeyWithHandle(9201, kv.IntHandle(7)), fixture.Rows[0].Value); err != nil {
			return err
		}
		return txn.Set(temp, value)
	}))
	require.NoError(t, partitionProtoRunStep(context.Background(), store, projected.(table.PhysicalTable), partitionProtoRange{PhysicalID: 9201, IndexID: 1}, 1, 1))
	require.Equal(t, []partitionProtoKV{entry}, partitionProtoScan(t, store, tablecodec.EncodeIndexSeekKey(9201, 1, nil)))
	var legacy BackfillTaskMeta
	require.NoError(t, json.Unmarshal([]byte(`{"ele_ids":[1]}`), &legacy))
	require.Empty(t, legacy.PartitionIDs)
	local, err := projectPartitionBackfillTable(tbl, []int64{9201, 9202}, []int64{1})
	require.NoError(t, err)
	require.Len(t, local.Meta().Partition.Definitions, 2)
	for _, pids := range [][]int64{nil, {9101}, {123456}, {9201, 9201}} {
		_, err = projectPartitionBackfillTable(tbl, pids, []int64{1})
		require.Error(t, err)
	}
	for _, ids := range [][]int64{nil, {999}, {1, 1}} {
		_, err = projectPartitionBackfillTable(tbl, []int64{9201, 9202}, ids)
		require.Error(t, err)
	}
	globals := info.Clone()
	for _, id := range []int64{10, 11, 12} {
		idx := globals.Indices[0].Clone()
		idx.ID = id
		idx.Global = true
		idx.GlobalIndexVersion = model.GlobalIndexVersionV1
		globals.Indices = append(globals.Indices, idx)
	}
	globals.Partition.DDLChangedIndex = map[int64]bool{10: false, 11: true}
	globalTable := tables.MockTableFromMeta(globals)
	require.NotNil(t, globalTable)
	_, err = projectPartitionBackfillTable(globalTable, []int64{9201, 9202, 9102}, []int64{11})
	require.NoError(t, err)
	for _, id := range []int64{10, 12} {
		_, err = projectPartitionBackfillTable(globalTable, []int64{9201, 9202, 9102}, []int64{id})
		require.Error(t, err)
	}
	after, err := json.Marshal(tbl.Meta())
	require.NoError(t, err)
	require.Equal(t, before, after, "private projection mutated public metadata")
	projected.Meta().Partition.Definitions[0].LessThan[0] = "25"
	require.Equal(t, "50", tbl.Meta().Partition.AddingDefinitions[0].LessThan[0], "projection aliases original bounds")
}
