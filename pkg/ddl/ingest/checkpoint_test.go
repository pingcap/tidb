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

package ingest_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

func createDummyFile(t *testing.T, folder string) {
	f, err := os.Create(filepath.Join(folder, "test-file"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

type mockGetTSClient struct {
	pd.Client

	pts int64
	lts int64
}

func (m *mockGetTSClient) GetTS(context.Context) (int64, int64, error) {
	p, l := m.pts, m.lts
	m.pts++
	m.lts++
	return p, l, nil
}

func TestCheckpointManagerUpdateReorg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("insert into mysql.tidb_ddl_reorg (job_id, ele_id, ele_type) values (1, 1, '_idx_');")
	rs := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 8, 8, 0)
	ctx := context.Background()
	sessPool := session.NewSessionPool(rs)
	tmpFolder := t.TempDir()
	createDummyFile(t, tmpFolder)
	expectedTS := oracle.ComposeTS(13, 35)
	mgr, err := ingest.NewCheckpointManager(ctx, sessPool, 1, 1, tmpFolder, &mockGetTSClient{pts: 12, lts: 34}, kv.Key{'1'})
	require.NoError(t, err)
	defer mgr.Close()

	mgr.FinishChunk(kv.KeyRange{
		StartKey: []byte{'1'},
		EndKey:   []byte{'2'},
	}, 2, []byte{'1'})
	require.NoError(t, mgr.AdvanceWatermark())
	mgr.Close() // Wait the global checkpoint to be updated to the reorg table.
	r, err := tk.Exec("select reorg_meta from mysql.tidb_ddl_reorg where job_id = 1 and ele_id = 1;")
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(context.Background(), req)
	require.NoError(t, err)
	row := req.GetRow(0)
	require.Equal(t, 1, row.Len())
	reorgMetaRaw := row.GetBytes(0)
	reorgMeta := &ingest.JobReorgMeta{}
	require.NoError(t, json.Unmarshal(reorgMetaRaw, reorgMeta))
	require.Nil(t, r.Close())
	require.Equal(t, 2, reorgMeta.Checkpoint.GlobalKeyCount)
	require.EqualValues(t, "2\x00", reorgMeta.Checkpoint.GlobalSyncKey)
	require.EqualValues(t, expectedTS, reorgMeta.Checkpoint.TS)
}

func TestCheckpointManagerResumeReorg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	reorgMeta := &ingest.JobReorgMeta{
		Checkpoint: &ingest.ReorgCheckpoint{
			GlobalSyncKey:  kv.Key("2\x00"),
			GlobalKeyCount: 2,
			PhysicalID:     1,
			InstanceAddr:   ingest.InstanceAddr(),
			Version:        1,
			TS:             123456,
		},
	}
	reorgMetaRaw, err := json.Marshal(reorgMeta)
	require.NoError(t, err)
	tk.MustExec("insert into mysql.tidb_ddl_reorg (job_id, ele_id, ele_type, reorg_meta) values (1, 1, '_idx_', ?);", reorgMetaRaw)
	rs := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 8, 8, 0)
	ctx := context.Background()
	sessPool := session.NewSessionPool(rs)
	tmpFolder := t.TempDir()
	mgr, err := ingest.NewCheckpointManager(ctx, sessPool, 1, 1, tmpFolder, nil, kv.Key{'1'})
	require.NoError(t, err)
	defer mgr.Close()
	keyCnt, globalNextKey := mgr.TotalKeyCount(), mgr.NextStartKey()
	require.Equal(t, 2, keyCnt)
	require.EqualValues(t, "2\x00", globalNextKey)
	require.EqualValues(t, 123456, mgr.GetImportTS())

	// delete folder contents
	createDummyFile(t, tmpFolder)
	mgr2, err := ingest.NewCheckpointManager(ctx, sessPool, 1, 1, tmpFolder, nil, kv.Key{'1'})
	require.NoError(t, err)
	defer mgr2.Close()
	keyCnt, globalNextKey = mgr2.TotalKeyCount(), mgr2.NextStartKey()
	require.Equal(t, 2, keyCnt)
	require.EqualValues(t, "2\x00", globalNextKey)
	require.EqualValues(t, 123456, mgr2.GetImportTS())
}

func TestCanPromoteWatermark(t *testing.T) {
	k0 := kv.Key("0")
	k1 := kv.Key("1")
	k2 := kv.Key("2")
	k3 := kv.Key("3")

	tests := []struct {
		name string
		r    ingest.ProcessedRange
		low  kv.Key
		want bool
	}{
		{
			name: "start_leq_low",
			r: ingest.ProcessedRange{
				StartKey: k0,
				EndKey:   k1,
			},
			low:  k1,
			want: true, // StartKey (0) <= low (1)
		},
		{
			name: "start_gt_low",
			r: ingest.ProcessedRange{
				StartKey: k2,
				EndKey:   k3,
			},
			low:  k1,
			want: false, // StartKey (2) > low (1)
		},
		{
			name: "prevtail_leq_low",
			r: ingest.ProcessedRange{
				StartKey:    k2,
				EndKey:      k3,
				PrevTailKey: k1,
			},
			low:  k1,
			want: true, // PrevTailKey (1) <= low (1)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ingest.CanPromoteWatermark(tt.r, tt.low)
			require.Equal(t, tt.want, got)
		})
	}
}

func k(s string) kv.Key { return kv.Key(s) }

// helper to stringify ranges for easier assertions in error messages
func rangesToStr(rs []ingest.ProcessedRange) string {
	out := ""
	for i, r := range rs {
		if i > 0 {
			out += ","
		}
		out += "[" + hex.EncodeToString(r.StartKey) + ":" + hex.EncodeToString(r.EndKey) + "]"
	}
	return out
}

// helper to build ranges easily
func makeRanges(keys ...string) []ingest.ProcessedRange {
	out := make([]ingest.ProcessedRange, 0, len(keys)/2)
	for i := 0; i+1 < len(keys); i += 2 {
		out = append(out, ingest.ProcessedRange{StartKey: k(keys[i]), EndKey: k(keys[i+1])})
	}
	return out
}

func TestMergeAndCompactRanges(t *testing.T) {
	tests := []struct {
		name string
		in   []ingest.ProcessedRange
		want []ingest.ProcessedRange
	}{
		{
			name: "filter_invalid",
			in: []ingest.ProcessedRange{
				{StartKey: nil, EndKey: nil},
				{StartKey: k("a"), EndKey: k("")},
				{StartKey: k("c"), EndKey: k("b")},
			},
			want: nil,
		},
		{
			name: "no_merge_sorted",
			in:   makeRanges("01", "02", "03", "04"),
			want: makeRanges("01", "02", "03", "04"),
		},
		{
			name: "overlap_merge",
			in:   makeRanges("01", "05", "03", "06"),
			want: makeRanges("01", "06"),
		},
		{
			name: "stitch_prev_tail",
			in: []ingest.ProcessedRange{
				{StartKey: k("01"), EndKey: k("02")},
				{StartKey: k("03"), EndKey: k("04"), PrevTailKey: k("02")},
			},
			want: []ingest.ProcessedRange{{StartKey: k("01"), EndKey: k("04")}},
		},
		{
			name: "stitch_prev_tail_next",
			in: []ingest.ProcessedRange{
				{StartKey: k("01"), EndKey: k("02").Next()},
				{StartKey: k("03"), EndKey: k("04"), PrevTailKey: k("02")},
			},
			want: []ingest.ProcessedRange{{StartKey: k("01"), EndKey: k("04")}},
		},
		{
			name: "unsorted_input_sorted_then_merged",
			in: []ingest.ProcessedRange{
				{StartKey: k("10"), EndKey: k("12"), PrevTailKey: k("09")},
				{StartKey: k("02"), EndKey: k("05"), PrevTailKey: k("01")},
				{StartKey: k("06"), EndKey: k("07"), PrevTailKey: k("05")},
			},
			want: []ingest.ProcessedRange{{StartKey: k("02"), EndKey: k("07")}, {StartKey: k("10"), EndKey: k("12")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ingest.MergeAndCompactRanges(tt.in)
			require.Equal(t, len(tt.want), len(got), "count mismatch, got %s", rangesToStr(got))
			for i := range tt.want {
				require.Equal(t, tt.want[i].StartKey, got[i].StartKey, "start mismatch at %d", i)
				require.Equal(t, tt.want[i].EndKey, got[i].EndKey, "end mismatch at %d", i)
			}
		})
	}
}

func TestPruneRanges(t *testing.T) {
	tests := []struct {
		name string
		in   []ingest.ProcessedRange
		low  kv.Key
		want []ingest.ProcessedRange
	}{
		{name: "empty_low_returns_input", in: makeRanges("a", "b"), low: k(""), want: makeRanges("a", "b")},
		{name: "empty_input_returns_input", in: nil, low: k("c"), want: nil},
		{name: "none_below_low", in: makeRanges("d", "e", "f", "g"), low: k("c"), want: makeRanges("d", "e", "f", "g")},
		{name: "some_below_low", in: makeRanges("a", "b", "c", "d", "e", "f"), low: k("c"), want: makeRanges("c", "d", "e", "f")},
		{name: "all_below_low", in: makeRanges("a", "b", "b", "c"), low: k("d"), want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ingest.PruneRanges(tt.in, tt.low)
			require.Equal(t, len(tt.want), len(got), "count mismatch: got %d want %d", len(got), len(tt.want))
			for i := range tt.want {
				require.Equal(t, tt.want[i].StartKey, got[i].StartKey)
				require.Equal(t, tt.want[i].EndKey, got[i].EndKey)
			}
		})
	}
}

func TestSubtractOne(t *testing.T) {
	a := kv.KeyRange{StartKey: k("a"), EndKey: k("d").Next()}

	tests := []struct {
		name string
		b    ingest.ProcessedRange // closed [Start, End]
		want []kv.KeyRange
	}{
		{name: "non_overlap_before", b: ingest.ProcessedRange{StartKey: k("0"), EndKey: k("0")}, want: []kv.KeyRange{a}},
		{name: "touch_end", b: ingest.ProcessedRange{StartKey: k("e"), EndKey: k("f")}, want: []kv.KeyRange{a}},
		{name: "middle_cut", b: ingest.ProcessedRange{StartKey: k("b"), EndKey: k("c")}, want: []kv.KeyRange{{StartKey: k("a"), EndKey: k("b")}, {StartKey: k("c").Next(), EndKey: k("d").Next()}}},
		{name: "cover_all", b: ingest.ProcessedRange{StartKey: k("a"), EndKey: k("e")}, want: nil},
		{name: "left_overlap", b: ingest.ProcessedRange{StartKey: k("0"), EndKey: k("b")}, want: []kv.KeyRange{{StartKey: k("b").Next(), EndKey: k("d").Next()}}},
		{name: "right_overlap", b: ingest.ProcessedRange{StartKey: k("d"), EndKey: k("z")}, want: []kv.KeyRange{{StartKey: k("a"), EndKey: k("d")}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ingest.SubtractOne(a, tt.b)
			require.Equal(t, len(tt.want), len(got), "count mismatch for %s", tt.name)
			for i := range tt.want {
				require.Equal(t, tt.want[i].StartKey, got[i].StartKey)
				require.Equal(t, tt.want[i].EndKey, got[i].EndKey)
			}
		})
	}
}

func TestSubtractRanges(t *testing.T) {
	a := kv.KeyRange{StartKey: k("a"), EndKey: k("d").Next()}

	tests := []struct {
		name     string
		input    []kv.KeyRange
		imported []ingest.ProcessedRange
		want     []kv.KeyRange
	}{
		{name: "no_imported", input: []kv.KeyRange{a}, imported: nil, want: []kv.KeyRange{a}},
		{name: "middle_cut", input: []kv.KeyRange{a}, imported: makeRanges("b", "c"), want: []kv.KeyRange{{StartKey: k("a"), EndKey: k("b")}, {StartKey: k("c").Next(), EndKey: k("d").Next()}}},
		{name: "cover_all", input: []kv.KeyRange{a}, imported: makeRanges("a", "e"), want: nil},
		{name: "multiple_inputs", input: []kv.KeyRange{{StartKey: k("a"), EndKey: k("f").Next()}, {StartKey: k("m"), EndKey: k("z").Next()}}, imported: makeRanges("b", "d", "o", "q"), want: []kv.KeyRange{{StartKey: k("a"), EndKey: k("b")}, {StartKey: k("d").Next(), EndKey: k("f").Next()}, {StartKey: k("m"), EndKey: k("o")}, {StartKey: k("q").Next(), EndKey: k("z").Next()}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ingest.SubtractRanges(tt.input, tt.imported)
			require.Equal(t, len(tt.want), len(got), "count mismatch for %s", tt.name)
			for i := range tt.want {
				require.Equal(t, tt.want[i].StartKey, got[i].StartKey)
				require.Equal(t, tt.want[i].EndKey, got[i].EndKey)
			}
		})
	}
}
