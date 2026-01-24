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

package external

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func TestSeekPropsOffsets(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()

	rc1 := &rangePropertiesCollector{
		props: []*rangeProperty{
			{
				firstKey: []byte("key1"),
				offset:   10,
			},
			{
				firstKey: []byte("key3"),
				offset:   30,
			},
			{
				firstKey: []byte("key5"),
				offset:   50,
			},
		},
	}
	file1 := "/test1"
	w1, err := store.Create(ctx, file1, nil)
	require.NoError(t, err)
	_, err = w1.Write(ctx, rc1.encode())
	require.NoError(t, err)
	err = w1.Close(ctx)
	require.NoError(t, err)

	rc2 := &rangePropertiesCollector{
		props: []*rangeProperty{
			{
				firstKey: []byte("key2"),
				offset:   20,
			},
			{
				firstKey: []byte("key4"),
				offset:   40,
			},
		},
	}
	file2 := "/test2"
	w2, err := store.Create(ctx, file2, nil)
	require.NoError(t, err)
	_, err = w2.Write(ctx, rc2.encode())
	require.NoError(t, err)
	err = w2.Close(ctx)
	require.NoError(t, err)

	got, err := seekPropsOffsets(ctx, []kv.Key{[]byte("key2.5")}, []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{10, 20}}, got)

	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key2.5"), []byte("key2.6")}, []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{10, 20}, {10, 20}}, got)

	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key3")}, []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{30, 20}}, got)

	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key2.5"), []byte("key3")}, []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{10, 20}, {30, 20}}, got)

	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key0")}, []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{0, 0}}, got)

	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key1")}, []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{10, 0}}, got)

	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key0"), []byte("key1")}, []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{0, 0}, {10, 0}}, got)

	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key999")}, []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{50, 40}}, got)

	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key999"), []byte("key999")}, []string{file1, file2}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{50, 40}, {50, 40}}, got)

	file3 := "/test3"
	w3, err := store.Create(ctx, file3, nil)
	require.NoError(t, err)
	err = w3.Close(ctx)
	require.NoError(t, err)

	file4 := "/test4"
	w4, err := store.Create(ctx, file4, nil)
	require.NoError(t, err)
	_, err = w4.Write(ctx, rc1.encode())
	require.NoError(t, err)
	err = w4.Close(ctx)
	require.NoError(t, err)
	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key3")}, []string{file1, file2, file3, file4}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{30, 20, 0, 30}}, got)

	got, err = seekPropsOffsets(ctx, []kv.Key{[]byte("key3"), []byte("key999")}, []string{file1, file2, file3, file4}, store)
	require.NoError(t, err)
	require.Equal(t, [][]uint64{{30, 20, 0, 30}, {50, 40, 0, 50}}, got)
}

func TestGetAllFileNames(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()
	w := NewWriterBuilder().
		SetMemorySizeLimit(10*(lengthBytes*2+2)).
		SetBlockSize(10*(lengthBytes*2+2)).
		SetPropSizeDistance(5).
		SetPropKeysDistance(3).
		Build(store, "/subtask", "0")

	keys := make([][]byte, 0, 30)
	values := make([][]byte, 0, 30)
	for i := 0; i < 30; i++ {
		keys = append(keys, []byte{byte(i)})
		values = append(values, []byte{byte(i)})
	}

	for i, key := range keys {
		err := w.WriteRow(ctx, key, values[i], nil)
		require.NoError(t, err)
	}
	err := w.Close(ctx)
	require.NoError(t, err)

	w2 := NewWriterBuilder().
		SetMemorySizeLimit(10*(lengthBytes*2+2)).
		SetBlockSize(10*(lengthBytes*2+2)).
		SetPropSizeDistance(5).
		SetPropKeysDistance(3).
		Build(store, "/subtask", "3")
	for i, key := range keys {
		err := w2.WriteRow(ctx, key, values[i], nil)
		require.NoError(t, err)
	}
	require.NoError(t, err)
	err = w2.Close(ctx)
	require.NoError(t, err)

	w3 := NewWriterBuilder().
		SetMemorySizeLimit(10*(lengthBytes*2+2)).
		SetBlockSize(10*(lengthBytes*2+2)).
		SetPropSizeDistance(5).
		SetPropKeysDistance(3).
		Build(store, "/subtask", "12")
	for i, key := range keys {
		err := w3.WriteRow(ctx, key, values[i], nil)
		require.NoError(t, err)
	}
	err = w3.Close(ctx)
	require.NoError(t, err)

	dataFiles, statFiles, err := GetAllFileNames(ctx, store, "/subtask")
	require.NoError(t, err)
	require.Equal(t, []string{
		"/subtask/0_stat/0", "/subtask/0_stat/1", "/subtask/0_stat/2",
		"/subtask/12_stat/0", "/subtask/12_stat/1", "/subtask/12_stat/2",
		"/subtask/3_stat/0", "/subtask/3_stat/1", "/subtask/3_stat/2",
	}, statFiles)
	require.Equal(t, []string{
		"/subtask/0/0", "/subtask/0/1", "/subtask/0/2",
		"/subtask/12/0", "/subtask/12/1", "/subtask/12/2",
		"/subtask/3/0", "/subtask/3/1", "/subtask/3/2",
	}, dataFiles)
}

func TestCleanUpFiles(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()
	w := NewWriterBuilder().
		SetMemorySizeLimit(10*(lengthBytes*2+2)).
		SetBlockSize(10*(lengthBytes*2+2)).
		SetPropSizeDistance(5).
		SetPropKeysDistance(3).
		Build(store, "/subtask", "0")
	keys := make([][]byte, 0, 30)
	values := make([][]byte, 0, 30)
	for i := 0; i < 30; i++ {
		keys = append(keys, []byte{byte(i)})
		values = append(values, []byte{byte(i)})
	}
	for i, key := range keys {
		err := w.WriteRow(ctx, key, values[i], nil)
		require.NoError(t, err)
	}
	err := w.Close(ctx)
	require.NoError(t, err)

	dataFiles, statFiles, err := GetAllFileNames(ctx, store, "/subtask")
	require.NoError(t, err)
	require.Equal(t, []string{
		"/subtask/0_stat/0", "/subtask/0_stat/1", "/subtask/0_stat/2",
	}, statFiles)
	require.Equal(t, []string{
		"/subtask/0/0", "/subtask/0/1", "/subtask/0/2",
	}, dataFiles)

	require.NoError(t, CleanUpFiles(ctx, store, "/subtask"))

	dataFiles, statFiles, err = GetAllFileNames(ctx, store, "/subtask")
	require.NoError(t, err)
	require.Equal(t, []string(nil), statFiles)
	require.Equal(t, []string(nil), dataFiles)
}

func TestGetMaxOverlapping(t *testing.T) {
	// [1, 3), [2, 4)
	points := []Endpoint{
		{Key: []byte{1}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{3}, Tp: ExclusiveEnd, Weight: 1},
		{Key: []byte{2}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{4}, Tp: ExclusiveEnd, Weight: 1},
	}
	require.EqualValues(t, 2, GetMaxOverlapping(points))
	// [1, 3), [2, 4), [3, 5)
	points = []Endpoint{
		{Key: []byte{1}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{3}, Tp: ExclusiveEnd, Weight: 1},
		{Key: []byte{2}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{4}, Tp: ExclusiveEnd, Weight: 1},
		{Key: []byte{3}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{5}, Tp: ExclusiveEnd, Weight: 1},
	}
	require.EqualValues(t, 2, GetMaxOverlapping(points))
	// [1, 3], [2, 4], [3, 5]
	points = []Endpoint{
		{Key: []byte{1}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{3}, Tp: InclusiveEnd, Weight: 1},
		{Key: []byte{2}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{4}, Tp: InclusiveEnd, Weight: 1},
		{Key: []byte{3}, Tp: InclusiveStart, Weight: 1},
		{Key: []byte{5}, Tp: InclusiveEnd, Weight: 1},
	}
	require.EqualValues(t, 3, GetMaxOverlapping(points))
}

func TestSortedKVMeta(t *testing.T) {
	summary := []*WriterSummary{
		{
			Min:       []byte("a"),
			Max:       []byte("b"),
			TotalSize: 123,
			MultipleFilesStats: []MultipleFilesStat{
				{
					Filenames: [][2]string{
						{"f1", "stat1"},
						{"f2", "stat2"},
					},
				},
			},
		},
		{
			Min:       []byte("x"),
			Max:       []byte("y"),
			TotalSize: 177,
			MultipleFilesStats: []MultipleFilesStat{
				{
					Filenames: [][2]string{
						{"f3", "stat3"},
						{"f4", "stat4"},
					},
				},
			},
		},
	}
	meta0 := NewSortedKVMeta(summary[0])
	require.Equal(t, []byte("a"), meta0.StartKey)
	require.Equal(t, []byte{'b', 0}, meta0.EndKey)
	require.Equal(t, uint64(123), meta0.TotalKVSize)
	require.Equal(t, summary[0].MultipleFilesStats, meta0.MultipleFilesStats)
	meta1 := NewSortedKVMeta(summary[1])
	require.Equal(t, []byte("x"), meta1.StartKey)
	require.Equal(t, []byte{'y', 0}, meta1.EndKey)
	require.Equal(t, uint64(177), meta1.TotalKVSize)
	require.Equal(t, summary[1].MultipleFilesStats, meta1.MultipleFilesStats)

	meta0.MergeSummary(summary[1])
	require.Equal(t, []byte("a"), meta0.StartKey)
	require.Equal(t, []byte{'y', 0}, meta0.EndKey)
	require.Equal(t, uint64(300), meta0.TotalKVSize)
	mergedStats := append([]MultipleFilesStat{}, summary[0].MultipleFilesStats...)
	mergedStats = append(mergedStats, summary[1].MultipleFilesStats...)
	require.Equal(t, mergedStats, meta0.MultipleFilesStats)

	meta00 := NewSortedKVMeta(summary[0])
	meta00.Merge(meta1)
	require.Equal(t, meta0, meta00)
}

func TestKeyMinMax(t *testing.T) {
	require.Equal(t, []byte("a"), BytesMin([]byte("a"), []byte("b")))
	require.Equal(t, []byte("a"), BytesMin([]byte("b"), []byte("a")))

	require.Equal(t, []byte("b"), BytesMax([]byte("a"), []byte("b")))
	require.Equal(t, []byte("b"), BytesMax([]byte("b"), []byte("a")))
}
<<<<<<< HEAD
=======

func TestMarshalFields(t *testing.T) {
	type Example struct {
		X string
		Y int `json:"y"`
	}

	testCases := []struct {
		name            string
		instInternal    any
		instExternal    any
		expectedMarshal string
		expectedOmit    string
	}{
		{
			name: "non-public",
			instInternal: struct {
				a int
			}{a: 42},
			instExternal: struct {
				a int `external:"true"`
			}{a: 42},
			expectedMarshal: `{}`,
			expectedOmit:    `{}`,
		},
		{
			name: "-",
			instInternal: struct {
				A string `json:"-"`
			}{A: "42"},
			instExternal: struct {
				A string `json:"-" external:"true"`
			}{A: "42"},
			expectedMarshal: `{}`,
			expectedOmit:    `{}`,
		},
		{
			name: "omitempty",
			instInternal: struct {
				A string `json:"a,omitempty"`
			}{A: ""},
			instExternal: struct {
				A string `json:"a,omitempty" external:"true"`
			}{A: ""},
			expectedMarshal: `{}`,
			expectedOmit:    `{}`,
		},
		{
			name: "int",
			instInternal: struct {
				A int
			}{A: 42},
			instExternal: struct {
				A int `external:"true"`
			}{A: 42},
			expectedMarshal: `{"A":42}`,
			expectedOmit:    `{}`,
		},
		{
			name: "rename",
			instInternal: struct {
				A string `json:"a"`
			}{A: "42"},
			instExternal: struct {
				A string `json:"a" external:"true"`
			}{A: "42"},
			expectedMarshal: `{"a":"42"}`,
			expectedOmit:    `{}`,
		},
		{
			name: "embed",
			instInternal: struct {
				Example
			}{Example: Example{X: "42", Y: 42}},
			instExternal: struct {
				Example `external:"true"`
			}{Example: Example{X: "42", Y: 42}},
			expectedMarshal: `{"X":"42","y":42}`,
			expectedOmit:    `{}`,
		},
		{
			name: "nested",
			instInternal: struct {
				Example Example
			}{Example: Example{X: "42", Y: 42}},
			instExternal: struct {
				Example Example `external:"true"`
			}{Example: Example{X: "42", Y: 42}},
			expectedMarshal: `{"Example":{"X":"42","y":42}}`,
			expectedOmit:    `{}`,
		},
		{
			name: "inline",
			instInternal: struct {
				Example `json:",inline"`
			}{Example: Example{X: "42", Y: 42}},
			instExternal: struct {
				Example `json:",inline" external:"true"`
			}{Example: Example{X: "42", Y: 42}},
			expectedMarshal: `{"X":"42","y":42}`,
			expectedOmit:    `{}`,
		},
		{
			name: "slice",
			instInternal: struct {
				A []string
			}{A: []string{"42"}},
			instExternal: struct {
				A []string `external:"true"`
			}{A: []string{"42"}},
			expectedMarshal: `{"A":["42"]}`,
			expectedOmit:    `{}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := marshalInternalFields(tc.instInternal)
			require.NoError(t, err)
			require.Equal(t, tc.expectedMarshal, string(data))

			data, err = marshalExternalFields(tc.instInternal)
			require.NoError(t, err)
			require.Equal(t, tc.expectedOmit, string(data))

			data, err = marshalInternalFields(tc.instExternal)
			require.NoError(t, err)
			require.Equal(t, tc.expectedOmit, string(data))

			data, err = marshalExternalFields(tc.instExternal)
			require.NoError(t, err)
			require.Equal(t, tc.expectedMarshal, string(data))
		})
	}
}

func TestReadWriteJSON(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()

	type testStruct struct {
		BaseExternalMeta
		X int
		Y string `external:"true"`
	}
	ts := &testStruct{
		X: 42,
		Y: "test",
	}

	data, err := ts.BaseExternalMeta.Marshal(ts)
	require.NoError(t, err)
	js, err := json.Marshal(ts)
	require.NoError(t, err)
	require.Equal(t, data, js)

	ts.BaseExternalMeta.ExternalPath = "/test"
	err = ts.WriteJSONToExternalStorage(ctx, store, ts)
	require.NoError(t, err)
	data, err = ts.BaseExternalMeta.Marshal(ts)
	require.NoError(t, err)

	var ts1 testStruct
	err = ts1.ReadJSONFromExternalStorage(ctx, store, &ts1)
	require.NoError(t, err)
	require.NotEqual(t, ts, ts1)

	var ts2 testStruct
	err = json.Unmarshal(data, &ts2)
	require.NoError(t, err)
	require.NotEqual(t, ts, ts2)

	err = ts2.ReadJSONFromExternalStorage(ctx, store, &ts2)
	require.NoError(t, err)
	require.Equal(t, *ts, ts2)
}

func TestExternalMetaPath(t *testing.T) {
	require.Equal(t, "1/plan/merge-sort/1/meta.json", PlanMetaPath(1, "merge-sort", 1))
	require.Equal(t, "2/plan/ingest/3/meta.json", PlanMetaPath(2, "ingest", 3))

	require.Equal(t, "1/1/meta.json", SubtaskMetaPath(1, 1))
	require.Equal(t, "2/3/meta.json", SubtaskMetaPath(2, 3))
}

func TestRemoveDuplicates(t *testing.T) {
	valGetter := func(e *int) []byte {
		return []byte{byte(*e)}
	}
	cases := []struct {
		in   []int
		out  []int
		dups []int
	}{
		// no duplicates
		{in: []int{}, out: []int{}, dups: []int{}},
		{in: []int{1}, out: []int{1}, dups: []int{}},
		{in: []int{1, 2}, out: []int{1, 2}, dups: []int{}},
		{in: []int{1, 2, 3}, out: []int{1, 2, 3}, dups: []int{}},
		{in: []int{1, 2, 3, 4, 5}, out: []int{1, 2, 3, 4, 5}, dups: []int{}},
		// duplicates at beginning
		{in: []int{1, 1}, out: []int{}, dups: []int{1, 1}},
		{in: []int{1, 1, 1}, out: []int{}, dups: []int{1, 1, 1}},
		{in: []int{1, 1, 2, 3}, out: []int{2, 3}, dups: []int{1, 1}},
		{in: []int{1, 1, 1, 2, 3}, out: []int{2, 3}, dups: []int{1, 1, 1}},
		// duplicates in middle
		{in: []int{1, 2, 2, 3}, out: []int{1, 3}, dups: []int{2, 2}},
		{in: []int{1, 2, 2, 2, 3}, out: []int{1, 3}, dups: []int{2, 2, 2}},
		{in: []int{1, 2, 2, 2, 3, 3, 4}, out: []int{1, 4}, dups: []int{2, 2, 2, 3, 3}},
		{in: []int{1, 2, 2, 2, 3, 3, 4, 4, 5}, out: []int{1, 5}, dups: []int{2, 2, 2, 3, 3, 4, 4}},
		{in: []int{1, 2, 2, 2, 3, 4, 4, 5}, out: []int{1, 3, 5}, dups: []int{2, 2, 2, 4, 4}},
		{in: []int{1, 2, 2, 2, 3, 4, 4, 5, 5, 6, 7, 8, 8, 9}, out: []int{1, 3, 6, 7, 9}, dups: []int{2, 2, 2, 4, 4, 5, 5, 8, 8}},
		// duplicates at end
		{in: []int{1, 2, 3, 3}, out: []int{1, 2}, dups: []int{3, 3}},
		{in: []int{1, 2, 3, 3, 3}, out: []int{1, 2}, dups: []int{3, 3, 3}},
		// mixing
		{in: []int{1, 1, 2, 3, 3, 4}, out: []int{2, 4}, dups: []int{1, 1, 3, 3}},
		{in: []int{1, 2, 3, 3, 4, 4}, out: []int{1, 2}, dups: []int{3, 3, 4, 4}},
		{in: []int{1, 1, 2, 3, 4, 4}, out: []int{2, 3}, dups: []int{1, 1, 4, 4}},
		{in: []int{1, 1, 2, 2, 3, 3}, out: []int{}, dups: []int{1, 1, 2, 2, 3, 3}},
		{in: []int{1, 1, 2, 2, 2, 3, 3}, out: []int{}, dups: []int{1, 1, 2, 2, 2, 3, 3}},
		{in: []int{1, 1, 2, 2, 2, 3, 3, 4, 4}, out: []int{}, dups: []int{1, 1, 2, 2, 2, 3, 3, 4, 4}},
		{in: []int{1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 5}, out: []int{}, dups: []int{1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 5}},
		{in: []int{1, 1, 2, 2, 2, 3, 4, 4, 5, 5}, out: []int{3}, dups: []int{1, 1, 2, 2, 2, 4, 4, 5, 5}},
		{in: []int{1, 1, 2, 2, 2, 3, 4, 4, 5, 5, 6, 7, 8, 8, 9, 9}, out: []int{3, 6, 7}, dups: []int{1, 1, 2, 2, 2, 4, 4, 5, 5, 8, 8, 9, 9}},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			require.True(t, slices.IsSorted(c.in))
			require.True(t, slices.IsSorted(c.out))
			require.True(t, slices.IsSorted(c.dups))
			require.Equal(t, len(c.dups), len(c.in)-len(c.out))
			tmpIn := make([]int, len(c.in))
			copy(tmpIn, c.in)
			out, dups, dupCnt := removeDuplicates(tmpIn, valGetter, true)
			require.EqualValues(t, c.out, out)
			require.EqualValues(t, c.dups, dups)
			require.Equal(t, dupCnt, len(dups))

			tmpIn = make([]int, len(c.in))
			copy(tmpIn, c.in)
			out, dups, dupCnt = removeDuplicates(tmpIn, valGetter, false)
			require.EqualValues(t, c.out, out)
			require.Empty(t, dups)
			require.Equal(t, dupCnt, len(c.dups))
		})
	}
}

func TestRemoveDuplicatesMoreThan2(t *testing.T) {
	valGetter := func(e *int) []byte {
		return []byte{byte(*e)}
	}
	cases := []struct {
		in    []int
		out   []int
		dups  []int
		total int
	}{
		// no duplicates
		{in: []int{}, out: []int{}, dups: []int{}, total: 0},
		{in: []int{1}, out: []int{1}, dups: []int{}, total: 0},
		{in: []int{1, 2}, out: []int{1, 2}, dups: []int{}, total: 0},
		{in: []int{1, 2, 3}, out: []int{1, 2, 3}, dups: []int{}, total: 0},
		{in: []int{1, 2, 3, 4, 5}, out: []int{1, 2, 3, 4, 5}, dups: []int{}, total: 0},
		// duplicates at beginning
		{in: []int{1, 1}, out: []int{1, 1}, dups: []int{}, total: 2},
		{in: []int{1, 1, 1}, out: []int{1, 1}, dups: []int{1}, total: 3},
		{in: []int{1, 1, 1, 1}, out: []int{1, 1}, dups: []int{1, 1}, total: 4},
		{in: []int{1, 1, 1, 1, 1}, out: []int{1, 1}, dups: []int{1, 1, 1}, total: 5},
		{in: []int{1, 1, 2, 3}, out: []int{1, 1, 2, 3}, dups: []int{}, total: 2},
		{in: []int{1, 1, 1, 2, 3}, out: []int{1, 1, 2, 3}, dups: []int{1}, total: 3},
		{in: []int{1, 1, 1, 1, 2, 3}, out: []int{1, 1, 2, 3}, dups: []int{1, 1}, total: 4},
		// duplicates in middle
		{in: []int{1, 2, 2, 3}, out: []int{1, 2, 2, 3}, dups: []int{}, total: 2},
		{in: []int{1, 2, 2, 2, 3}, out: []int{1, 2, 2, 3}, dups: []int{2}, total: 3},
		{in: []int{1, 2, 2, 2, 2, 3}, out: []int{1, 2, 2, 3}, dups: []int{2, 2}, total: 4},
		{in: []int{1, 2, 2, 2, 2, 2, 3}, out: []int{1, 2, 2, 3}, dups: []int{2, 2, 2}, total: 5},
		{in: []int{1, 2, 2, 2, 3, 3, 4}, out: []int{1, 2, 2, 3, 3, 4}, dups: []int{2}, total: 5},
		{in: []int{1, 2, 2, 2, 3, 3, 4, 4, 5}, out: []int{1, 2, 2, 3, 3, 4, 4, 5}, dups: []int{2}, total: 7},
		{in: []int{1, 2, 2, 2, 3, 4, 4, 5}, out: []int{1, 2, 2, 3, 4, 4, 5}, dups: []int{2}, total: 5},
		{in: []int{1, 2, 2, 2, 3, 4, 4, 5, 5, 5, 6, 7, 8, 8, 9}, out: []int{1, 2, 2, 3, 4, 4, 5, 5, 6, 7, 8, 8, 9}, dups: []int{2, 5}, total: 10},
		// duplicates at end
		{in: []int{1, 2, 3, 3}, out: []int{1, 2, 3, 3}, dups: []int{}, total: 2},
		{in: []int{1, 2, 3, 3, 3}, out: []int{1, 2, 3, 3}, dups: []int{3}, total: 3},
		{in: []int{1, 2, 3, 3, 3, 3}, out: []int{1, 2, 3, 3}, dups: []int{3, 3}, total: 4},
		{in: []int{1, 2, 3, 3, 3, 3, 3}, out: []int{1, 2, 3, 3}, dups: []int{3, 3, 3}, total: 5},
		// mixing
		{in: []int{1, 1, 1, 1, 1, 2, 3, 3, 3, 4}, out: []int{1, 1, 2, 3, 3, 4}, dups: []int{1, 1, 1, 3}, total: 8},
		{in: []int{1, 2, 3, 3, 3, 4, 4, 4}, out: []int{1, 2, 3, 3, 4, 4}, dups: []int{3, 4}, total: 6},
		{in: []int{1, 1, 1, 2, 3, 4, 4, 4}, out: []int{1, 1, 2, 3, 4, 4}, dups: []int{1, 4}, total: 6},
		{in: []int{1, 1, 1, 2, 2, 2, 3, 3, 3}, out: []int{1, 1, 2, 2, 3, 3}, dups: []int{1, 2, 3}, total: 9},
		{in: []int{1, 1, 2, 2, 2, 3, 3}, out: []int{1, 1, 2, 2, 3, 3}, dups: []int{2}, total: 7},
		{in: []int{1, 1, 2, 2, 2, 3, 3, 4, 4, 4}, out: []int{1, 1, 2, 2, 3, 3, 4, 4}, dups: []int{2, 4}, total: 10},
		{in: []int{1, 1, 2, 2, 2, 3, 3, 4, 4, 4, 5, 5}, out: []int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}, dups: []int{2, 4}, total: 12},
		{in: []int{1, 1, 2, 2, 2, 3, 4, 4, 4, 5, 5, 5}, out: []int{1, 1, 2, 2, 3, 4, 4, 5, 5}, dups: []int{2, 4, 5}, total: 11},
		{in: []int{1, 1, 2, 2, 2, 3, 4, 4, 5, 5, 5, 6, 7, 8, 8, 9, 9}, out: []int{1, 1, 2, 2, 3, 4, 4, 5, 5, 6, 7, 8, 8, 9, 9}, dups: []int{2, 5}, total: 14},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			require.True(t, slices.IsSorted(c.in))
			require.True(t, slices.IsSorted(c.out))
			require.True(t, slices.IsSorted(c.dups))
			require.Equal(t, len(c.dups), len(c.in)-len(c.out))
			tmpIn := make([]int, len(c.in))
			copy(tmpIn, c.in)
			out, dups, totalDup := removeDuplicatesMoreThanTwo(tmpIn, valGetter)
			require.EqualValues(t, c.out, out)
			require.EqualValues(t, c.dups, dups)
			require.Equal(t, c.total, totalDup)
		})
	}
}

func TestDivideMergeSortDataFilesBasic(t *testing.T) {
	testCases := []struct {
		fileCnt       int
		nodeCnt       int
		expectedSizes []int
	}{
		{31, 3, []int{31}},
		{64, 2, []int{32, 32}},
		{64, 3, []int{32, 32}},
		{127, 3, []int{43, 42, 42}},
		{128, 3, []int{43, 43, 42}},
		{4000, 6, []int{667, 667, 667, 667, 666, 666}},
		{4000, 7, []int{572, 572, 572, 571, 571, 571, 571}},
		{40000, 7, []int{4000, 4000, 4000, 4000, 4000, 4000, 4000, 1715, 1715, 1714, 1714, 1714, 1714, 1714}},
		{31000, 7, []int{4000, 4000, 4000, 4000, 4000, 4000, 4000, 429, 429, 429, 429, 428, 428, 428}},
		{28100, 7, []int{4000, 4000, 4000, 4000, 4000, 4000, 4000, 34, 33, 33}},
		{28031, 7, []int{4000, 4000, 4000, 4000, 4000, 4000, 4000, 31}},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("distribute %d files to %d nodes", tc.fileCnt, tc.nodeCnt)
		t.Run(name, func(t *testing.T) {
			items := make([]string, tc.fileCnt)
			result, err := DivideMergeSortDataFiles(items, tc.nodeCnt, 16)
			require.NoError(t, err)
			actualSizes := make([]int, len(result))
			for i, batch := range result {
				actualSizes[i] = len(batch)
			}
			require.EqualValues(t, tc.expectedSizes, actualSizes)
		})
	}
}

func TestDivideMergeSortDataFilesSubtaskCount(t *testing.T) {
	const Concurrency = 16
	for _, fileCount := range []int{3000, 4000, 40000, 400000, 712345, 1000000} {
		for _, nodeCount := range []int{1, 3, 7, 16, 30, 60, 97} {
			dataFiles := make([]string, fileCount)
			dataFilesGroup, err := DivideMergeSortDataFiles(dataFiles, nodeCount, Concurrency)
			require.NoError(t, err)
			var totalTargetFileCount int
			for _, dataFiles := range dataFilesGroup {
				totalTargetFileCount += len(splitDataFiles(dataFiles, Concurrency))
			}
			t.Logf("nodeCount: %d, fileCount: %d, subtaskCount:%d, totalTargetFileCount: %d",
				nodeCount, fileCount, len(dataFilesGroup), totalTargetFileCount)
			require.LessOrEqual(t, len(dataFilesGroup), 250)
			require.LessOrEqual(t, totalTargetFileCount, 4000)
		}
	}
}
>>>>>>> 06674e23094 (*: adjusts the merge sort overlap threshold/merge sort file count step based on concurrency (#62483))
