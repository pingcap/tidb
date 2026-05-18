// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package simplesst

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

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
			out, dups, dupCnt := RemoveDuplicates(tmpIn, valGetter, true)
			require.EqualValues(t, c.out, out)
			require.EqualValues(t, c.dups, dups)
			require.Equal(t, dupCnt, len(dups))

			tmpIn = make([]int, len(c.in))
			copy(tmpIn, c.in)
			out, dups, dupCnt = RemoveDuplicates(tmpIn, valGetter, false)
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
			out, dups, totalDup := RemoveDuplicatesMoreThanTwo(tmpIn, valGetter)
			require.EqualValues(t, c.out, out)
			require.EqualValues(t, c.dups, dups)
			require.Equal(t, c.total, totalDup)
		})
	}
}

type blockingOpenMemStorage struct {
	*objstore.MemStorage
	releaseCh chan struct{}
	startedCh chan struct{}
	current   atomic.Int32
	max       atomic.Int32
}

func (s *blockingOpenMemStorage) Open(
	ctx context.Context,
	path string,
	o *storeapi.ReaderOption,
) (objectio.Reader, error) {
	cur := s.current.Add(1)
	for {
		oldMax := s.max.Load()
		if cur <= oldMax || s.max.CompareAndSwap(oldMax, cur) {
			break
		}
	}
	select {
	case s.startedCh <- struct{}{}:
	default:
	}
	select {
	case <-s.releaseCh:
	case <-ctx.Done():
		s.current.Add(-1)
		return nil, ctx.Err()
	}
	s.current.Add(-1)
	return s.MemStorage.Open(ctx, path, o)
}

func TestGetReadRangeFromProps(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()

	// file1 has props at offsets 10, 30, 50 with keys "key1", "key3", "key5"
	rc1 := &rangePropertiesCollector{
		props: []*RangeProperty{
			{FirstKey: []byte("key1"), Offset: 10, Size: 10, Keys: 1},
			{FirstKey: []byte("key3"), Offset: 30, Size: 10, Keys: 1},
			{FirstKey: []byte("key5"), Offset: 50, Size: 10, Keys: 1},
		},
	}
	file1 := "/test1"
	w1, err := store.Create(ctx, file1, nil)
	require.NoError(t, err)
	_, err = w1.Write(ctx, rc1.encode())
	require.NoError(t, err)
	err = w1.Close(ctx)
	require.NoError(t, err)

	// file2 has props at offsets 20, 40 with keys "key2", "key4"
	rc2 := &rangePropertiesCollector{
		props: []*RangeProperty{
			{FirstKey: []byte("key2"), Offset: 20, Size: 10, Keys: 1},
			{FirstKey: []byte("key4"), Offset: 40, Size: 10, Keys: 1},
		},
	}
	file2 := "/test2"
	w2, err := store.Create(ctx, file2, nil)
	require.NoError(t, err)
	_, err = w2.Write(ctx, rc2.encode())
	require.NoError(t, err)
	err = w2.Close(ctx)
	require.NoError(t, err)

	paths := []string{file1, file2}

	// single key between props
	got, err := GetReadRangeFromProps(ctx, [][]byte{[]byte("key2.5")}, paths, store)
	require.NoError(t, err)
	// key2.5: file1 => prop "key1" matches (offset=10), file2 => prop "key2" matches (offset=20)
	require.Equal(t, []uint64{10, 20}, got[0])

	// two keys between props
	got, err = GetReadRangeFromProps(ctx, [][]byte{[]byte("key2.5"), []byte("key2.6")}, paths, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, got[0])
	require.Equal(t, []uint64{10, 20}, got[1])

	// key exactly on a prop boundary
	got, err = GetReadRangeFromProps(ctx, [][]byte{[]byte("key3")}, paths, store)
	require.NoError(t, err)
	// key3: file1 => prop "key3" matches (offset=30), file2 => prop "key2" matches (offset=20)
	require.Equal(t, []uint64{30, 20}, got[0])

	// two keys, second exactly on a prop boundary
	got, err = GetReadRangeFromProps(ctx, [][]byte{[]byte("key2.5"), []byte("key3")}, paths, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, got[0])
	require.Equal(t, []uint64{30, 20}, got[1])

	// key below all props
	got, err = GetReadRangeFromProps(ctx, [][]byte{[]byte("key0")}, paths, store)
	require.NoError(t, err)
	// key0: no prop <= key0, so offset stays at zero default
	require.Equal(t, []uint64{0, 0}, got[0])

	// key exactly on first prop
	got, err = GetReadRangeFromProps(ctx, [][]byte{[]byte("key1")}, paths, store)
	require.NoError(t, err)
	// key1: file1 => prop "key1" matches (offset=10), file2 => no prop <= key1 so 0
	require.Equal(t, []uint64{10, 0}, got[0])

	// two keys: one below all, one on first prop
	got, err = GetReadRangeFromProps(ctx, [][]byte{[]byte("key0"), []byte("key1")}, paths, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 0}, got[0])
	require.Equal(t, []uint64{10, 0}, got[1])

	// key above all props
	got, err = GetReadRangeFromProps(ctx, [][]byte{[]byte("key999")}, paths, store)
	require.NoError(t, err)
	// key999: file1 => last prop "key5" (offset=50), file2 => last prop "key4" (offset=40)
	require.Equal(t, []uint64{50, 40}, got[0])

	// two identical keys above all props
	got, err = GetReadRangeFromProps(ctx, [][]byte{[]byte("key999"), []byte("key999")}, paths, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{50, 40}, got[0])
	require.Equal(t, []uint64{50, 40}, got[1])

	// empty stat file should return zero offsets
	file3 := "/test3"
	w3, err := store.Create(ctx, file3, nil)
	require.NoError(t, err)
	err = w3.Close(ctx)
	require.NoError(t, err)
	got, err = GetReadRangeFromProps(ctx, [][]byte{[]byte("key3")}, []string{file1, file2, file3}, store)
	require.NoError(t, err)
	require.Equal(t, []uint64{30, 20, 0}, got[0])
}

func TestGetReadRangeFromPropsEmptyJobKeys(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()

	writer, err := store.Create(ctx, "/test-empty-job-keys", nil)
	require.NoError(t, err)
	_, err = writer.Write(ctx, (&rangePropertiesCollector{
		props: []*RangeProperty{{FirstKey: []byte("key1"), Offset: 10, Size: 10, Keys: 1}},
	}).encode())
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.NoError(t, err)

	got, err := GetReadRangeFromProps(ctx, nil, []string{"/test-empty-job-keys"}, store)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestGetReadRangeFromPropsLimitsParallelRead(t *testing.T) {
	backup := getReadRangeFromPropsConcurrency
	getReadRangeFromPropsConcurrency = 2
	defer func() {
		getReadRangeFromPropsConcurrency = backup
	}()

	ctx := context.Background()
	store := &blockingOpenMemStorage{
		MemStorage: objstore.NewMemStorage(),
		releaseCh:  make(chan struct{}),
		startedCh:  make(chan struct{}, 16),
	}

	paths := make([]string, 5)
	for i := range paths {
		paths[i] = fmt.Sprintf("/test-open-limit-%d", i)
		writer, err := store.Create(ctx, paths[i], nil)
		require.NoError(t, err)
		_, err = writer.Write(ctx, (&rangePropertiesCollector{
			props: []*RangeProperty{{FirstKey: []byte("key1"), Offset: 10, Size: 10, Keys: 1}},
		}).encode())
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := GetReadRangeFromProps(ctx, [][]byte{[]byte("key1")}, paths, store)
		errCh <- err
	}()

	for range 2 {
		select {
		case <-store.startedCh:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for limited parallel reads to start")
		}
	}

	// The errgroup limit prevents additional goroutines from entering Open.
	select {
	case <-store.startedCh:
		t.Fatal("more than 2 concurrent opens detected despite concurrency limit")
	default:
	}
	require.EqualValues(t, 2, store.max.Load())

	close(store.releaseCh)
	require.NoError(t, <-errCh)
}
