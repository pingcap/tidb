// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package iter_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/stretchr/testify/require"
)

func TestParTrans(t *testing.T) {
	items := iter.OfRange(0, 200)
	mapped := iter.Transform(items, func(c context.Context, i int) (int, error) {
		select {
		case <-c.Done():
			return 0, c.Err()
		case <-time.After(100 * time.Millisecond):
		}
		return i + 100, nil
	}, iter.WithChunkSize(128), iter.WithConcurrency(64))
	cx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	r := iter.CollectAll(cx, mapped)
	require.NoError(t, r.Err)
	require.Len(t, r.Item, 200)
	require.Equal(t, r.Item, iter.CollectAll(cx, iter.OfRange(100, 300)).Item)
}

func TestFilter(t *testing.T) {
	items := iter.OfRange(0, 10)
	items = iter.FlatMap(items, func(n int) iter.TryNextor[int] {
		return iter.Map(iter.OfRange(n, 10), func(i int) int { return n * i })
	})
	items = iter.FilterOut(items, func(n int) bool { return n == 0 || (n+1)%13 != 0 })
	coll := iter.CollectAll(context.Background(), items)
	require.Equal(t, []int{12, 12, 25, 64}, coll.Item, "%s", coll)
}

func TestEnumerate(t *testing.T) {
	items := iter.OfRange(0, 10)
	enums := iter.Enumerate(items)
	enums = iter.FilterOut(enums, func(ni iter.Indexed[int]) bool { return ni.Item%2 == 0 })
	coll := iter.CollectAll(context.Background(), enums)
	expects := []int{1, 3, 5, 7, 9}
	for i, col := range coll.Item {
		require.Equal(t, col.Item, col.Index)
		require.Equal(t, expects[i], col.Item)
	}
}

func TestFailure(t *testing.T) {
	items := iter.ConcatAll(iter.OfRange(0, 5), iter.Fail[int](errors.New("meow?")), iter.OfRange(5, 10))
	items = iter.FlatMap(items, func(n int) iter.TryNextor[int] {
		return iter.Map(iter.OfRange(n, 10), func(i int) int { return n * i })
	})
	items = iter.FilterOut(items, func(n int) bool { return n == 0 || (n+1)%13 != 0 })
	coll := iter.CollectAll(context.Background(), items)
	require.Error(t, coll.Err, "%s", coll)
	require.Nil(t, coll.Item)
}

func TestCollect(t *testing.T) {
	items := iter.OfRange(0, 100)
	ctx := context.Background()
	coll := iter.CollectMany(ctx, items, 10)
	require.Len(t, coll.Item, 10, "%s", coll)
	require.Equal(t, coll.Item, iter.CollectAll(ctx, iter.OfRange(0, 10)).Item)
}

func TestTapping(t *testing.T) {
	items := iter.OfRange(0, 101)
	ctx := context.Background()
	n := 0

	items = iter.Tap(items, func(i int) { n += i })
	iter.CollectAll(ctx, items)
	require.Equal(t, 5050, n)
}

func TestSome(t *testing.T) {
	req := require.New(t)
	it := iter.OfRange(0, 2)
	c := context.Background()
	req.Equal(it.TryNext(c), iter.Emit(0))
	req.Equal(it.TryNext(c), iter.Emit(1))
	req.Equal(it.TryNext(c), iter.Done[int]())
	req.Equal(it.TryNext(c), iter.Done[int]())
}

func TestErrorDuringTransforming(t *testing.T) {
	req := require.New(t)
	items := iter.OfRange(1, 20)
	running := new(atomic.Int32)
	items = iter.Transform(items, func(ctx context.Context, i int) (int, error) {
		if i == 10 {
			return 0, errors.New("meow")
		}
		running.Add(1)
		return i, nil
	}, iter.WithChunkSize(16), iter.WithConcurrency(8))

	coll := iter.CollectAll(context.TODO(), items)
	req.Greater(running.Load(), int32(8))
	// Should be melted down.
	req.Less(running.Load(), int32(16))
	req.ErrorContains(coll.Err, "meow")
}
