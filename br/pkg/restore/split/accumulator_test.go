// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableSplitAccumulatorMatchesSequential(t *testing.T) {
	for _, workers := range []int{1, 3, 8} {
		rng := rand.New(rand.NewSource(int64(workers)))
		acc := NewTableSplitAccumulator(context.Background(), workers)
		want := map[int64]*SplitHelper{}
		for i := 0; i < 50000; i++ {
			// skewed table distribution: a few hot tables, many cold ones
			id := int64(rng.Intn(5))
			if rng.Intn(4) == 0 {
				id = int64(5 + rng.Intn(200))
			}
			a, b := rng.Intn(1000), rng.Intn(1000)
			if a > b {
				a, b = b, a
			}
			v := Valued{
				Key:   Span{StartKey: []byte(fmt.Sprintf("t%d_%05d", id, a)), EndKey: []byte(fmt.Sprintf("t%d_%05d", id, b))},
				Value: Value{Size: uint64(rng.Intn(1 << 20)), Number: int64(rng.Intn(1 << 10))},
			}
			require.NoError(t, acc.Add(id, v))
			h, ok := want[id]
			if !ok {
				h = NewSplitHelper()
				want[id] = h
			}
			h.Merge(v)
		}
		got, err := acc.Wait()
		require.NoError(t, err)
		require.Len(t, got, len(want))
		for id, h := range want {
			require.Equal(t, allFragments(h.Traverse), allFragments(got[id].Traverse), "workers %d table %d", workers, id)
		}
	}
}

func TestTableSplitAccumulatorAbortAndCancel(t *testing.T) {
	v := Valued{Key: Span{StartKey: []byte("a"), EndKey: []byte("b")}, Value: Value{Size: 1, Number: 1}}

	acc := NewTableSplitAccumulator(context.Background(), 4)
	for i := 0; i < 10000; i++ {
		require.NoError(t, acc.Add(int64(i%7), v))
	}
	acc.Abort() // must not hang or panic

	ctx, cancel := context.WithCancel(context.Background())
	acc = NewTableSplitAccumulator(ctx, 2)
	for i := 0; i < 1000; i++ {
		require.NoError(t, acc.Add(1, v))
	}
	cancel()
	_, err := acc.Wait()
	require.Error(t, err)
}

// TestTableSplitAccumulatorManyTables exercises the global bound on partial batches.
func TestTableSplitAccumulatorManyTables(t *testing.T) {
	acc := NewTableSplitAccumulator(context.Background(), 4)
	want := map[int64]*SplitHelper{}
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 3*accumulatorMaxLocalSpans; i++ {
		id := int64(rng.Intn(5000)) // many tables, each mostly holding a partial batch
		a := rng.Intn(100)
		v := Valued{
			Key:   Span{StartKey: []byte(fmt.Sprintf("t%d_%03d", id, a)), EndKey: []byte(fmt.Sprintf("t%d_%03d", id, a+1+rng.Intn(10)))},
			Value: Value{Size: uint64(rng.Intn(1 << 20)), Number: int64(1 + rng.Intn(1<<10))},
		}
		require.NoError(t, acc.Add(id, v))
		require.Less(t, acc.nLocal, accumulatorMaxLocalSpans)
		if _, ok := want[id]; !ok {
			want[id] = NewSplitHelper()
		}
		want[id].Merge(v)
	}
	got, err := acc.Wait()
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for id, h := range want {
		require.Equal(t, allFragments(h.Traverse), allFragments(got[id].Traverse), "table %d", id)
	}
}

func TestTableSplitAccumulatorAddAfterClose(t *testing.T) {
	v := Valued{Key: Span{StartKey: []byte("a"), EndKey: []byte("b")}, Value: Value{Size: 1, Number: 1}}
	acc := NewTableSplitAccumulator(context.Background(), 2)
	require.NoError(t, acc.Add(1, v))
	_, err := acc.Wait()
	require.NoError(t, err)
	require.Error(t, acc.Add(1, v))

	acc = NewTableSplitAccumulator(context.Background(), 2)
	acc.Abort()
	require.Error(t, acc.Add(1, v))
}
