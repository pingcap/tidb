package executor

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type closeCountingIter struct {
	keys   []kv.Key
	values [][]byte
	idx    int
	closed *int32
}

func (it *closeCountingIter) Valid() bool {
	return it.idx < len(it.keys)
}

func (it *closeCountingIter) Key() kv.Key {
	return it.keys[it.idx]
}

func (it *closeCountingIter) Value() []byte {
	return it.values[it.idx]
}

func (it *closeCountingIter) Next() error {
	it.idx++
	return nil
}

func (it *closeCountingIter) Close() {
	atomic.AddInt32(it.closed, 1)
}

type testSnapshotIterer struct {
	iters []kv.Iterator
	idx   int
}

func (m *testSnapshotIterer) SnapshotIter(_ kv.Key, _ kv.Key) kv.Iterator {
	it := m.iters[m.idx]
	m.idx++
	return it
}

func (m *testSnapshotIterer) SnapshotIterReverse(_ kv.Key, _ kv.Key) kv.Iterator {
	it := m.iters[m.idx]
	m.idx++
	return it
}

func TestTxnMemBufferIterClosesIteratorOnRangeSwitch(t *testing.T) {
	sctx := mock.NewContext()

	var closed1 int32
	iter1 := &closeCountingIter{
		keys:   []kv.Key{kv.Key("k1")},
		values: [][]byte{[]byte("v1")},
		closed: &closed1,
	}

	var closed2 int32
	iter2 := &closeCountingIter{
		keys:   nil,
		values: nil,
		closed: &closed2,
	}

	memBuf := &testSnapshotIterer{iters: []kv.Iterator{iter1, iter2}}
	it := &txnMemBufferIter{
		sctx:       sctx,
		kvRanges:   []kv.KeyRange{{StartKey: kv.Key("a"), EndKey: kv.Key("b")}, {StartKey: kv.Key("c"), EndKey: kv.Key("d")}},
		cacheTable: nil,
		memBuf:     memBuf,
		reverse:    false,
	}

	require.True(t, it.Valid())
	require.Equal(t, kv.Key("k1"), it.Key())
	require.NoError(t, it.Next())

	require.False(t, it.Valid())
	require.Equal(t, int32(1), atomic.LoadInt32(&closed1))
	require.Equal(t, int32(1), atomic.LoadInt32(&closed2))
}

func TestIterMemBufferSnapshotClosesIterators(t *testing.T) {
	sctx := mock.NewContext()
	ranges := []kv.KeyRange{{StartKey: kv.Key("a"), EndKey: kv.Key("z")}}

	t.Run("close on completion", func(t *testing.T) {
		var closed int32
		iter := &closeCountingIter{
			keys:   []kv.Key{kv.Key("k1"), kv.Key("k2")},
			values: [][]byte{[]byte("v1"), []byte("v2")},
			closed: &closed,
		}
		memBuf := &testSnapshotIterer{iters: []kv.Iterator{iter}}

		seen := 0
		err := iterMemBufferSnapshot(sctx, memBuf, nil, ranges, false, func(_ []byte, _ []byte) error {
			seen++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, seen)
		require.Equal(t, int32(1), atomic.LoadInt32(&closed))
	})

	t.Run("close on error", func(t *testing.T) {
		var closed int32
		iter := &closeCountingIter{
			keys:   []kv.Key{kv.Key("k1"), kv.Key("k2")},
			values: [][]byte{[]byte("v1"), []byte("v2")},
			closed: &closed,
		}
		memBuf := &testSnapshotIterer{iters: []kv.Iterator{iter}}

		sentinel := errors.New("sentinel")
		seen := 0
		err := iterMemBufferSnapshot(sctx, memBuf, nil, ranges, false, func(_ []byte, _ []byte) error {
			seen++
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, 1, seen)
		require.Equal(t, int32(1), atomic.LoadInt32(&closed))
	})
}
