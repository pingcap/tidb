package extsort

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/rawkv"
)

type memoryRawKVClient struct {
	m map[string]string
}

func newMemoryRawKVClient() *memoryRawKVClient {
	return &memoryRawKVClient{
		m: make(map[string]string),
	}
}

func (m *memoryRawKVClient) BatchPut(
	_ context.Context,
	keys [][]byte,
	values [][]byte,
	_ ...rawkv.RawOption,
) error {
	for i, key := range keys {
		m.m[string(key)] = string(values[i])
	}
	return nil
}

func (*memoryRawKVClient) Close() error {
	return nil
}

var (
	smallRawKVPutSorterOpts = RawKVPutSorterOptions{
		WriterBufferSize:     20,
		WriterBufferKeyCount: 2,
		AsyncFlushInterval:   100 * time.Millisecond,
	}
)

func TestTaskName(t *testing.T) {
	ctx := context.Background()
	cli := newMemoryRawKVClient()
	w := newRawKVPutWriter(ctx, cli, "task-name", &smallRawKVPutSorterOpts)
	for i := byte(0); i < 10; i++ {
		err := w.Put([]byte{i}, []byte{i})
		require.NoError(t, err)
	}
	err := w.Close()
	require.NoError(t, err)
	expected := map[string]string{}
	for i := byte(0); i < 10; i++ {
		expected["task-name/"+string([]byte{i})] = string([]byte{i})
	}
	require.Equal(t, expected, cli.m)

	cli = newMemoryRawKVClient()
	w = newRawKVPutWriter(ctx, cli, "", &smallRawKVPutSorterOpts)
	for i := byte(0); i < 10; i++ {
		err := w.Put([]byte{i}, []byte{i})
		require.NoError(t, err)
	}
	err = w.Close()
	require.NoError(t, err)
	expected = map[string]string{}
	for i := byte(0); i < 10; i++ {
		expected["/"+string([]byte{i})] = string([]byte{i})
	}
	require.Equal(t, expected, cli.m)
}

func newRng(t *testing.T) *rand.Rand {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	return rand.New(rand.NewSource(seed))
}

func TestAsyncFlush(t *testing.T) {
	ctx := context.Background()
	cli := newMemoryRawKVClient()
	w := newRawKVPutWriter(ctx, cli, "task-name", &smallRawKVPutSorterOpts)
	w.wg.Add(1)
	go w.asyncFlushLoop()

	rng := newRng(t)

	keyLen, valLen := rng.Intn(100), rng.Intn(100)
	key, val := make([]byte, keyLen), make([]byte, valLen)
	rng.Read(key)
	rng.Read(val)
	err := w.Put(key, val)
	require.NoError(t, err)

	waitAndCheckFlushed := func() {
		require.Eventually(t, func() bool {
			startOfKvs, endOfKvs := w.kvsBuf.start.Load(), w.kvsBuf.end.Load()
			if startOfKvs != endOfKvs {
				return false
			}
			startOfData, endOfData := w.dataBuf.start.Load(), w.dataBuf.end.Load()
			return startOfData == endOfData
		}, time.Second, time.Millisecond*50)
	}
	waitAndCheckFlushed()

	expected := map[string]string{
		"task-name/" + string(key): string(val),
	}
	require.Equal(t, expected, cli.m)

	for i := 0; i < 10; i++ {
		keyLen, valLen = rng.Intn(100), rng.Intn(100)
		key, val = make([]byte, keyLen), make([]byte, valLen)
		rng.Read(key)
		rng.Read(val)
		err = w.Put(key, val)
		require.NoError(t, err)
		expected["task-name/"+string(key)] = string(val)
		time.Sleep(time.Millisecond * time.Duration(rng.Intn(100)))
	}
	waitAndCheckFlushed()

	require.Equal(t, expected, cli.m)
	require.NoError(t, w.Close())
}

type mockRawKVClient struct {
	mock.Mock
}

func (m *mockRawKVClient) BatchPut(ctx context.Context, keys [][]byte, values [][]byte, opts ...rawkv.RawOption) error {
	args := m.Called(ctx, keys, values, opts)
	return args.Error(0)
}

func (m *mockRawKVClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestFailedToBatchPut(t *testing.T) {
	ctx := context.Background()
	cli := &mockRawKVClient{}
	w := newRawKVPutWriter(ctx, cli, "task-name", &smallRawKVPutSorterOpts)

	// no need to call BatchPut if there is no data
	cli.On("Close").Return(nil).Once()
	err := w.Close()
	require.NoError(t, err)
	cli.AssertExpectations(t)

	// test flush when reach kvs buffer limit

	opts := RawKVPutSorterOptions{
		WriterBufferSize:     1_000_000,
		WriterBufferKeyCount: 4,
		AsyncFlushInterval:   10 * time.Millisecond,
	}
	errBatchPut := errors.New("batch put failed")
	cli.On("BatchPut",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errBatchPut)
	w = newRawKVPutWriter(ctx, cli, "task-name", &opts)
	w.wg.Add(1)
	go w.asyncFlushLoop()

	for i := byte(0); i < 2; i++ {
		err = w.Put([]byte{i}, []byte{i})
		require.NoError(t, err)
	}
	// sleep to wait for async flush, and failed async flush will not interrupt
	// the main logic
	time.Sleep(time.Millisecond * 50)
	err = w.Put([]byte{2}, []byte{2})
	require.NoError(t, err)
	// the 4th Put will trigger a flush because WriterBufferKeyCount is 4
	err = w.Put([]byte{3}, []byte{3})
	require.ErrorIs(t, err, errBatchPut)
	require.ErrorIs(t, w.Close(), errBatchPut)

	// test flush when reach data buffer limit

	opts = RawKVPutSorterOptions{
		WriterBufferSize:     40,
		WriterBufferKeyCount: 128,
		AsyncFlushInterval:   10 * time.Millisecond,
	}
	w = newRawKVPutWriter(ctx, cli, "", &opts)
	w.wg.Add(1)
	go w.asyncFlushLoop()

	dummy := make([]byte, 5)
	for i := byte(0); i < 2; i++ {
		err = w.Put(dummy, dummy)
		require.NoError(t, err)
	}

	// sleep to wait for async flush, and failed async flush will not interrupt
	// the main logic
	time.Sleep(time.Millisecond * 50)
	err = w.Put(dummy, dummy)
	require.NoError(t, err)
	// the 4th Put will trigger a flush because WriterBufferSize is 40
	err = w.Put(dummy, dummy)
	require.ErrorIs(t, err, errBatchPut)
	require.ErrorIs(t, w.Close(), errBatchPut)
}
