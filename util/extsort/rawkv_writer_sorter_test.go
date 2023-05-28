package extsort

import (
	"context"
	"math/rand"
	"testing"
	"time"

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
}
