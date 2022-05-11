// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/rawkv"

	"github.com/pingcap/tidb/util/codec"
)

// fakeRawkvClient is a mock for rawkv.client
type fakeRawkvClient struct {
	rawkv.Client
	kvs []kv.Entry
}

func newFakeRawkvClient() *fakeRawkvClient {
	return &fakeRawkvClient{
		kvs: make([]kv.Entry, 0),
	}
}

func (f *fakeRawkvClient) BatchPut(
	ctx context.Context,
	keys [][]byte,
	values [][]byte,
	options ...rawkv.RawOption,
) error {
	if len(keys) != len(values) {
		return errors.Annotate(berrors.ErrInvalidArgument,
			"the length of keys don't equal the length of values")
	}

	for i := 0; i < len(keys); i++ {
		entry := kv.Entry{
			Key:   keys[i],
			Value: values[i],
		}
		f.kvs = append(f.kvs, entry)
	}
	return nil
}

func (f *fakeRawkvClient) Close() error {
	return nil
}

func TestRawKVBatchClient(t *testing.T) {
	fakeRawkvClient := newFakeRawkvClient()
	batchCount := 3
	rawkvBatchClient := restore.NewRawKVBatchClient(fakeRawkvClient, batchCount)
	defer rawkvBatchClient.Close()

	rawkvBatchClient.SetColumnFamily("default")

	kvs := []kv.Entry{
		{Key: codec.EncodeUintDesc([]byte("key1"), 1), Value: []byte("v1")},
		{Key: codec.EncodeUintDesc([]byte("key2"), 2), Value: []byte("v2")},
		{Key: codec.EncodeUintDesc([]byte("key3"), 3), Value: []byte("v3")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 4), Value: []byte("v4")},
		{Key: codec.EncodeUintDesc([]byte("key5"), 5), Value: []byte("v5")},
	}

	for i := 0; i < batchCount; i++ {
		require.Equal(t, 0, len(fakeRawkvClient.kvs))
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value, uint64(i+1))
		require.Nil(t, err)
	}
	require.Equal(t, batchCount, len(fakeRawkvClient.kvs))

	for i := batchCount; i < len(kvs); i++ {
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value, uint64(i+1))
		require.Nil(t, err)
	}
	require.Equal(t, batchCount, len(fakeRawkvClient.kvs))
	err := rawkvBatchClient.PutRest(context.TODO())
	require.Nil(t, err)
	sort.Slice(fakeRawkvClient.kvs, func(i, j int) bool {
		return bytes.Compare(fakeRawkvClient.kvs[i].Key, fakeRawkvClient.kvs[j].Key) < 0
	})
	require.Equal(t, kvs, fakeRawkvClient.kvs)
}

func TestRawKVBatchClientDuplicated(t *testing.T) {
	fakeRawkvClient := newFakeRawkvClient()
	batchCount := 3
	rawkvBatchClient := restore.NewRawKVBatchClient(fakeRawkvClient, batchCount)
	defer rawkvBatchClient.Close()

	rawkvBatchClient.SetColumnFamily("default")

	kvs := []kv.Entry{
		{Key: codec.EncodeUintDesc([]byte("key1"), 1), Value: []byte("v1")},
		{Key: codec.EncodeUintDesc([]byte("key1"), 2), Value: []byte("v2")},
		{Key: codec.EncodeUintDesc([]byte("key3"), 3), Value: []byte("v3")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 4), Value: []byte("v4")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 5), Value: []byte("v5")},
	}

	expectedKvs := []kv.Entry{
		// we keep the large ts entry, and we only make sure there is no duplicated entry in a batch.
		// which is 3. so the duplicated key4 not in a batch will have two versions finally.
		{Key: codec.EncodeUintDesc([]byte("key1"), 2), Value: []byte("v2")},
		{Key: codec.EncodeUintDesc([]byte("key3"), 3), Value: []byte("v3")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 5), Value: []byte("v5")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 4), Value: []byte("v4")},
	}

	for i := 0; i < batchCount; i++ {
		require.Equal(t, 0, len(fakeRawkvClient.kvs))
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value, uint64(i+1))
		require.Nil(t, err)
	}
	// There only two different keys which doesn't send to kv.
	require.Equal(t, 0, len(fakeRawkvClient.kvs))

	for i := batchCount; i < 5; i++ {
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value, uint64(i+1))
		require.Nil(t, err)
		require.Equal(t, batchCount, len(fakeRawkvClient.kvs))
	}

	err := rawkvBatchClient.PutRest(context.TODO())
	require.Nil(t, err)
	sort.Slice(fakeRawkvClient.kvs, func(i, j int) bool {
		return bytes.Compare(fakeRawkvClient.kvs[i].Key, fakeRawkvClient.kvs[j].Key) < 0
	})
	require.Equal(t, expectedKvs, fakeRawkvClient.kvs)
}
