// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/rawkv"
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
			"the lenth of keys don't equal the length of values")
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
		{Key: []byte("key1"), Value: []byte("v1")},
		{Key: []byte("key2"), Value: []byte("v2")},
		{Key: []byte("key3"), Value: []byte("v3")},
		{Key: []byte("key4"), Value: []byte("v4")},
		{Key: []byte("key5"), Value: []byte("v5")},
	}

	for i := 0; i < batchCount; i++ {
		require.Equal(t, len(fakeRawkvClient.kvs), 0)
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value)
		require.Nil(t, err)
	}
	require.Equal(t, len(fakeRawkvClient.kvs), batchCount)

	for i := batchCount; i < len(kvs); i++ {
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value)
		require.Nil(t, err)
	}
	require.Equal(t, len(fakeRawkvClient.kvs), batchCount)
	err := rawkvBatchClient.PutRest(context.TODO())
	require.Nil(t, err)
	require.Equal(t, kvs, fakeRawkvClient.kvs)
}
