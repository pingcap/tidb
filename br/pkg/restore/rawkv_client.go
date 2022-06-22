// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/hack"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	pd "github.com/tikv/pd/client"
)

// RawkvClient is the interface for rawkv.client
type RawkvClient interface {
	Get(ctx context.Context, key []byte, options ...rawkv.RawOption) ([]byte, error)
	Put(ctx context.Context, key, value []byte, options ...rawkv.RawOption) error
	BatchGet(ctx context.Context, keys [][]byte, options ...rawkv.RawOption) ([][]byte, error)
	BatchPut(ctx context.Context, keys, values [][]byte, options ...rawkv.RawOption) error
	Close() error
}

// NewRawkvClient create a rawkv client.
func NewRawkvClient(ctx context.Context, pdAddrs []string, security config.Security) (RawkvClient, error) {
	return rawkv.NewClient(
		ctx,
		pdAddrs,
		security,
		pd.WithCustomTimeoutOption(10*time.Second),
		pd.WithMaxErrorRetry(5))
}

type KVPair struct {
	ts    uint64
	key   []byte
	value []byte
}

// RawKVBatchClient is used to put raw kv-entry into tikv.
// Note: it is not thread safe.
type RawKVBatchClient struct {
	cf   string
	cap  int
	size int
	// use map to remove duplicate entry, cause duplicate entry will make tikv panic when resolved_ts enabled.
	// see https://github.com/tikv/tikv/blob/a401f78bc86f7e6ea6a55ad9f453ae31be835b55/components/resolved_ts/src/cmd.rs#L204
	kvs         map[hack.MutableString]KVPair
	rawkvClient RawkvClient
}

// NewRawKVBatchClient create a batch rawkv client.
func NewRawKVBatchClient(
	rawkvClient RawkvClient,
	batchCount int,
) *RawKVBatchClient {
	return &RawKVBatchClient{
		cap:         batchCount,
		kvs:         make(map[hack.MutableString]KVPair),
		rawkvClient: rawkvClient,
	}
}

// Close closes the RawKVBatchClient.
func (c *RawKVBatchClient) Close() {
	c.rawkvClient.Close()
}

// SetColumnFamily set the columnFamily for the client.
func (c *RawKVBatchClient) SetColumnFamily(columnFamily string) {
	c.cf = columnFamily
}

// Put puts (key, value) into buffer justly, wait for batch write if the buffer is full.
func (c *RawKVBatchClient) Put(ctx context.Context, key, value []byte, originTs uint64) error {
	k := TruncateTS(key)
	sk := hack.String(k)
	if v, ok := c.kvs[sk]; ok {
		if v.ts < originTs {
			c.kvs[sk] = KVPair{originTs, key, value}
		}
	} else {
		c.kvs[sk] = KVPair{originTs, key, value}
		c.size++
	}

	if c.size >= c.cap {
		keys := make([][]byte, 0, len(c.kvs))
		values := make([][]byte, 0, len(c.kvs))
		for _, kv := range c.kvs {
			keys = append(keys, kv.key)
			values = append(values, kv.value)
		}
		err := c.rawkvClient.BatchPut(ctx, keys, values, rawkv.SetColumnFamily(c.cf))
		if err != nil {
			return errors.Trace(err)
		}

		c.reset()
	}
	return nil
}

// PutRest writes the rest pairs (key, values) into tikv.
func (c *RawKVBatchClient) PutRest(ctx context.Context) error {
	if c.size > 0 {
		keys := make([][]byte, 0, len(c.kvs))
		values := make([][]byte, 0, len(c.kvs))
		for _, kv := range c.kvs {
			keys = append(keys, kv.key)
			values = append(values, kv.value)
		}
		err := c.rawkvClient.BatchPut(ctx, keys, values, rawkv.SetColumnFamily(c.cf))
		if err != nil {
			return errors.Trace(err)
		}

		c.reset()
	}
	return nil
}

func (c *RawKVBatchClient) reset() {
	c.kvs = make(map[hack.MutableString]KVPair)
	c.size = 0
}
