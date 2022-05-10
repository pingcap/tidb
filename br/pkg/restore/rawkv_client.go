// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"time"

	"github.com/pingcap/errors"
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

// RawKVBatchClient is used to put raw kv-entry into tikv.
// Note: it is not thread safe.
type RawKVBatchClient struct {
	cf          string
	cap         int
	size        int
	keys        [][]byte
	values      [][]byte
	rawkvClient RawkvClient
}

// NewRawKVBatchClient create a batch rawkv client.
func NewRawKVBatchClient(
	rawkvClient RawkvClient,
	batchCount int,
) *RawKVBatchClient {
	return &RawKVBatchClient{
		cap:         batchCount,
		keys:        make([][]byte, 0, batchCount),
		values:      make([][]byte, 0, batchCount),
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
func (c *RawKVBatchClient) Put(ctx context.Context, key, value []byte) error {
	c.keys = append(c.keys, key)
	c.values = append(c.values, value)
	c.size++

	if c.size >= c.cap {
		err := c.rawkvClient.BatchPut(ctx, c.keys, c.values, rawkv.SetColumnFamily(c.cf))
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
		err := c.rawkvClient.BatchPut(ctx, c.keys, c.values, rawkv.SetColumnFamily(c.cf))
		if err != nil {
			return errors.Trace(err)
		}

		c.reset()
	}
	return nil
}

func (c *RawKVBatchClient) reset() {
	c.keys = make([][]byte, 0, c.cap)
	c.values = make([][]byte, 0, c.cap)
	c.size = 0
}
