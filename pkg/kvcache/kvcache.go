package kvcache

import (
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	TotalMemoryLimit = 32 * 1024 * 1024 * 1024 // 16GB
)

var GlobalKVCache *KVCache = NewKVCache(TotalMemoryLimit / 128) // assume average key-value size is 128 bytes

var KVCacheCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "server",
		Name:      "kv_cache",
		Help:      "Counter of kv cache hit/miss.",
	}, []string{"result"})

var (
	HitCounter  = KVCacheCounter.WithLabelValues("hit")
	MissCounter = KVCacheCounter.WithLabelValues("miss")
)

func init() {
	prometheus.MustRegister(KVCacheCounter)
}

type KVCache struct {
	cache *lru.TwoQueueCache[string, []byte]
}

func NewKVCache(size int) *KVCache {
	l, err := lru.New2Q[string, []byte](size)
	if err != nil {
		panic(err)
	}
	return &KVCache{cache: l}
}

func (c *KVCache) Put(key []byte, value []byte) {
	strKey := string(hack.String(key))
	c.cache.Add(strKey, value)
}

func (c *KVCache) Get(key []byte) ([]byte, bool) {
	strKey := string(hack.String(key))
	if v, ok := c.cache.Get(strKey); ok {
		HitCounter.Inc()
		return v, true
	}
	MissCounter.Inc()
	return nil, false
}

func (c *KVCache) PutByHandle(tid int64, handle kv.Handle, value []byte) {
	var key []byte
	if partitionHandle, isPartitionHandle := handle.(*kv.PartitionHandle); isPartitionHandle {
		tid = partitionHandle.PartitionID
		handle = partitionHandle.Handle
	}
	switch h := handle.(type) {
	case kv.IntHandle:
		key = tablecodec.EncodeRowKey(tid, codec.EncodeInt(nil, h.IntValue()))
	case *kv.CommonHandle:
		key = tablecodec.EncodeRowKey(tid, h.Encoded())
	default:
		return
	}
	c.Put(key, value)
}

func (c *KVCache) GetByHandle(tid int64, handle kv.Handle) ([]byte, bool) {
	var key []byte
	if partitionHandle, isPartitionHandle := handle.(*kv.PartitionHandle); isPartitionHandle {
		tid = partitionHandle.PartitionID
		handle = partitionHandle.Handle
	}
	switch h := handle.(type) {
	case kv.IntHandle:
		key = tablecodec.EncodeRowKey(tid, codec.EncodeInt(nil, h.IntValue()))
	case *kv.CommonHandle:
		key = tablecodec.EncodeRowKey(tid, h.Encoded())
	default:
		return nil, false
	}
	return c.Get(key)
}
