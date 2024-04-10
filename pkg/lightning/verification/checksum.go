// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package verification

import (
	"fmt"
	"hash/crc64"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"go.uber.org/zap/zapcore"
)

var ecmaTable = crc64.MakeTable(crc64.ECMA)

// KVChecksum is the checksum of a collection of key-value pairs. The zero value
// of KVChecksum is a checksum for empty content and zero keyspace.
type KVChecksum struct {
	base      uint64
	prefixLen int
	bytes     uint64
	kvs       uint64
	checksum  uint64
}

// NewKVChecksum creates a pointer to zero KVChecksum.
func NewKVChecksum() *KVChecksum {
	return &KVChecksum{}
}

// NewKVChecksumWithKeyspace creates a new KVChecksum with the given checksum and keyspace.
func NewKVChecksumWithKeyspace(keyspace []byte) *KVChecksum {
	return &KVChecksum{
		base:      crc64.Update(0, ecmaTable, keyspace),
		prefixLen: len(keyspace),
	}
}

// MakeKVChecksum creates a new KVChecksum with the given checksum.
func MakeKVChecksum(bytes uint64, kvs uint64, checksum uint64) KVChecksum {
	return KVChecksum{
		bytes:    bytes,
		kvs:      kvs,
		checksum: checksum,
	}
}

// UpdateOne updates the checksum with a single key-value pair.
func (c *KVChecksum) UpdateOne(kv common.KvPair) {
	sum := crc64.Update(c.base, ecmaTable, kv.Key)
	sum = crc64.Update(sum, ecmaTable, kv.Val)

	c.bytes += uint64(c.prefixLen + len(kv.Key) + len(kv.Val))
	c.kvs++
	c.checksum ^= sum
}

// Update updates the checksum with a batch of key-value pairs.
func (c *KVChecksum) Update(kvs []common.KvPair) {
	var (
		checksum uint64
		sum      uint64
		kvNum    int
		bytes    int
	)

	for _, pair := range kvs {
		sum = crc64.Update(c.base, ecmaTable, pair.Key)
		sum = crc64.Update(sum, ecmaTable, pair.Val)
		checksum ^= sum
		kvNum++
		bytes += c.prefixLen
		bytes += len(pair.Key) + len(pair.Val)
	}

	c.bytes += uint64(bytes)
	c.kvs += uint64(kvNum)
	c.checksum ^= checksum
}

// Add adds the checksum of another KVChecksum.
func (c *KVChecksum) Add(other *KVChecksum) {
	c.bytes += other.bytes
	c.kvs += other.kvs
	c.checksum ^= other.checksum
}

// Sum returns the checksum.
func (c *KVChecksum) Sum() uint64 {
	return c.checksum
}

// SumSize returns the total size of the key-value pairs.
func (c *KVChecksum) SumSize() uint64 {
	return c.bytes
}

// SumKVS returns the total number of key-value pairs.
func (c *KVChecksum) SumKVS() uint64 {
	return c.kvs
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (c *KVChecksum) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("cksum", c.checksum)
	encoder.AddUint64("size", c.bytes)
	encoder.AddUint64("kvs", c.kvs)
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (c *KVChecksum) MarshalJSON() ([]byte, error) {
	result := fmt.Sprintf(`{"checksum":%d,"size":%d,"kvs":%d}`, c.checksum, c.bytes, c.kvs)
	return []byte(result), nil
}

// KVGroupChecksum is KVChecksum(s) each for a data KV group or index KV groups.
type KVGroupChecksum struct {
	m        map[int64]*KVChecksum
	keyspace []byte
}

// DataKVGroupID represents the ID for data KV group, as index id starts from 1,
// so we use -1 to represent data kv group.
const DataKVGroupID = -1

// NewKVGroupChecksumWithKeyspace creates a new KVGroupChecksum with the given
// keyspace.
func NewKVGroupChecksumWithKeyspace(keyspace []byte) *KVGroupChecksum {
	m := make(map[int64]*KVChecksum, 8)
	m[DataKVGroupID] = NewKVChecksumWithKeyspace(keyspace)
	return &KVGroupChecksum{m: m, keyspace: keyspace}
}

// NewKVGroupChecksumForAdd creates a new KVGroupChecksum, and it can't be used
// with UpdateOneDataKV or UpdateOneIndexKV.
func NewKVGroupChecksumForAdd() *KVGroupChecksum {
	m := make(map[int64]*KVChecksum, 8)
	m[DataKVGroupID] = NewKVChecksum()
	return &KVGroupChecksum{m: m}
}

// UpdateOneDataKV updates the checksum with a single data(record) key-value
// pair. It will not check the key-value pair's key is a real data key again.
func (c *KVGroupChecksum) UpdateOneDataKV(kv common.KvPair) {
	c.m[DataKVGroupID].UpdateOne(kv)
}

// UpdateOneIndexKV updates the checksum with a single index key-value pair. It
// will not check the key-value pair's key is a real index key of that index ID
// again.
func (c *KVGroupChecksum) UpdateOneIndexKV(indexID int64, kv common.KvPair) {
	cksum := c.m[indexID]
	if cksum == nil {
		cksum = NewKVChecksumWithKeyspace(c.keyspace)
		c.m[indexID] = cksum
	}
	cksum.UpdateOne(kv)
}

// Add adds the checksum of another KVGroupChecksum.
func (c *KVGroupChecksum) Add(other *KVGroupChecksum) {
	for id, cksum := range other.m {
		thisCksum := c.getOrCreateOneGroup(id)
		thisCksum.Add(cksum)
	}
}

func (c *KVGroupChecksum) getOrCreateOneGroup(id int64) *KVChecksum {
	cksum, ok := c.m[id]
	if ok {
		return cksum
	}
	cksum = NewKVChecksumWithKeyspace(c.keyspace)
	c.m[id] = cksum
	return cksum
}

// AddRawGroup adds the raw information of a KV group.
func (c *KVGroupChecksum) AddRawGroup(id int64, bytes, kvs, checksum uint64) {
	oneGroup := c.getOrCreateOneGroup(id)
	tmp := MakeKVChecksum(bytes, kvs, checksum)
	oneGroup.Add(&tmp)
}

// DataAndIndexSumSize returns the total size of data KV pairs and index KV pairs.
func (c *KVGroupChecksum) DataAndIndexSumSize() (dataSize, indexSize uint64) {
	for id, cksum := range c.m {
		if id == DataKVGroupID {
			dataSize = cksum.SumSize()
		} else {
			indexSize += cksum.SumSize()
		}
	}
	return
}

// DataAndIndexSumKVS returns the total number of data KV pairs and index KV pairs.
func (c *KVGroupChecksum) DataAndIndexSumKVS() (dataKVS, indexKVS uint64) {
	for id, cksum := range c.m {
		if id == DataKVGroupID {
			dataKVS = cksum.SumKVS()
		} else {
			indexKVS += cksum.SumKVS()
		}
	}
	return
}

// GetInnerChecksums returns a cloned map of index ID to its KVChecksum.
func (c *KVGroupChecksum) GetInnerChecksums() map[int64]*KVChecksum {
	m := make(map[int64]*KVChecksum, len(c.m))
	for id, cksum := range c.m {
		m[id] = &KVChecksum{
			base:      cksum.base,
			prefixLen: cksum.prefixLen,
			bytes:     cksum.bytes,
			kvs:       cksum.kvs,
			checksum:  cksum.checksum,
		}
	}
	return m
}

// MergedChecksum merges all groups of this checksum into a single KVChecksum.
func (c *KVGroupChecksum) MergedChecksum() KVChecksum {
	merged := NewKVChecksum()
	for _, cksum := range c.m {
		merged.Add(cksum)
	}
	return *merged
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (c *KVGroupChecksum) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	for id, cksum := range c.m {
		err := encoder.AddObject(fmt.Sprintf("id=%d", id), cksum)
		if err != nil {
			return err
		}
	}
	return nil
}
