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
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"fmt"
	"hash/crc64"

	"go.uber.org/zap/zapcore"
)

var ecmaTable = crc64.MakeTable(crc64.ECMA)

// Checksum represents the field needs checksum.
type Checksum struct {
	bytes    uint64
	kvs      uint64
	checksum uint64
}

// NewKVChecksum creates Checksum.
func NewKVChecksum(checksum uint64) *Checksum {
	return &Checksum{
		checksum: checksum,
	}
}

// MakeKVChecksum creates Checksum.
func MakeKVChecksum(bytes uint64, kvs uint64, checksum uint64) Checksum {
	return Checksum{
		bytes:    bytes,
		kvs:      kvs,
		checksum: checksum,
	}
}

// UpdateOne add kv with its values.
func (c *Checksum) UpdateOne(kv Pair) {
	sum := crc64.Update(0, ecmaTable, kv.Key)
	sum = crc64.Update(sum, ecmaTable, kv.Val)

	c.bytes += uint64(len(kv.Key) + len(kv.Val))
	c.kvs++
	c.checksum ^= sum
}

// Update add batch of kvs with their values.
func (c *Checksum) Update(kvs []Pair) {
	var (
		checksum uint64
		sum      uint64
		kvNum    int
		bytes    int
	)

	for _, pair := range kvs {
		sum = crc64.Update(0, ecmaTable, pair.Key)
		sum = crc64.Update(sum, ecmaTable, pair.Val)
		checksum ^= sum
		kvNum++
		bytes += (len(pair.Key) + len(pair.Val))
	}

	c.bytes += uint64(bytes)
	c.kvs += uint64(kvNum)
	c.checksum ^= checksum
}

// Add other checksum.
func (c *Checksum) Add(other *Checksum) {
	c.bytes += other.bytes
	c.kvs += other.kvs
	c.checksum ^= other.checksum
}

// Sum returns the checksum.
func (c *Checksum) Sum() uint64 {
	return c.checksum
}

// SumSize returns the bytes.
func (c *Checksum) SumSize() uint64 {
	return c.bytes
}

// SumKVS returns the kv count.
func (c *Checksum) SumKVS() uint64 {
	return c.kvs
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (c *Checksum) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("cksum", c.checksum)
	encoder.AddUint64("size", c.bytes)
	encoder.AddUint64("kvs", c.kvs)
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (c Checksum) MarshalJSON() ([]byte, error) {
	result := fmt.Sprintf(`{"checksum":%d,"size":%d,"kvs":%d}`, c.checksum, c.bytes, c.kvs)
	return []byte(result), nil
}
