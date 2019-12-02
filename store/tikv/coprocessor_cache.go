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

package tikv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/config"
)

type CoprCache struct {
	cache                   *ristretto.Cache
	admissionMaxSize        int
	admissionMinProcessTime time.Duration
}

type CoprCacheValue struct {
	Data              []byte
	TimeStamp         uint64
	RegionId          uint64
	RegionDataVersion uint64
}

func (v *CoprCacheValue) String() string {
	return fmt.Sprintf("{ Ts = %d, RegionId = %d, RegionDataVersion = %d, len(Data) = %d }", v.TimeStamp, v.RegionId, v.RegionDataVersion, len(v.Data))
}

func newCoprCache(config *config.CoprocessorCache) (*CoprCache, error) {
	if config == nil || !config.Enabled {
		return nil, nil
	}
	capacityInBytes := int64(config.CapacityMb * 1024.0 * 1024.0)
	estimatedEntities := capacityInBytes / int64(config.AdmissionMaxResultBytes) * 2
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: estimatedEntities * 10,
		MaxCost:     int64(config.CapacityMb * 1024.0 * 1024.0),
		BufferItems: 64,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := CoprCache{
		cache:                   cache,
		admissionMaxSize:        int(config.AdmissionMaxResultBytes),
		admissionMinProcessTime: time.Duration(config.AdmissionMinProcessMs) * time.Millisecond,
	}
	return &c, nil
}

func coprCacheBuildKey(copReq *coprocessor.Request) []byte {
	l := len(copReq.Data) + 1 + 8 + 1
	for _, r := range copReq.Ranges {
		l = l + len(r.Start) + 1 + len(r.End) + 1
	}

	key := bytes.NewBuffer(make([]byte, 0, l))
	_, _ = key.Write(copReq.Data)
	_ = key.WriteByte(0)
	_ = binary.Write(key, binary.LittleEndian, copReq.Tp)
	for _, r2 := range copReq.Ranges {
		_, _ = key.Write(r2.Start)
		_ = key.WriteByte(0)
		_, _ = key.Write(r2.End)
		_ = key.WriteByte(0)
	}

	return key.Bytes()
}

// Get gets a cache item according to cache key.
func (c *CoprCache) Get(key []byte) *CoprCacheValue {
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	return value.(*CoprCacheValue)
}

// CheckAdmission checks whether an item is worth caching.
func (c *CoprCache) CheckAdmission(dataSize int, processTime time.Duration) bool {
	if c == nil {
		return false
	}
	if dataSize == 0 || dataSize > c.admissionMaxSize {
		return false
	}
	if processTime == 0 || processTime < c.admissionMinProcessTime {
		return false
	}
	return true
}

// Set inserts an item to the cache.
// It is recommended to call `CheckAdmission` before inserting the item to the cache.
func (c *CoprCache) Set(key []byte, value *CoprCacheValue) bool {
	if c == nil {
		return false
	}
	return c.cache.Set(key, value, int64(len(value.Data)+24))
}
