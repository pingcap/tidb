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
	"fmt"
	"time"
	"unsafe"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/config"
)

type coprCache struct {
	cache                   *ristretto.Cache
	admissionMaxSize        int
	admissionMinProcessTime time.Duration
}

type coprCacheValue struct {
	Data              []byte
	TimeStamp         uint64
	RegionID          uint64
	RegionDataVersion uint64
}

func (v *coprCacheValue) String() string {
	return fmt.Sprintf("{ Ts = %d, RegionID = %d, RegionDataVersion = %d, len(Data) = %d }",
		v.TimeStamp,
		v.RegionID,
		v.RegionDataVersion,
		len(v.Data))
}

func newCoprCache(config *config.CoprocessorCache) (*coprCache, error) {
	if config == nil || !config.Enabled {
		return nil, nil
	}
	capacityInBytes := int64(config.CapacityMB * 1024.0 * 1024.0)
	estimatedEntities := capacityInBytes / int64(config.AdmissionMaxResultMB) * 2
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: estimatedEntities * 10,
		MaxCost:     capacityInBytes,
		BufferItems: 64,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := coprCache{
		cache:                   cache,
		admissionMaxSize:        int(config.AdmissionMaxResultMB * 1024.0 * 1024.0),
		admissionMinProcessTime: time.Duration(config.AdmissionMinProcessMs) * time.Millisecond,
	}
	return &c, nil
}

func coprCacheBuildKey(copReq *coprocessor.Request) []byte {
	l := len(copReq.Data) + 1
	for _, r := range copReq.Ranges {
		l = l + len(r.Start) + 1 + len(r.End) + 1
	}

	key := bytes.NewBuffer(make([]byte, 0, l))
	key.Write(copReq.Data)
	key.WriteByte(0)
	for _, r := range copReq.Ranges {
		key.Write(r.Start)
		key.WriteByte(0)
		key.Write(r.End)
		key.WriteByte(0)
	}

	return key.Bytes()
}

// Get gets a cache item according to cache key.
func (c *coprCache) Get(key []byte) *coprCacheValue {
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	return value.(*coprCacheValue)
}

// CheckAdmission checks whether an item is worth caching.
func (c *coprCache) CheckAdmission(dataSize int, processTime time.Duration) bool {
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
func (c *coprCache) Set(key []byte, value *coprCacheValue) bool {
	if c == nil {
		return false
	}
	return c.cache.Set(key, value, int64(len(value.Data))+int64(unsafe.Sizeof(coprCacheValue{})))
}
