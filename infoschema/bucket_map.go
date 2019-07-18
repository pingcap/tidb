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

package infoschema

import (
	"fmt"
	"hash/fnv"
	"strings"
)

type bucketMap struct {
	s []map[string]interface{}
}

func (bm *bucketMap) Spawn() *bucketMap {
	// Spawn do not make any copy
	newBM := NewBucketMap()
	copy(newBM.s, bm.s)
	for i, m := range bm.s {
		newBM.s[i] = make(map[string]interface{})
		for k, v := range m {
			newBM.s[i][k] = v
		}
	}
	return newBM
}

func NewBucketMap() *bucketMap {
	s := make([]map[string]interface{}, bucketCount)
	for idx, _ := range s {
		s[idx] = make(map[string]interface{})
	}
	return &bucketMap{s}
}

func (bm *bucketMap) Set(key string, value interface{}) {
	idx := bucketIndex(key)
	bm.s[idx][key] = value
}

func (bm *bucketMap) Get(key string) (interface{}, bool) {
	idx := bucketIndex(key)
	v, ok := bm.s[idx][key]
	return v, ok
}

func (bm bucketMap) String() string {
	var s []string
	for idx, m := range bm.s {
		for k, v := range m {
			s = append(s, fmt.Sprintf("|%v|%v -> %v", idx, k, v))
		}
	}
	return "[" + strings.Join(s, ", ") + "]"
}

func (bm *bucketMap) Delete(key string) {
	idx := bucketIndex(key)
	delete(bm.s[idx], key)
}

func bucketIndex(s string) uint32 {
	return hashcode(s) % bucketCount
}

func hashcode(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
