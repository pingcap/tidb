// Copyright 2021 PingCAP, Inc.
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

package txn

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

type tikvSnapshot struct {
	*tikv.KVSnapshot
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
func (s *tikvSnapshot) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	data, err := s.KVSnapshot.BatchGet(ctx, keys)
	return data, extractKeyErr(err)
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	data, err := s.KVSnapshot.Get(ctx, k)
	return data, extractKeyErr(err)
}

// Iter return a list of key-value pair after `k`.
func (s *tikvSnapshot) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	scanner, err := s.KVSnapshot.Iter(k, upperBound)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &tikvScanner{scanner.(*tikv.Scanner)}, err
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *tikvSnapshot) IterReverse(k kv.Key) (kv.Iterator, error) {
	scanner, err := s.KVSnapshot.IterReverse(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &tikvScanner{scanner.(*tikv.Scanner)}, err
}
