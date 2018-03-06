// Copyright 2018 PingCAP, Inc.
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

package mock

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

type Store struct {
	Client kv.Client
}

// functions for Store.
func (s *Store) GetClient() kv.Client                                    { return s.Client }
func (s *Store) GetOracle() oracle.Oracle                                { return nil }
func (s *Store) Begin() (kv.Transaction, error)                          { return nil, nil }
func (s *Store) BeginWithStartTS(startTS uint64) (kv.Transaction, error) { return s.Begin() }
func (s *Store) GetSnapshot(ver kv.Version) (kv.Snapshot, error)         { return nil, nil }
func (s *Store) Close() error                                            { return nil }
func (s *Store) UUID() string                                            { return "mock" }
func (s *Store) CurrentVersion() (kv.Version, error)                     { return kv.Version{}, nil }
func (s *Store) SupportDeleteRange() bool                                { return false }
