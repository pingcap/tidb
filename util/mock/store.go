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
	"context"

	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// Store implements kv.Storage interface.
type Store struct {
	Client kv.Client
}

// GetClient implements kv.Storage interface.
func (s *Store) GetClient() kv.Client { return s.Client }

// GetMPPClient implements kv.Storage interface.
func (s *Store) GetMPPClient() kv.MPPClient { return nil }

// GetOracle implements kv.Storage interface.
func (s *Store) GetOracle() oracle.Oracle { return nil }

// Begin implements kv.Storage interface.
func (s *Store) Begin() (kv.Transaction, error) { return nil, nil }

// BeginWithOption implements kv.Storage interface.
func (s *Store) BeginWithOption(option tikv.StartTSOption) (kv.Transaction, error) {
	return s.Begin()
}

// GetSnapshot implements kv.Storage interface.
func (s *Store) GetSnapshot(ver kv.Version) kv.Snapshot { return nil }

// Close implements kv.Storage interface.
func (s *Store) Close() error { return nil }

// UUID implements kv.Storage interface.
func (s *Store) UUID() string { return "mock" }

// CurrentVersion implements kv.Storage interface.
func (s *Store) CurrentVersion(txnScope string) (kv.Version, error) { return kv.Version{}, nil }

// SupportDeleteRange implements kv.Storage interface.
func (s *Store) SupportDeleteRange() bool { return false }

// Name implements kv.Storage interface.
func (s *Store) Name() string { return "UtilMockStorage" }

// Describe implements kv.Storage interface.
func (s *Store) Describe() string {
	return "UtilMockStorage is a mock Store implementation, only for unittests in util package"
}

// GetMemCache implements kv.Storage interface
func (s *Store) GetMemCache() kv.MemManager {
	return nil
}

// ShowStatus implements kv.Storage interface.
func (s *Store) ShowStatus(ctx context.Context, key string) (interface{}, error) { return nil, nil }

// GetMinSafeTS implements kv.Storage interface.
func (s *Store) GetMinSafeTS(txnScope string) uint64 {
	return 0
}

// GetLockWaits implements kv.Storage interface.
func (s *Store) GetLockWaits() ([]*deadlockpb.WaitForEntry, error) {
	return nil, nil
}
