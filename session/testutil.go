// Copyright 2023 PingCAP, Inc.
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

//go:build intest

package session

import (
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

var (
	// GetBootstrapVersion is used in test
	GetBootstrapVersion = getBootstrapVersion
	// CurrentBootstrapVersion is used in test
	CurrentBootstrapVersion = currentBootstrapVersion
)

func CreateStoreAndBootstrap(t *testing.T) (kv.Storage, *domain.Domain) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	return store, dom
}

var sessionKitIDGenerator atomic.Uint64

func CreateSessionAndSetID(t *testing.T, store kv.Storage) Session {
	se, err := CreateSession4Test(store)
	se.SetConnectionID(sessionKitIDGenerator.Inc())
	require.NoError(t, err)
	return se
}
