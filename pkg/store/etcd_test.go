// Copyright 2024 PingCAP, Inc.
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

package store

import (
	"crypto/tls"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

type mockEtcdBackend struct {
	kv.Storage
	kv.EtcdBackend
	pdAddrs   []string
	metaAddrs []string
}

func (mebd *mockEtcdBackend) EtcdAddrs() ([]string, error) {
	return mebd.metaAddrs, nil
}

func (mebd *mockEtcdBackend) GetPDAddrs() ([]string, error) {
	return mebd.pdAddrs, nil
}

func (*mockEtcdBackend) TLSConfig() *tls.Config { return nil }

func (*mockEtcdBackend) StartGCWorker() error { return nil }

func TestNewEtcdCliGetEtcdAddrs(t *testing.T) {
	etcdStore, addrs, err := GetEtcdAddrs(nil)
	require.NoError(t, err)
	require.Empty(t, addrs)
	require.Nil(t, etcdStore)

	etcdStore, addrs, err = GetEtcdAddrs(&mockEtcdBackend{
		pdAddrs:   []string{"localhost:2379"},
		metaAddrs: []string{"localhost:2389"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"localhost:2389"}, addrs)
	require.NotNil(t, etcdStore)

	cli, err := NewEtcdCli(nil)
	require.NoError(t, err)
	require.Nil(t, cli)
}
