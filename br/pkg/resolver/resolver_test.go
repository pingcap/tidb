// Copyright 2022 PingCAP, Inc.
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

package resolver

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/stretchr/testify/require"
	grpcresolver "google.golang.org/grpc/resolver"
)

type mockClientConn struct {
	grpcresolver.ClientConn
	state grpcresolver.State
}

func (c *mockClientConn) UpdateState(state grpcresolver.State) error {
	c.state = state
	return nil
}

type mockSplitClient struct {
	restore.SplitClient
	addr    string
	newAddr string
}

func (c *mockSplitClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	store := &metapb.Store{
		Id:      storeID,
		Address: c.addr,
	}
	return store, nil
}

func (c *mockSplitClient) InvalidateStoreCache(storeID uint64) {
	c.addr = c.newAddr
}

func TestResolver(t *testing.T) {
	oldAddr := "192.168.1.1:20160"
	newAddr := "192.168.1.2:20160"
	sc := &mockSplitClient{
		addr:    oldAddr,
		newAddr: newAddr,
	}
	b := NewBuilder(sc)

	target := b.Target(1)
	u, err := url.Parse(target)
	require.NoError(t, err)
	require.Equal(t, b.Scheme(), u.Scheme)
	require.Equal(t, "1", u.Host)

	conn := &mockClientConn{}
	rs, err := b.Build(grpcresolver.Target{URL: *u}, conn, grpcresolver.BuildOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, conn.state.Addresses)
	require.Equal(t, oldAddr, conn.state.Addresses[0].Addr)

	conn.state.Addresses = nil
	rs.ResolveNow(grpcresolver.ResolveNowOptions{})
	require.NotEmpty(t, conn.state.Addresses)
	require.Equal(t, newAddr, conn.state.Addresses[0].Addr)
}
