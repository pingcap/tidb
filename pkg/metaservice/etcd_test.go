// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metaservice

import (
	"context"
	"net"
	"runtime"
	"testing"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/tests/v3/integration"
)

type mockPDClient struct {
	pd.Client
	members []*pdpb.Member
}

func (c *mockPDClient) GetAllMembers(context.Context) (*pdpb.GetMembersResponse, error) {
	return &pdpb.GetMembersResponse{Members: c.members}, nil
}

// ETCD use ip:port as unix socket address, however this address is invalid on windows.
// We have to skip some of the test in such case.
// https://github.com/etcd-io/etcd/blob/f0faa5501d936cd8c9f561bb9d1baca70eb67ab1/pkg/types/urls.go#L42
func unixSocketAvailable() bool {
	c, err := net.Listen("unix", "127.0.0.1:0")
	if err == nil {
		_ = c.Close()
		return true
	}
	return false
}

func TestGetPDAddrsPDOnlyClient(t *testing.T) {
	expectAddrs := []string{"127.0.0.1:1111"}
	pdCli := &mockPDClient{
		members: []*pdpb.Member{{
			ClientUrls: []string{"http://127.0.0.1:1111"},
		}},
	}

	serviceClient := newClient(nil, pdCli)
	require.NotNil(t, serviceClient)

	addrs, err := serviceClient.GetPDAddrs(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectAddrs, addrs)

	httpAddrs, err := serviceClient.GetPDHttpAddrs(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"http://127.0.0.1:1111"}, httpAddrs)
}

func TestNewClientReturnsNilWithoutClients(t *testing.T) {
	require.Nil(t, newClient(nil, nil))
}

// TestGetPDAddrsWithRealClient tests the GetPDAddrs method with a real etcd client
func TestGetPDAddrsWithRealClient(t *testing.T) {
	integration.BeforeTestExternal(t)
	if runtime.GOOS == "windows" {
		t.Skip("ETCD use ip:port as unix socket address, skip when it is unavailable.")
	}

	// Initialize etcd client
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	etcdCli := cluster.RandClient()

	expectAddrs := []string{"127.0.0.1:1111"}
	pdCli := &mockPDClient{
		members: []*pdpb.Member{{
			ClientUrls: []string{"http://127.0.0.1:1111"},
		}},
	}

	serviceClient := newClient(etcdCli, pdCli)
	addrs, err := serviceClient.GetPDAddrs(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectAddrs, addrs)

	t.Run("empty client urls returns error", func(t *testing.T) {
		pdCli := &mockPDClient{
			members: []*pdpb.Member{
				{},
				{ClientUrls: []string{}},
			},
		}

		serviceClient := newClient(etcdCli, pdCli)
		addrs, err := serviceClient.GetPDAddrs(context.Background())
		require.Error(t, err)
		require.Nil(t, addrs)
		require.EqualError(t, err, "no usable PD client URL found in PD members")
	})
}

// TestParseURL tests the ParseURL function with various inputs.
func TestParseURL(t *testing.T) {
	tests := []struct {
		rawURL   string
		prefix   string
		hostPort string
		err      bool
	}{
		// Successful test cases
		{"http://example.com:8080", "http://", "example.com:8080", false},
		{"https://localhost:443", "https://", "localhost:443", false},
		{"http://[2001:db8::1]:2379", "http://", "[2001:db8::1]:2379", false},
		{"https://[2001:db8::1]:443", "https://", "[2001:db8::1]:443", false},

		// Unsuccessful test cases
		{"ftp://example.com", "ftp://", "", true},              // Invalid prefix
		{"unix://localhost:m0", "unix://", "", true},           // Unix schema is unsupported
		{"unix://localhost", "unix://", "", true},              // Unix schema is unsupported
		{"http://example.com:8080:extra", "http://", "", true}, // Extra part after port
		{"https://:8080", "https://", "", true},                // Missing host
		{"http://", "http://", "", true},                       // Incomplete URL
		{"https://example.com", "https://", "", true},          // Missing port
		{"http://localhost", "http://", "", true},              // Missing port
		{"https://[2001:db8::1]", "https://", "", true},        // Missing port
		{"http://2001:db8::1:2379", "http://", "", true},       // Unbracketed IPv6 with port
		{"https://[2001:db8::1", "https://", "", true},         // Invalid bracketed IPv6
	}

	for _, test := range tests {
		prefix, hostPort, err := ParseURL(test.rawURL)

		// Check if the error status matches the expectation
		if test.err {
			require.Error(t, err, "Expected an error for input: "+test.rawURL)
			require.Empty(t, prefix, "Expected an error for input: "+test.rawURL)
			require.Empty(t, hostPort, "hostPort should be empty for input: "+test.rawURL)
		} else {
			require.NoError(t, err, "Did not expect an error for input: "+test.rawURL)
			require.Equal(t, test.prefix, prefix, "prefix mismatch for input: "+test.rawURL)
			require.Equal(t, test.hostPort, hostPort, "hostPort mismatch for input: "+test.rawURL)
		}
	}
}
