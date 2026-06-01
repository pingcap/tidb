// Copyright 2025 PingCAP, Inc.
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

package metaservice_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/metaservice"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// TestGetPDAddrsWithRealClient tests the GetPDAddrs method with a real etcd client
func TestGetPDAddrsWithRealClient(t *testing.T) {
	integration.BeforeTestExternal(t)
	if !unixSocketAvailable() {
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

	serviceClient := metaservice.NewEtcdMetaServiceClient(etcdCli, pdCli)
	addrs, err := serviceClient.GetPDAddrs()
	require.NoError(t, err)
	require.Equal(t, expectAddrs, addrs)
}

// TestGetPDLeaderAddrsWithRealClient tests the GetPDLeaderAddrs method with a real etcd client
func TestGetPDLeaderAddrsWithRealClient(t *testing.T) {
	integration.BeforeTestExternal(t)
	if !unixSocketAvailable() {
		t.Skip("ETCD use ip:port as unix socket address, skip when it is unavailable.")
	}

	// Initialize etcd client
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	etcdCli := cluster.RandClient()

	serviceClient := metaservice.NewEtcdMetaServiceClient(etcdCli, nil)
	ctx := context.Background()

	leaderAddr, err := serviceClient.GetPDLeaderAddrs(ctx)

	require.NoError(t, err)
	require.NotEmpty(t, leaderAddr, "Leader address should not be empty")
}

func TestGetPDLeaderAddrsReturnsErrorWhenLeaderNotFound(t *testing.T) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:1"},
		DialTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, etcdCli.Close())
	}()

	serviceClient := metaservice.NewEtcdMetaServiceClient(etcdCli, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	leaderAddr, err := serviceClient.GetPDLeaderAddrs(ctx)

	require.Error(t, err)
	require.ErrorContains(t, err, "pd leader not found")
	require.Empty(t, leaderAddr)
}

// TestParseURL tests the ParseURL function with various inputs.
func TestParseURL(t *testing.T) {
	tests := []struct {
		rawURL string
		prefix string
		host   string
		port   string
		err    bool
	}{
		// Successful test cases
		{"unix://localhost:m0", "unix://", "localhost", "m0", false},
		{"http://example.com:8080", "http://", "example.com", "8080", false},
		{"https://example.com", "https://", "example.com", "443", false}, // Default port for HTTPS
		{"http://localhost", "http://", "localhost", "80", false},        // Default port for HTTP
		{"https://localhost:443", "https://", "localhost", "443", false}, // Specified port for HTTPS
		{"http://[2001:db8::1]:2379", "http://", "[2001:db8::1]", "2379", false},
		{"https://[2001:db8::1]", "https://", "[2001:db8::1]", "443", false},

		// Unsuccessful test cases
		{"ftp://example.com", "ftp://", "", "", true},              // Invalid prefix
		{"unix://localhost", "unix://", "localhost", "", true},     // Missing port
		{"http://example.com:8080:extra", "http://", "", "", true}, // Extra part after port
		{"https://:8080", "https://", "", "", true},                // Missing host
		{"http://", "http://", "", "", true},                       // Incomplete URL
		{"http://2001:db8::1:2379", "http://", "", "", true},       // Unbracketed IPv6 with port
		{"https://[2001:db8::1", "https://", "", "", true},         // Invalid bracketed IPv6
	}

	for _, test := range tests {
		prefix, host, port, err := metaservice.ParseURL(test.rawURL)

		// Check if the error status matches the expectation
		if test.err {
			require.Error(t, err, "Expected an error for input: "+test.rawURL)
			require.Empty(t, prefix, "Expected an error for input: "+test.rawURL)
			require.Empty(t, host, "Host should be empty for input: "+test.rawURL)
			require.Empty(t, port, "Port should be empty for input: "+test.rawURL)
		} else {
			require.NoError(t, err, "Did not expect an error for input: "+test.rawURL)
			require.Equal(t, test.prefix, prefix, "prefix mismatch for input: "+test.rawURL)
			require.Equal(t, test.host, host, "Host mismatch for input: "+test.rawURL)
			require.Equal(t, test.port, port, "Port mismatch for input: "+test.rawURL)
		}
	}
}
