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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mockstore

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type clientRedirector struct {
	mockClient tikv.Client
	sync.Once
	rpcClient tikv.Client
}

// newClientRedirector wraps a mock tikv client and redirects
// all TiDB requests using a rpcClient.
// It is a workaround for mock clients does not support TiDB
// RPC services.
func newClientRedirector(mockClient tikv.Client) tikv.Client {
	return &clientRedirector{
		mockClient: mockClient,
	}
}

func (c *clientRedirector) Close() error {
	err := c.mockClient.Close()
	if err != nil {
		return err
	}
	if c.rpcClient != nil {
		err = c.rpcClient.Close()
	}
	return err
}

func (c *clientRedirector) CloseAddr(addr string) error {
	err := c.mockClient.CloseAddr(addr)
	if err != nil {
		return err
	}
	if c.rpcClient != nil {
		err = c.rpcClient.CloseAddr(addr)
	}
	return err
}

func (c *clientRedirector) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.StoreTp == tikvrpc.TiDB {
		c.Once.Do(func() {
			c.rpcClient = tikv.NewRPCClient(tikv.WithSecurity(config.GetGlobalConfig().Security.ClusterSecurity()))
		})
		return c.rpcClient.SendRequest(ctx, addr, req, timeout)
	}
	return c.mockClient.SendRequest(ctx, addr, req, timeout)
}
