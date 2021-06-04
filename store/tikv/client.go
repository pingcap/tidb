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

package tikv

import (
	"github.com/pingcap/tidb/store/tikv/client"
	"github.com/pingcap/tidb/store/tikv/config"
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client = client.Client

// Timeout durations.
const (
	ReadTimeoutMedium = client.ReadTimeoutMedium
	ReadTimeoutShort  = client.ReadTimeoutShort
)

// NewTestRPCClient is for some external tests.
func NewTestRPCClient(security config.Security) Client {
	return client.NewTestRPCClient(security)
}

// NewRPCClient creates a client that manages connections and rpc calls with tikv-servers.
func NewRPCClient(security config.Security, opts ...func(c *client.RPCClient)) *client.RPCClient {
	return client.NewRPCClient(security, opts...)
}
