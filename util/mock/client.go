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

	"github.com/pingcap/tidb/kv"
)

// Client implement kv.Client interface, mocked from "CopClient" defined in
// "store/tikv/copprocessor.go".
type Client struct {
	kv.RequestTypeSupportedChecker
	MockResponse kv.Response
}

// Send implement kv.Client interface.
<<<<<<< HEAD
func (c *Client) Send(ctx context.Context, req *kv.Request, kv *kv.Variables, sessionMemTracker *memory.Tracker, enabledRateLimit bool, eventCb trxevents.EventCallback) kv.Response {
=======
func (c *Client) Send(ctx context.Context, req *kv.Request, kv interface{}, option *kv.ClientSendOption) kv.Response {
>>>>>>> 2bbeebd0d... store: forbid collecting info if enable-collect-execution-info disabled (#31282)
	return c.MockResponse
}
