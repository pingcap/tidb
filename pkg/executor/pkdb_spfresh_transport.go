// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"net/http"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/util"
)

var spfreshSearchClient = sync.OnceValue(func() *http.Client {
	return newSPFreshSearchClient(util.InternalHTTPClient())
})

// Search bursts should reuse their worker connections after responses complete.
// Bound retained idle sockets independently of the active SQL concurrency limit.
func newSPFreshSearchClient(base *http.Client) *http.Client {
	source := base.Transport
	if source == nil {
		// The current plaintext internal client uses the default transport.
		source = http.DefaultTransport
	}
	transport := source.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 128
	transport.MaxIdleConns = 512
	transport.IdleConnTimeout = 90 * time.Second
	client := *base
	client.Transport = transport
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &client
}
