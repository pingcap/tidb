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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSPFreshTransportReusesConcurrentConnections(t *testing.T) {
	for _, tls := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls_%t", tls), func(t *testing.T) {
			const concurrency = 64
			entered := make(chan struct{}, concurrency)
			gates := map[string]chan struct{}{"/first": make(chan struct{}), "/second": make(chan struct{})}
			var opened, redirected atomic.Int32
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/redirect" {
					http.Redirect(w, r, "/unexpected", http.StatusTemporaryRedirect)
					return
				}
				gate, ok := gates[r.URL.Path]
				if !ok {
					redirected.Add(1)
					return
				}
				entered <- struct{}{}
				select {
				case <-gate:
					_, _ = io.WriteString(w, "complete response")
				case <-r.Context().Done():
				}
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					opened.Add(1)
				}
			}
			if tls {
				server.StartTLS()
			} else {
				server.Start()
			}
			defer server.Close()
			base := &http.Client{Timeout: 10 * time.Second}
			if tls {
				base.Transport = server.Client().Transport
			}
			client := newSPFreshSearchClient(base)
			defer client.CloseIdleConnections()
			require.Equal(t, base.Timeout, client.Timeout)
			if tls {
				require.Zero(t, base.Transport.(*http.Transport).MaxIdleConnsPerHost)
			} else {
				require.Nil(t, base.Transport)
			}
			for _, path := range []string{"/first", "/second"} {
				errors := make(chan error, concurrency)
				for range concurrency {
					go func() {
						resp, err := client.Get(server.URL + path)
						if err == nil {
							_, err = io.Copy(io.Discard, resp.Body)
							closeErr := resp.Body.Close()
							if err == nil {
								err = closeErr
							}
						}
						errors <- err
					}()
				}
				for range concurrency {
					select {
					case <-entered:
					case <-time.After(10 * time.Second):
						t.Fatal("concurrent requests did not reach the worker")
					}
				}
				close(gates[path])
				for range concurrency {
					require.NoError(t, <-errors)
				}
				require.EqualValues(t, concurrency, opened.Load(), "second burst must reuse the first burst's sockets")
			}
			response, err := client.Get(server.URL + "/redirect")
			require.NoError(t, err)
			require.Equal(t, http.StatusTemporaryRedirect, response.StatusCode)
			require.NoError(t, response.Body.Close())
			require.Zero(t, redirected.Load())
		})
	}
}
