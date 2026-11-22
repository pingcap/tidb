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

package advertisedstatus

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestEndpointURL(t *testing.T) {
	testCases := []struct {
		name    string
		network string
		host    string
	}{
		{name: "IPv4", network: "tcp4", host: "127.0.0.1"},
		{name: "IPv6", network: "tcp6", host: "::1"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			listener, err := net.Listen(testCase.network, net.JoinHostPort(testCase.host, "0"))
			if err != nil && testCase.network == "tcp6" {
				t.Skipf("IPv6 loopback is unavailable: %v", err)
			}
			require.NoError(t, err)

			requests := make(chan [2]string, 1)
			server, connectionClosed := newEndpointTestServer(t, listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- [2]string{r.Host, r.URL.Path}
				_, _ = io.WriteString(w, `{"ddl_id":"local-id"}`)
			}))
			reports := make(chan endpointTestReport, 1)
			ctx := context.WithValue(t.Context(), testReporterKey{}, func(input endpointCheckInput, result endpointCheckResult) {
				reports <- endpointTestReport{input: input, result: result}
			})

			Start(ctx, Options{
				ReportStatus:     true,
				StatusListener:   server.Listener,
				AdvertiseAddress: testCase.host,
				LocalID:          "local-id",
			})

			select {
			case request := <-requests:
				require.Equal(t, server.Listener.Addr().String(), request[0])
				require.Equal(t, "/info", request[1])
			case <-time.After(time.Second):
				require.FailNow(t, "advertised endpoint was not requested")
			}
			select {
			case <-connectionClosed:
			case <-time.After(time.Second):
				require.FailNow(t, "advertised endpoint connection was not closed")
			}
			select {
			case report := <-reports:
				require.Failf(t, "identity match was reported as a warning", "reason: %s", report.result.reason)
			default:
			}
		})
	}
}

func TestStartPrerequisites(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, `{"ddl_id":"local-id"}`)
	}))
	t.Cleanup(server.Close)

	baseOptions := endpointTestOptions(t, server.Listener, "local-id")
	testCases := []struct {
		name   string
		update func(*Options)
	}{
		{name: "report status disabled", update: func(options *Options) { options.ReportStatus = false }},
		{name: "listener missing", update: func(options *Options) { options.StatusListener = nil }},
		{name: "advertise address missing", update: func(options *Options) { options.AdvertiseAddress = "" }},
		{name: "local identity missing", update: func(options *Options) { options.LocalID = "" }},
	}

	var reports atomic.Int32
	ctx := context.WithValue(t.Context(), testReporterKey{}, func(endpointCheckInput, endpointCheckResult) {
		reports.Add(1)
	})
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			options := baseOptions
			testCase.update(&options)
			Start(ctx, options)
		})
	}
	require.Never(t, func() bool {
		return requests.Load() != 0 || reports.Load() != 0
	}, 100*time.Millisecond, 10*time.Millisecond)
}

func TestEndpointCheckResponses(t *testing.T) {
	testCases := []struct {
		name             string
		statusCode       int
		body             string
		expectedReason   endpointCheckReason
		expectedRemoteID string
	}{
		{
			name:             "identity match",
			statusCode:       http.StatusOK,
			body:             `{"ddl_id":"local-id"}`,
			expectedRemoteID: "local-id",
		},
		{
			name:             "identity mismatch",
			statusCode:       http.StatusOK,
			body:             `{"ddl_id":"remote-id"}`,
			expectedReason:   endpointIdentityMismatch,
			expectedRemoteID: "remote-id",
		},
		{
			name:           "missing identity",
			statusCode:     http.StatusOK,
			body:           `{"is_owner":false}`,
			expectedReason: endpointMissingIdentity,
		},
		{
			name:           "malformed JSON",
			statusCode:     http.StatusOK,
			body:           `{"ddl_id":`,
			expectedReason: endpointInvalidResponse,
		},
		{
			name:           "oversized body",
			statusCode:     http.StatusOK,
			body:           `{"ddl_id":"` + strings.Repeat("x", endpointResponseBodyLimit) + `"}`,
			expectedReason: endpointInvalidResponse,
		},
		{
			name:           "non-2xx status",
			statusCode:     http.StatusInternalServerError,
			body:           `{"ddl_id":"local-id"}`,
			expectedReason: endpointUnexpectedStatus,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/info" {
					http.NotFound(w, r)
					return
				}
				w.WriteHeader(testCase.statusCode)
				_, _ = io.WriteString(w, testCase.body)
			}))
			t.Cleanup(server.Close)

			client := newEndpointTestClient(t)
			result := checkEndpoint(t.Context(), client, server.URL+"/info", "local-id")
			require.Equal(t, testCase.expectedReason, result.reason)
			require.Equal(t, testCase.expectedRemoteID, result.remoteID)
			if testCase.expectedReason == endpointUnexpectedStatus {
				require.Equal(t, "500 Internal Server Error", result.status)
			}
		})
	}
}

func TestEndpointCheckRedirect(t *testing.T) {
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		_, _ = io.WriteString(w, `{"ddl_id":"local-id"}`)
	}))
	t.Cleanup(target.Close)

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/info", http.StatusFound)
	}))
	t.Cleanup(redirect.Close)

	client := newEndpointTestClient(t)
	result := checkEndpoint(t.Context(), client, redirect.URL+"/info", "local-id")
	require.Equal(t, endpointUnexpectedStatus, result.reason)
	require.Equal(t, "302 Found", result.status)
	require.Zero(t, targetRequests.Load())
}

func TestEndpointCheckRequestFailures(t *testing.T) {
	t.Run("connection failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		endpoint := server.URL + "/info"
		server.Close()

		client := newEndpointTestClient(t)
		result := checkEndpoint(t.Context(), client, endpoint, "local-id")
		require.Equal(t, endpointRequestFailed, result.reason)
		require.Error(t, result.err)
	})

	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}))
		t.Cleanup(server.Close)

		client := newEndpointTestClient(t)
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		result := checkEndpoint(ctx, client, server.URL+"/info", "local-id")
		require.Equal(t, endpointRequestFailed, result.reason)
		require.ErrorIs(t, result.err, context.DeadlineExceeded)
	})

	t.Run("response body read failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, `{"ddl_id":"local-id"}`)
		}))
		t.Cleanup(server.Close)

		client := newEndpointTestClient(t)
		result := checkEndpoint(t.Context(), client, server.URL+"/info", "local-id")
		require.Equal(t, endpointRequestFailed, result.reason)
		require.ErrorIs(t, result.err, io.ErrUnexpectedEOF)
	})

	t.Run("lifecycle cancellation", func(t *testing.T) {
		requestStarted := make(chan struct{})
		handlerExited := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			close(requestStarted)
			<-r.Context().Done()
			close(handlerExited)
		}))
		t.Cleanup(server.Close)

		client := newEndpointTestClient(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		resultCh := make(chan endpointCheckResult, 1)
		go func() {
			resultCh <- checkEndpoint(ctx, client, server.URL+"/info", "local-id")
		}()
		select {
		case <-requestStarted:
		case <-time.After(time.Second):
			require.FailNow(t, "advertised endpoint was not requested")
		}
		cancel()

		select {
		case result := <-resultCh:
			require.Equal(t, endpointRequestFailed, result.reason)
			require.ErrorIs(t, result.err, context.Canceled)
		case <-time.After(time.Second):
			require.FailNow(t, "request did not stop after lifecycle cancellation")
		}
		select {
		case <-handlerExited:
		case <-time.After(time.Second):
			require.FailNow(t, "handler context was not canceled")
		}
	})

	t.Run("TLS verification failure", func(t *testing.T) {
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"ddl_id":"local-id"}`)
		}))
		server.Config.ErrorLog = log.New(io.Discard, "", 0)
		server.StartTLS()
		t.Cleanup(server.Close)

		client := newEndpointTestClient(t)
		result := checkEndpoint(t.Context(), client, server.URL+"/info", "local-id")
		require.Equal(t, endpointRequestFailed, result.reason)
		require.Error(t, result.err)
	})
}

func TestHTTPClientPreservesTLS(t *testing.T) {
	var clientCertificateSeen atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientCertificateSeen.Store(r.TLS != nil && len(r.TLS.PeerCertificates) == 1)
		_, _ = io.WriteString(w, `{"ddl_id":"local-id"}`)
	}))
	server.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)

	baseTransport := server.Client().Transport.(*http.Transport).Clone()
	baseTLSConfig := baseTransport.TLSClientConfig
	baseTLSConfig.Certificates = []tls.Certificate{server.TLS.Certificates[0]}
	setEndpointTestTransport(t, baseTransport)
	client := newEndpointTestClient(t)
	require.NotSame(t, baseTransport, client.Transport)
	require.Equal(t, endpointCheckTimeout, client.Timeout)
	result := checkEndpoint(t.Context(), client, server.URL+"/info", "local-id")
	require.Empty(t, result.reason)
	require.NoError(t, result.err)
	require.True(t, clientCertificateSeen.Load())
}

func TestHTTPClientBypassesProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://proxy.invalid")
	t.Setenv("NO_PROXY", "")

	var endpointRequests atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		endpointRequests.Add(1)
		_, _ = io.WriteString(w, `{"ddl_id":"local-id"}`)
	}))
	t.Cleanup(endpoint.Close)

	var proxyRequests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyRequests.Add(1)
		_, _ = io.WriteString(w, `{"ddl_id":"proxy-id"}`)
	}))
	t.Cleanup(proxy.Close)
	proxyURL, err := url.Parse(proxy.URL)
	require.NoError(t, err)

	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.Proxy = func(*http.Request) (*url.URL, error) { return proxyURL, nil }
	setEndpointTestTransport(t, baseTransport)
	client := newEndpointTestClient(t)
	require.NotNil(t, baseTransport.Proxy)
	require.Nil(t, client.Transport.(*http.Transport).Proxy)

	result := checkEndpoint(t.Context(), client, endpoint.URL+"/info", "local-id")
	require.Empty(t, result.reason)
	require.Equal(t, int32(1), endpointRequests.Load())
	require.Zero(t, proxyRequests.Load())
}

func TestStartLifecycle(t *testing.T) {
	t.Run("cancellation stops without reporting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var reports atomic.Int32
		var requests atomic.Int32
		requestStarted := make(chan struct{})
		handlerExited := make(chan struct{})
		server, connectionClosed := newEndpointTestServer(t, nil, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			close(requestStarted)
			<-r.Context().Done()
			close(handlerExited)
		}))
		ctx = context.WithValue(ctx, testReporterKey{}, func(endpointCheckInput, endpointCheckResult) {
			reports.Add(1)
		})
		options := endpointTestOptions(t, server.Listener, "local-id")

		returned := make(chan struct{})
		go func() {
			Start(ctx, options)
			close(returned)
		}()
		select {
		case <-returned:
		case <-time.After(time.Second):
			require.FailNow(t, "Start waited for the endpoint check")
		}
		select {
		case <-requestStarted:
		case <-time.After(time.Second):
			require.FailNow(t, "advertised endpoint was not requested")
		}

		cancel()
		select {
		case <-handlerExited:
		case <-time.After(time.Second):
			require.FailNow(t, "handler context was not canceled")
		}
		select {
		case <-connectionClosed:
		case <-time.After(time.Second):
			require.FailNow(t, "canceled endpoint connection was not closed")
		}
		require.Never(t, func() bool { return reports.Load() != 0 }, 100*time.Millisecond, 10*time.Millisecond)
		require.Equal(t, int32(1), requests.Load())
		require.Zero(t, reports.Load())
	})

	t.Run("completed failure reports once", func(t *testing.T) {
		var requests atomic.Int32
		var reports atomic.Int32
		server, connectionClosed := newEndpointTestServer(t, nil, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			_, _ = io.WriteString(w, `{"ddl_id":"remote-id"}`)
		}))
		reported := make(chan endpointTestReport, 2)
		ctx := context.WithValue(t.Context(), testReporterKey{}, func(input endpointCheckInput, result endpointCheckResult) {
			reports.Add(1)
			reported <- endpointTestReport{input: input, result: result}
		})
		Start(ctx, endpointTestOptions(t, server.Listener, "local-id"))

		select {
		case report := <-reported:
			require.Equal(t, server.URL+"/info", report.input.endpoint)
			require.Equal(t, "local-id", report.input.localID)
			require.Equal(t, endpointIdentityMismatch, report.result.reason)
			require.Equal(t, "remote-id", report.result.remoteID)
		case <-time.After(time.Second):
			require.FailNow(t, "completed failure was not reported")
		}
		select {
		case <-connectionClosed:
		case <-time.After(time.Second):
			require.FailNow(t, "advertised endpoint connection was not closed")
		}
		require.Never(t, func() bool {
			return requests.Load() > 1 || reports.Load() > 1
		}, 100*time.Millisecond, 10*time.Millisecond)
		require.Equal(t, int32(1), requests.Load())
		require.Equal(t, int32(1), reports.Load())
	})
}

type endpointTestReport struct {
	input  endpointCheckInput
	result endpointCheckResult
}

func newEndpointTestClient(t *testing.T) *http.Client {
	t.Helper()
	client := newEndpointHTTPClient()
	t.Cleanup(client.CloseIdleConnections)
	return client
}

// Tests using this helper must not run in parallel because InternalHTTPClient is process-global.
func setEndpointTestTransport(t *testing.T, transport *http.Transport) {
	t.Helper()
	internalClient := util.InternalHTTPClient()
	originalTransport := internalClient.Transport
	internalClient.Transport = transport
	t.Cleanup(func() {
		internalClient.Transport = originalTransport
	})
}

func endpointTestOptions(t *testing.T, listener net.Listener, localID string) Options {
	t.Helper()
	advertiseAddress, _, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	return Options{
		ReportStatus:     true,
		StatusListener:   listener,
		AdvertiseAddress: advertiseAddress,
		LocalID:          localID,
	}
}

func newEndpointTestServer(
	t *testing.T,
	listener net.Listener,
	handler http.Handler,
) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	connectionClosed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(handler)
	if listener != nil {
		server.Listener = listener
	}
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case connectionClosed <- struct{}{}:
			default:
			}
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	return server, connectionClosed
}
