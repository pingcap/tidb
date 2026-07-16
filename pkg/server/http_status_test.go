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

package server

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdvertisedStatusEndpointURL(t *testing.T) {
	testCases := []struct {
		name     string
		host     string
		port     int
		expected string
	}{
		{name: "IPv4", host: "192.0.2.1", port: 10080, expected: "http://192.0.2.1:10080/info"},
		{name: "IPv6", host: "2001:db8::1", port: 10081, expected: "http://[2001:db8::1]:10081/info"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			endpoint, err := buildAdvertisedStatusEndpointURL("http", testCase.host, testCase.port)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, endpoint)
		})
	}
}

func TestAdvertisedStatusEndpointCheckInput(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	port := listener.Addr().(*net.TCPAddr).Port
	input, ok := newAdvertisedStatusEndpointCheckInput(true, listener, "2001:db8::1", "local-id", "https")
	require.True(t, ok)
	require.Equal(t, "https://[2001:db8::1]:"+strconv.Itoa(port)+"/info", input.endpoint)
	require.Equal(t, "local-id", input.localID)

	zeroPortListener := &advertisedStatusEndpointTestListener{addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}}
	testCases := []struct {
		name             string
		reportStatus     bool
		listener         net.Listener
		advertiseAddress string
		localID          string
	}{
		{name: "report status disabled", listener: listener, advertiseAddress: "127.0.0.1", localID: "local-id"},
		{name: "listener missing", reportStatus: true, advertiseAddress: "127.0.0.1", localID: "local-id"},
		{name: "effective port missing", reportStatus: true, listener: zeroPortListener, advertiseAddress: "127.0.0.1", localID: "local-id"},
		{name: "advertise address missing", reportStatus: true, listener: listener, localID: "local-id"},
		{name: "local identity missing", reportStatus: true, listener: listener, advertiseAddress: "127.0.0.1"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, ok := newAdvertisedStatusEndpointCheckInput(
				testCase.reportStatus,
				testCase.listener,
				testCase.advertiseAddress,
				testCase.localID,
				"http",
			)
			require.False(t, ok)
		})
	}
}

func TestAdvertisedStatusEndpointCheckResponses(t *testing.T) {
	testCases := []struct {
		name             string
		statusCode       int
		body             string
		expectedReason   advertisedStatusEndpointCheckReason
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
			expectedReason:   advertisedStatusEndpointIdentityMismatch,
			expectedRemoteID: "remote-id",
		},
		{
			name:           "missing identity",
			statusCode:     http.StatusOK,
			body:           `{"is_owner":false}`,
			expectedReason: advertisedStatusEndpointMissingIdentity,
		},
		{
			name:           "malformed JSON",
			statusCode:     http.StatusOK,
			body:           `{"ddl_id":`,
			expectedReason: advertisedStatusEndpointInvalidResponse,
		},
		{
			name:           "oversized body",
			statusCode:     http.StatusOK,
			body:           `{"ddl_id":"` + strings.Repeat("x", advertisedStatusEndpointResponseBodyLimit) + `"}`,
			expectedReason: advertisedStatusEndpointInvalidResponse,
		},
		{
			name:           "non-2xx status",
			statusCode:     http.StatusInternalServerError,
			body:           `{"ddl_id":"local-id"}`,
			expectedReason: advertisedStatusEndpointUnexpectedStatus,
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

			client, err := newAdvertisedStatusEndpointHTTPClient(&http.Client{}, time.Second)
			require.NoError(t, err)
			result := checkAdvertisedStatusEndpoint(t.Context(), client, server.URL+"/info", "local-id")
			require.Equal(t, testCase.expectedReason, result.reason)
			require.Equal(t, testCase.expectedRemoteID, result.remoteID)
			if testCase.expectedReason == advertisedStatusEndpointUnexpectedStatus {
				require.Equal(t, "500 Internal Server Error", result.status)
			}
		})
	}
}

func TestAdvertisedStatusEndpointCheckRedirect(t *testing.T) {
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

	client, err := newAdvertisedStatusEndpointHTTPClient(&http.Client{}, time.Second)
	require.NoError(t, err)
	result := checkAdvertisedStatusEndpoint(t.Context(), client, redirect.URL+"/info", "local-id")
	require.Equal(t, advertisedStatusEndpointUnexpectedStatus, result.reason)
	require.Equal(t, "302 Found", result.status)
	require.Zero(t, targetRequests.Load())
}

func TestAdvertisedStatusEndpointCheckRequestFailures(t *testing.T) {
	t.Run("connection failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		endpoint := server.URL + "/info"
		server.Close()

		client, err := newAdvertisedStatusEndpointHTTPClient(&http.Client{}, time.Second)
		require.NoError(t, err)
		result := checkAdvertisedStatusEndpoint(t.Context(), client, endpoint, "local-id")
		require.Equal(t, advertisedStatusEndpointRequestFailed, result.reason)
		require.Error(t, result.err)
	})

	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}))
		t.Cleanup(server.Close)

		client, err := newAdvertisedStatusEndpointHTTPClient(&http.Client{}, 50*time.Millisecond)
		require.NoError(t, err)
		result := checkAdvertisedStatusEndpoint(t.Context(), client, server.URL+"/info", "local-id")
		require.Equal(t, advertisedStatusEndpointRequestFailed, result.reason)
		require.Error(t, result.err)
	})

	t.Run("response body read failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, `{"ddl_id":"local-id"}`)
		}))
		t.Cleanup(server.Close)

		client, err := newAdvertisedStatusEndpointHTTPClient(&http.Client{}, time.Second)
		require.NoError(t, err)
		result := checkAdvertisedStatusEndpoint(t.Context(), client, server.URL+"/info", "local-id")
		require.Equal(t, advertisedStatusEndpointRequestFailed, result.reason)
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

		client, err := newAdvertisedStatusEndpointHTTPClient(&http.Client{}, time.Minute)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		resultCh := make(chan advertisedStatusEndpointCheckResult, 1)
		go func() {
			resultCh <- checkAdvertisedStatusEndpoint(ctx, client, server.URL+"/info", "local-id")
		}()
		<-requestStarted
		cancel()

		select {
		case result := <-resultCh:
			require.Equal(t, advertisedStatusEndpointRequestFailed, result.reason)
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

		client, err := newAdvertisedStatusEndpointHTTPClient(&http.Client{}, time.Second)
		require.NoError(t, err)
		result := checkAdvertisedStatusEndpoint(t.Context(), client, server.URL+"/info", "local-id")
		require.Equal(t, advertisedStatusEndpointRequestFailed, result.reason)
		require.Error(t, result.err)
	})
}

func TestAdvertisedStatusEndpointCheckPreservesTLS(t *testing.T) {
	var clientCertificateSeen atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientCertificateSeen.Store(r.TLS != nil && len(r.TLS.PeerCertificates) == 1)
		_, _ = io.WriteString(w, `{"ddl_id":"local-id"}`)
	}))
	server.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)

	baseClient := server.Client()
	baseTLSConfig := baseClient.Transport.(*http.Transport).TLSClientConfig
	baseTLSConfig.Certificates = []tls.Certificate{server.TLS.Certificates[0]}
	client, err := newAdvertisedStatusEndpointHTTPClient(baseClient, time.Second)
	require.NoError(t, err)
	result := checkAdvertisedStatusEndpoint(t.Context(), client, server.URL+"/info", "local-id")
	require.Empty(t, result.reason)
	require.NoError(t, result.err)
	require.True(t, clientCertificateSeen.Load())
}

func TestAdvertisedStatusEndpointCheckBypassesProxy(t *testing.T) {
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
	baseClient := &http.Client{Transport: baseTransport}
	client, err := newAdvertisedStatusEndpointHTTPClient(baseClient, time.Second)
	require.NoError(t, err)
	require.NotNil(t, baseTransport.Proxy)
	require.Nil(t, client.Transport.(*http.Transport).Proxy)

	result := checkAdvertisedStatusEndpoint(t.Context(), client, endpoint.URL+"/info", "local-id")
	require.Empty(t, result.reason)
	require.Equal(t, int32(1), endpointRequests.Load())
	require.Zero(t, proxyRequests.Load())
}

func TestAdvertisedStatusEndpointCheckLifecycle(t *testing.T) {
	input := advertisedStatusEndpointCheckInput{
		endpoint: "http://127.0.0.1:10080/info",
		localID:  "local-id",
	}

	t.Run("cancellation stops without reporting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var calls atomic.Int32
		var reports atomic.Int32
		transport := &advertisedStatusEndpointTestTransport{}
		started := make(chan struct{})
		exited := make(chan struct{})
		receivedInput := make(chan [2]string, 1)
		checker := func(ctx context.Context, _ *http.Client, endpoint, expectedID string) advertisedStatusEndpointCheckResult {
			receivedInput <- [2]string{endpoint, expectedID}
			calls.Add(1)
			close(started)
			<-ctx.Done()
			close(exited)
			return advertisedStatusEndpointCheckResult{reason: advertisedStatusEndpointRequestFailed, err: ctx.Err()}
		}

		returned := make(chan struct{})
		go func() {
			scheduleAdvertisedStatusEndpointCheck(
				ctx,
				input,
				&http.Client{Transport: transport},
				checker,
				func(advertisedStatusEndpointCheckInput, advertisedStatusEndpointCheckResult) {
					reports.Add(1)
				},
			)
			close(returned)
		}()
		require.Eventually(t, func() bool {
			select {
			case <-started:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond)
		require.Equal(t, [2]string{"http://127.0.0.1:10080/info", "local-id"}, <-receivedInput)
		require.Eventually(t, func() bool {
			select {
			case <-returned:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond, "scheduling must not wait for the checker")

		cancel()
		require.Eventually(t, func() bool {
			select {
			case <-exited:
				return true
			default:
				return false
			}
		}, time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool {
			return transport.closeIdleCalls.Load() == 1
		}, time.Second, 10*time.Millisecond)
		require.Equal(t, int32(1), calls.Load())
		require.Zero(t, reports.Load())
	})

	t.Run("completed failure reports once", func(t *testing.T) {
		var calls atomic.Int32
		var reports atomic.Int32
		transport := &advertisedStatusEndpointTestTransport{}
		reported := make(chan advertisedStatusEndpointCheckResult, 1)
		scheduleAdvertisedStatusEndpointCheck(
			t.Context(),
			input,
			&http.Client{Transport: transport},
			func(context.Context, *http.Client, string, string) advertisedStatusEndpointCheckResult {
				calls.Add(1)
				return advertisedStatusEndpointCheckResult{reason: advertisedStatusEndpointIdentityMismatch}
			},
			func(_ advertisedStatusEndpointCheckInput, result advertisedStatusEndpointCheckResult) {
				reports.Add(1)
				reported <- result
			},
		)

		select {
		case result := <-reported:
			require.Equal(t, advertisedStatusEndpointIdentityMismatch, result.reason)
		case <-time.After(time.Second):
			require.FailNow(t, "completed failure was not reported")
		}
		require.Eventually(t, func() bool {
			return transport.closeIdleCalls.Load() == 1
		}, time.Second, 10*time.Millisecond)
		require.Equal(t, int32(1), calls.Load())
		require.Equal(t, int32(1), reports.Load())
	})
}

type advertisedStatusEndpointTestListener struct {
	addr net.Addr
}

func (*advertisedStatusEndpointTestListener) Accept() (net.Conn, error) {
	return nil, net.ErrClosed
}

func (*advertisedStatusEndpointTestListener) Close() error {
	return nil
}

func (l *advertisedStatusEndpointTestListener) Addr() net.Addr {
	return l.addr
}

type advertisedStatusEndpointTestTransport struct {
	closeIdleCalls atomic.Int32
}

func (*advertisedStatusEndpointTestTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, net.ErrClosed
}

func (t *advertisedStatusEndpointTestTransport) CloseIdleConnections() {
	t.closeIdleCalls.Add(1)
}
