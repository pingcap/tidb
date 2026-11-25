// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tosstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vtos "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

func TestCredentialSources(t *testing.T) {
	for _, key := range []string{"TOS_ACCESS_KEY", "TOS_SECRET_KEY", "TOS_SECURITY_TOKEN", "VOLCENGINE_OIDC_TOKEN_FILE", "VOLCENGINE_OIDC_ROLE_TRN", "VOLCENGINE_OIDC_STS_ENDPOINT"} {
		t.Setenv(key, "")
	}
	q := &backuppb.S3{}
	for _, key := range []string{"TOS_ACCESS_KEY", "TOS_SECRET_KEY", "TOS_SECURITY_TOKEN", "VOLCENGINE_OIDC_TOKEN_FILE", "VOLCENGINE_OIDC_ROLE_TRN"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "private-value")
			_, err := newCredentials(q, nil)
			require.ErrorIs(t, err, berrors.ErrStorageInvalidConfig)
			require.NotContains(t, err.Error(), "private-value")
		})
	}
	t.Setenv("TOS_ACCESS_KEY", "env-id")
	t.Setenv("TOS_SECRET_KEY", "env-secret")
	t.Setenv("TOS_SECURITY_TOKEN", "env-token")
	for _, partial := range []*backuppb.S3{{AccessKey: "id"}, {SecretAccessKey: "private-value"}, {SessionToken: "private-value"}} {
		_, err := newCredentials(partial, nil)
		require.ErrorIs(t, err, berrors.ErrStorageInvalidConfig)
	}
	p, err := newCredentials(q, nil)
	require.NoError(t, err)
	require.Equal(t, "env-token", p.Credential().SecurityToken)
	p, err = newCredentials(testBackend(), nil)
	require.NoError(t, err)
	require.Equal(t, "test-id", p.Credential().AccessKeyID)
}

func TestOIDCExchange(t *testing.T) {
	file := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(file, []byte(" jwt-from-file\n"), 0600))
	t.Setenv("VOLCENGINE_OIDC_TOKEN_FILE", file)
	t.Setenv("VOLCENGINE_OIDC_ROLE_TRN", "base-role")
	t.Setenv("VOLCENGINE_OIDC_ROLE_POLICY", "policy")
	for _, endpoint := range []string{"http://host", "https://host?private-value", "https://user:private-value@host"} {
		t.Setenv("VOLCENGINE_OIDC_STS_ENDPOINT", endpoint)
		_, err := newOIDCSource("region", nil)
		require.ErrorIs(t, err, berrors.ErrStorageInvalidConfig)
		require.NotContains(t, err.Error(), "private-value")
	}
	t.Setenv("VOLCENGINE_OIDC_STS_ENDPOINT", "sts.test")
	source, err := newOIDCSource("region", nil)
	require.NoError(t, err)
	n := 0
	source.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		n++
		require.Equal(t, "https", req.URL.Scheme)
		require.Equal(t, "sts.test", req.URL.Host)
		require.Equal(t, "AssumeRoleWithOIDC", req.URL.Query().Get("Action"))
		require.NoError(t, req.ParseForm())
		require.Equal(t, "3600", req.PostForm.Get("DurationSeconds"))
		require.Equal(t, "base-role", req.PostForm.Get("RoleTrn"))
		require.Equal(t, "jwt-from-file", req.PostForm.Get("OIDCToken"))
		require.Equal(t, "policy", req.PostForm.Get("Policy"))
		switch n {
		case 1:
			return response(429, ""), nil
		case 2:
			return response(503, ""), nil
		default:
			return response(200, fmt.Sprintf(`{"Result":{"Credentials":{"AccessKeyId":"oidc-id","SecretAccessKey":"oidc-sk","SessionToken":"base-token","Expiration":%q}}}`, time.Now().Add(time.Hour).Format(time.RFC3339))), nil
		}
	})
	cred, err := source.fetch(context.Background())
	require.NoError(t, err)
	require.Equal(t, "oidc-id", cred.AccessKeyID)
	require.Equal(t, 3, n)
	for _, tc := range []struct {
		status int
		body   string
		want   []string
	}{
		{200, strings.Repeat(" ", 64<<10) + "{}", nil},
		{200, `{"ResponseMetadata":{"RequestId":"req-oidc-1","Error":{"Code":"InvalidToken","Message":"private-value"}}}`, []string{"InvalidToken", "req-oidc-1"}},
		{200, `{"Result":{"Credentials":{"Expiration":"private-value"}}}`, nil},
		{403, "private-value", nil},
		{307, "private-value", nil},
	} {
		n = 0
		source.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			n++
			res := response(tc.status, tc.body)
			res.Header.Set("Location", "https://redirect.test/")
			return res, nil
		})
		_, err := source.fetch(context.Background())
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-value")
		for _, sub := range tc.want {
			require.ErrorContains(t, err, sub)
		}
		require.Equal(t, 1, n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	source.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		cancel()
		return response(503, ""), nil
	})
	_, err = source.fetch(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// Body read errors preserve net.Error rather than being masked as invalid JSON.
	readNetErr := &net.OpError{Op: "read", Net: "tcp", Err: context.DeadlineExceeded}
	source.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(iotest.ErrReader(readNetErr)),
		}, nil
	})
	_, err = source.fetch(context.Background())
	var gotNetErr net.Error
	require.ErrorAs(t, err, &gotNetErr)
	require.True(t, gotNetErr.Timeout())
}

func TestAssumeRole(t *testing.T) {
	t.Run("forward assumed credentials", func(t *testing.T) {
		calls := 0
		hc := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			assert.Equal(t, "sts.cn-beijing.volcengineapi.com", req.URL.Host)
			return response(200, fmt.Sprintf(`{"Result":{"Credentials":{"AccessKeyId":"assumed","SecretAccessKey":"secret","SessionToken":"token","ExpiredTime":%q}}}`, time.Now().Add(time.Hour).Format(time.RFC3339))), nil
		})}
		for _, forward := range []bool{false, true} {
			q := testBackend()
			q.Region, q.Endpoint, q.RoleArn = "", "https://tos-cn-beijing.volces.com", "role"
			s, err := NewTOSStorage(context.Background(), q, &storeapi.Options{SendCredentials: forward, HTTPClient: hc})
			require.NoError(t, err)
			s.Close()
			if forward {
				require.Equal(t, []string{"assumed", "secret", "token", "", "cn-beijing"}, []string{q.AccessKey, q.SecretAccessKey, q.SessionToken, q.RoleArn, q.Region})
				consumer, err := NewTOSStorage(context.Background(), q, nil)
				require.NoError(t, err)
				consumer.Close()
			} else {
				require.Equal(t, []string{"", "", "", "role"}, []string{q.AccessKey, q.SecretAccessKey, q.SessionToken, q.RoleArn})
			}
		}
		require.Equal(t, 2, calls)
	})
	t.Run("preserve caller HTTP proxy on AssumeRole", func(t *testing.T) {
		for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy"} {
			t.Setenv(key, "")
		}
		var proxyHost atomic.Value
		proxyErr := errors.New("proxy invoked")
		tr := &http.Transport{
			Proxy: func(req *http.Request) (*url.URL, error) {
				proxyHost.Store(req.URL.Host)
				return nil, proxyErr
			},
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("unexpected direct dial bypassing proxy")
			},
		}
		q := testBackend()
		q.Region, q.Endpoint, q.RoleArn = "", "https://tos-cn-beijing.volces.com", "role"
		_, err := NewTOSStorage(context.Background(), q, &storeapi.Options{HTTPClient: &http.Client{Transport: tr}})
		require.Error(t, err)
		require.Equal(t, "sts.cn-beijing.volcengineapi.com", proxyHost.Load())
		require.NotNil(t, tr.Proxy)
	})
	t.Run("custom global http.DefaultTransport does not panic on AssumeRole", func(t *testing.T) {
		orig := http.DefaultTransport
		t.Cleanup(func() { http.DefaultTransport = orig })
		calls := 0
		http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			assert.Equal(t, "sts.cn-beijing.volcengineapi.com", req.URL.Host)
			return response(200, fmt.Sprintf(`{"Result":{"Credentials":{"AccessKeyId":"assumed","SecretAccessKey":"secret","SessionToken":"token","ExpiredTime":%q}}}`, time.Now().Add(time.Hour).Format(time.RFC3339))), nil
		})
		q := testBackend()
		q.Region, q.Endpoint, q.RoleArn = "", "https://tos-cn-beijing.volces.com", "role"
		s, err := NewTOSStorage(context.Background(), q, nil)
		require.NoError(t, err)
		s.Close()
		require.Equal(t, 1, calls)
	})
	t.Run("sanitized STS RequestFailure status mapping", func(t *testing.T) {
		for _, tc := range []struct {
			status    int
			code      string
			retryable bool
		}{
			{http.StatusForbidden, "AccessDenied", false},
			{http.StatusServiceUnavailable, "ServiceUnavailable", true},
		} {
			hc := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				body := fmt.Sprintf(`{"ResponseMetadata":{"RequestId":"req-sts-%d","Error":{"Code":%q,"Message":"private-sts-secret"}}}`, tc.status, tc.code)
				return response(tc.status, body), nil
			})}
			_, err := assumeRole(context.Background(), "cn-beijing", "target-role", vtos.Credential{AccessKeyID: "id", AccessKeySecret: "sk"}, hc)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.code)
			require.ErrorContains(t, err, fmt.Sprintf("req-sts-%d", tc.status))
			require.NotContains(t, err.Error(), "private-sts-secret")
			var httpErr *errdef.HTTPStatusError
			if tc.retryable {
				require.ErrorAs(t, err, &httpErr)
				require.Equal(t, tc.status, httpErr.StatusCode)
			} else {
				require.False(t, errors.As(err, &httpErr))
			}
		}
	})
	var calls int
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		require.Equal(t, "sts.cn-beijing.volcengineapi.com", req.URL.Host)
		require.Contains(t, req.Header.Get("Authorization"), "base-id/")
		require.Equal(t, "base-token", req.Header.Get("X-Security-Token"))
		require.NoError(t, req.ParseForm())
		require.Equal(t, "3600", req.Form.Get("DurationSeconds"))
		require.Equal(t, "target-role", req.Form.Get("RoleTrn"))
		return response(200, fmt.Sprintf(`{"Result":{"Credentials":{"AccessKeyId":"target-id","SecretAccessKey":"target-sk","SessionToken":"target-token","ExpiredTime":%q}}}`, time.Now().Add(time.Hour).Format(time.RFC3339))), nil
	})}
	cred, err := assumeRole(context.Background(), "cn-beijing", "target-role", vtos.Credential{AccessKeyID: "base-id", AccessKeySecret: "base-sk", SecurityToken: "base-token"}, client)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, "target-id", cred.AccessKeyID)
}

func TestCredentialRefresh(t *testing.T) {
	t.Run("presign refreshes once for a longer lifetime", func(t *testing.T) {
		ctx := context.Background()
		s, err := NewTOSStorage(ctx, testBackend(), nil)
		require.NoError(t, err)
		defer s.Close()
		p := s.cli.creds
		p.current.fetchedAt = time.Now().Add(-5 * time.Minute)
		p.current.expiry = time.Now().Add(30 * time.Minute)
		p.refreshAt = p.current.expiry.Add(-time.Minute)
		var fetches atomic.Int32
		p.fetch = func(context.Context) (credential, error) {
			fetches.Add(1)
			return temporaryCredential("fresh", "sk", "fresh-token", time.Now().Add(time.Hour).Format(time.RFC3339))
		}
		url, err := s.PresignFile(ctx, "file", time.Minute)
		require.NoError(t, err)
		require.Contains(t, url, "test-token")
		for range 2 {
			url, err := s.PresignFile(ctx, "file", 2*time.Hour)
			require.NoError(t, err)
			require.Contains(t, url, "fresh-token")
		}
		require.EqualValues(t, 1, fetches.Load())
	})
	t.Run("presign honors failure backoff while current credential is valid", func(t *testing.T) {
		ctx := context.Background()
		s, err := NewTOSStorage(ctx, testBackend(), nil)
		require.NoError(t, err)
		defer s.Close()
		p := s.cli.creds
		p.current.fetchedAt = time.Now().Add(-10 * time.Minute)
		p.current.expiry = time.Now().Add(30 * time.Minute)
		p.refreshAt = p.current.expiry.Add(-time.Minute)
		var fetches atomic.Int32
		stsErr := errors.New("STS unavailable")
		p.fetch = func(context.Context) (credential, error) {
			fetches.Add(1)
			return credential{}, stsErr
		}
		_, err = s.PresignFile(ctx, "file", 2*time.Hour)
		require.ErrorIs(t, err, stsErr)
		// Subsequent presign within the 30s backoff window reuses the still-valid
		// credential instead of hammering STS.
		signed, err := s.PresignFile(ctx, "file", 2*time.Hour)
		require.NoError(t, err)
		require.Contains(t, signed, "test-token")
		require.EqualValues(t, 1, fetches.Load())
	})
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	p := &credentials{fetch: func(ctx context.Context) (credential, error) {
		calls.Add(1)
		close(started)
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 10*time.Second {
			return credential{}, errors.New("missing shared deadline")
		}
		<-release
		return temporaryCredential("fresh", "sk", "token", time.Now().Add(2*time.Hour).Format(time.RFC3339))
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := p.resolve(ctx); done <- err }()
	<-started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	for range 8 {
		go func() { _, err := p.resolve(context.Background()); done <- err }()
	}
	close(release)
	for range 8 {
		require.NoError(t, <-done)
	}
	require.EqualValues(t, 1, calls.Load())
	require.Equal(t, "fresh", p.Credential().AccessKeyID)
	p.mu.Lock()
	require.Equal(t, 6*time.Minute, p.current.expiry.Sub(p.refreshAt))
	p.refreshAt = time.Now()
	started, release = make(chan struct{}), make(chan struct{})
	p.fetch = func(context.Context) (credential, error) {
		calls.Add(1)
		close(started)
		<-release
		return credential{}, errors.New("STS unavailable")
	}
	p.mu.Unlock()
	got, err := p.resolve(context.Background())
	require.NoError(t, err)
	require.Equal(t, "fresh", got.AccessKeyID)
	<-started
	p.mu.Lock()
	refreshed := p.call.done
	p.mu.Unlock()
	close(release)
	<-refreshed
	_, err = p.resolve(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2, calls.Load())
	p.mu.Lock()
	require.WithinDuration(t, time.Now().Add(30*time.Second), p.refreshAt, time.Second)
	p.current.expiry = time.Now().Add(-time.Second)
	p.mu.Unlock()
	require.Empty(t, p.Credential())

	t.Run("expired refresh failure honors backoff window", func(t *testing.T) {
		var fetches atomic.Int32
		stsErr := errors.New("STS unavailable")
		creds := &credentials{
			current: credential{
				Credential: vtos.Credential{AccessKeyID: "expired", AccessKeySecret: "sk"},
				expiry:     time.Now().Add(-time.Second),
			},
			fetch: func(context.Context) (credential, error) {
				fetches.Add(1)
				return credential{}, stsErr
			},
		}
		for range 3 {
			_, err := creds.resolve(context.Background())
			require.ErrorIs(t, err, stsErr)
		}
		require.EqualValues(t, 1, fetches.Load())
		creds.mu.Lock()
		require.WithinDuration(t, time.Now().Add(30*time.Second), creds.refreshAt, time.Second)
		creds.refreshAt = time.Now().Add(-time.Second)
		creds.fetch = func(context.Context) (credential, error) {
			fetches.Add(1)
			return temporaryCredential("recovered", "sk", "token", time.Now().Add(time.Hour).Format(time.RFC3339))
		}
		creds.mu.Unlock()
		got, err := creds.resolve(context.Background())
		require.NoError(t, err)
		require.Equal(t, "recovered", got.AccessKeyID)
		require.EqualValues(t, 2, fetches.Load())
		creds.close()
	})

	t.Run("panic during refresh is recovered and unblocks waiters", func(t *testing.T) {
		creds := &credentials{
			fetch: func(context.Context) (credential, error) {
				panic("boom")
			},
		}
		_, err := creds.resolve(context.Background())
		require.ErrorContains(t, err, "panicked")
		creds.close()
	})

	t.Run("close cancels and waits for in-flight refresh", func(t *testing.T) {
		baseCtx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		exited := make(chan struct{})
		creds := &credentials{
			baseCtx: baseCtx,
			cancel:  cancel,
			fetch: func(ctx context.Context) (credential, error) {
				close(started)
				defer close(exited)
				<-ctx.Done()
				return credential{}, ctx.Err()
			},
		}
		done := make(chan error, 1)
		go func() {
			_, err := creds.resolve(context.Background())
			done <- err
		}()
		<-started
		creds.close()
		select {
		case <-exited:
		default:
			t.Fatal("close returned before refresh goroutine exited")
		}
		require.ErrorIs(t, <-done, context.Canceled)
		_, err := creds.resolve(context.Background())
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestCredentialGuard(t *testing.T) {
	creds := &credentials{current: credential{Credential: vtos.Credential{AccessKeyID: "first", AccessKeySecret: "sk"}}}
	calls := 0
	httpClient := http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		require.Contains(t, req.Header.Get("Authorization"), "Credential=first/")
		creds.mu.Lock()
		creds.current.expiry = time.Now().Add(-time.Second)
		creds.mu.Unlock()
		return response(503, ""), nil
	})}
	guard := &credentialGuardTransport{next: vtos.NewDefaultTransportWithClient(httpClient)}
	svc, err := vtos.NewClientV2("https://tos-cn-beijing.volces.com",
		vtos.WithRegion("cn-beijing"), vtos.WithCredentials(creds), vtos.WithTransport(guard))
	require.NoError(t, err)
	defer svc.Close()
	_, err = svc.HeadBucket(context.Background(), &vtos.HeadBucketInput{Bucket: "bucket"})
	require.ErrorContains(t, err, "expired before signing")
	require.Equal(t, 1, calls)
}
