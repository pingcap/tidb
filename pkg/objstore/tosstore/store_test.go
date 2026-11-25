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
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}

func testBackend() *backuppb.S3 {
	return &backuppb.S3{
		Region: "cn-beijing", Bucket: "bucket", Prefix: "/backup",
		AccessKey: "test-id", SecretAccessKey: "test-secret", SessionToken: "test-token",
	}
}

func TestEndpointAndRegion(t *testing.T) {
	t.Setenv("VOLCENGINE_REGION", "")
	t.Setenv("REGION", "cn-shanghai")
	for _, tc := range []struct{ endpoint, input, region, host string }{
		{"", "", "cn-shanghai", "https://tos-cn-shanghai.volces.com"},
		{"", " cn-beijing ", "cn-beijing", "https://tos-cn-beijing.volces.com"},
		{"https://tos-cn-beijing.ivolces.com/", "", "cn-beijing", "https://tos-cn-beijing.ivolces.com"},
		{"http://tos-cn-beijing.volces.com", "cn-beijing", "cn-beijing", "http://tos-cn-beijing.volces.com"},
		{"https://tos-cn-beijing.volces.cn", "", "cn-beijing", "https://tos-cn-beijing.volces.cn"},
		{"https://tos-cn-beijing.ivolces.cn", "", "cn-beijing", "https://tos-cn-beijing.ivolces.cn"},
		{"https://custom.example.org:8000", "", "cn-shanghai", "https://custom.example.org:8000"},
	} {
		endpoint, region, err := endpointAndRegion(tc.endpoint, tc.input)
		require.NoError(t, err)
		require.Equal(t, tc.host, endpoint)
		require.Equal(t, tc.region, region)
	}
	t.Setenv("VOLCENGINE_REGION", "cn-beijing")
	endpoint, region, err := endpointAndRegion("", "")
	require.NoError(t, err)
	require.Equal(t, "cn-beijing", region)
	require.Equal(t, "https://tos-cn-beijing.ivolces.com", endpoint)
	endpoint, region, err = endpointAndRegion("", "cn-shanghai")
	require.NoError(t, err)
	require.Equal(t, "cn-shanghai", region)
	require.Equal(t, "https://tos-cn-shanghai.volces.com", endpoint)
	t.Setenv("VOLCENGINE_REGION", "")
	t.Setenv("REGION", "")
	for _, tc := range []struct{ endpoint, region string }{
		{"", ""}, {"https://tos-cn-shanghai.volces.com", "cn-beijing"}, {"https://tos-cn-shanghai.volces.cn", "cn-beijing"},
		{"https://tos-s3-cn-beijing.volces.com", ""}, {"https://mybucket.tos-s3-cn-beijing.ivolces.com", ""},
		{"ftp://private-value", "cn-beijing"}, {"https://private-value@host", "cn-beijing"}, {"https://host/path/private-value", "cn-beijing"},
		{"https://host?sig=private-value", "cn-beijing"}, {"https://host#private-value", "cn-beijing"}, {"", "private-value@host"},
	} {
		_, _, err := endpointAndRegion(tc.endpoint, tc.region)
		require.ErrorIs(t, err, berrors.ErrStorageInvalidConfig)
		require.NotContains(t, err.Error(), "private-value")
	}
}

func TestStorageConfig(t *testing.T) {
	t.Setenv("VOLCENGINE_REGION", "")
	t.Setenv("REGION", "")
	_, err := NewTOSStorage(context.Background(), &backuppb.S3{RoleArn: "role", ExternalId: "keyspace"}, &storeapi.Options{})
	require.ErrorContains(t, err, "volcengine TOS role-arn does not support external-id")
	for _, update := range []func(*backuppb.S3){
		func(q *backuppb.S3) { q.Profile = "private-value" },
		func(q *backuppb.S3) { q.SseKmsEncryptionContext = "private-value" },
		func(q *backuppb.S3) { q.RoleArn = "  " },
		func(q *backuppb.S3) { q.ExternalId = "\t " },
		func(q *backuppb.S3) { q.RoleArn, q.ExternalId = "role", "private-value" },
	} {
		backend := testBackend()
		update(backend)
		_, err := NewTOSStorage(context.Background(), backend, &storeapi.Options{})
		require.ErrorIs(t, err, berrors.ErrStorageInvalidConfig)
		require.NotContains(t, err.Error(), "private-value")
		require.Equal(t, "test-secret", backend.SecretAccessKey)
	}
	t.Setenv("TOS_ACCESS_KEY", "env-id")
	t.Setenv("TOS_SECRET_KEY", "env-secret")
	t.Setenv("TOS_SECURITY_TOKEN", "env-token")
	for _, send := range []bool{true, false} {
		q := &backuppb.S3{Bucket: "bucket", Prefix: "/a/", Region: "cn-beijing", ExternalId: "keyspace"}
		s, err := NewTOSStorage(context.Background(), q, &storeapi.Options{SendCredentials: send})
		require.NoError(t, err)
		require.Equal(t, "tos://bucket/a/", s.URI())
		want := []string{"", "", ""}
		if send {
			want = []string{"env-id", "env-secret", "env-token"}
		}
		require.Equal(t, want, []string{q.AccessKey, q.SecretAccessKey, q.SessionToken})
		s.Close()
	}

	t.Run("injected transport preserves object bytes", func(t *testing.T) {
		var object bytes.Buffer
		gz := gzip.NewWriter(&object)
		_, err := io.WriteString(gz, "original object")
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Empty(t, r.Header.Get("Accept-Encoding"))
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write(object.Bytes())
		}))
		hc := server.Client()
		tr := hc.Transport.(*http.Transport)
		q := testBackend()
		q.Endpoint = server.URL
		s, err := NewTOSStorage(context.Background(), q, &storeapi.Options{HTTPClient: hc})
		require.NoError(t, err)
		defer s.Close()
		r, err := s.Open(context.Background(), "object", nil)
		require.NoError(t, err)
		defer r.Close()
		b, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, object.Bytes(), b)
		require.Same(t, tr, hc.Transport)
		require.False(t, tr.DisableCompression)
	})

	t.Run("default client uses ProxyFromEnvironment", func(t *testing.T) {
		hc, closeIdle := newTOSHTTPClient(nil)
		defer closeIdle()
		tr, ok := hc.Transport.(*http.Transport)
		require.True(t, ok)
		require.NotNil(t, tr.Proxy)
		// Compare function pointers instead of calling ProxyFromEnvironment so
		// this test never triggers or depends on Go's process-wide envProxyOnce cache.
		require.Equal(t, reflect.ValueOf(http.ProxyFromEnvironment).Pointer(), reflect.ValueOf(tr.Proxy).Pointer())
	})

	t.Run("injected client with default transport", func(t *testing.T) {
		caller := &http.Client{Timeout: time.Second}
		hc, closeIdle := newTOSHTTPClient(caller)
		defer closeIdle()
		require.Equal(t, caller.Timeout, hc.Timeout)
		require.Nil(t, caller.Transport)
		tr := hc.Transport.(*http.Transport)
		require.NotSame(t, http.DefaultTransport, tr)
		require.True(t, tr.DisableCompression)
	})

	t.Run("redirect stops at the first server", func(t *testing.T) {
		var redirected atomic.Bool
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			redirected.Store(true)
			assert.Empty(t, r.Header.Get("X-Tos-Security-Token"))
		}))
		defer target.Close()
		location := fmt.Sprintf("http://localhost:%d", target.Listener.Addr().(*net.TCPAddr).Port)
		var sent atomic.Bool
		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sent.Store(true)
			assert.Equal(t, "test-token", r.Header.Get("X-Tos-Security-Token"))
			http.Redirect(w, r, location, http.StatusTemporaryRedirect)
		}))
		defer source.Close()
		q := testBackend()
		q.Endpoint = source.URL
		hc := source.Client()
		s, err := NewTOSStorage(context.Background(), q, &storeapi.Options{HTTPClient: hc})
		require.NoError(t, err)
		defer s.Close()
		err = s.cli.CheckBucketExistence(context.Background())
		require.True(t, sent.Load())
		require.False(t, redirected.Load())
		require.ErrorContains(t, err, "307")
		require.Nil(t, hc.CheckRedirect)
	})

	t.Run("close invokes owned idle connection cleanup", func(t *testing.T) {
		s, err := NewTOSStorage(context.Background(), testBackend(), nil)
		require.NoError(t, err)
		require.NotNil(t, s.closeIdle)
		closed := false
		orig := s.closeIdle
		s.closeIdle = func() {
			closed = true
			orig()
		}
		s.Close()
		require.True(t, closed)
	})

	t.Run("copy requires a TOS source", func(t *testing.T) {
		var copies int
		ctx := context.Background()
		q := testBackend()
		src, err := NewTOSStorage(ctx, q, nil)
		require.NoError(t, err)
		defer src.Close()
		dst, err := NewTOSStorage(ctx, testBackend(), &storeapi.Options{HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			copies++
			require.Equal(t, "/bucket/backup%2Fsrc", r.Header.Get("X-Tos-Copy-Source"))
			require.Equal(t, "/backup/dst", r.URL.Path)
			return response(200, `{"ETag":"copied"}`), nil
		})}})
		require.NoError(t, err)
		defer dst.Close()
		spec := storeapi.CopySpec{From: "src", To: "dst"}
		otherService := s3like.NewStorage(nil, storeapi.NewBucketPrefix("bucket", "backup"), q, nil)
		require.ErrorIs(t, dst.CopyFrom(ctx, src.Storage, spec), berrors.ErrStorageInvalidConfig)
		require.ErrorIs(t, dst.CopyFrom(ctx, (*TOSStore)(nil), spec), berrors.ErrStorageInvalidConfig)
		require.ErrorIs(t, dst.CopyFrom(ctx, otherService, spec), berrors.ErrStorageInvalidConfig)
		require.NoError(t, dst.CopyFrom(ctx, src, spec))
		require.Equal(t, 1, copies)
	})
	s, err := NewTOSStorage(context.Background(), &backuppb.S3{Bucket: "bucket", Prefix: "reports#old", Region: "cn-beijing"}, nil)
	require.NoError(t, err)
	require.Equal(t, "tos://bucket/reports%23old/", s.URI())
	s.Close()
}

func TestPermissionsAndPresigning(t *testing.T) {
	t.Setenv("VOLCENGINE_REGION", "cn-beijing")
	for _, tc := range []struct {
		endpoint, dataHost, signedHost string
	}{
		{"", "bucket.tos-cn-beijing.ivolces.com", "bucket.tos-cn-beijing.volces.com"},
		{"https://tos-cn-beijing.ivolces.com", "bucket.tos-cn-beijing.ivolces.com", "bucket.tos-cn-beijing.volces.com"},
		{"https://tos-cn-beijing.ivolces.cn", "bucket.tos-cn-beijing.ivolces.cn", "bucket.tos-cn-beijing.volces.cn"},
		{"https://custom.example.org", "bucket.custom.example.org", "bucket.custom.example.org"},
	} {
		q := testBackend()
		q.Endpoint = tc.endpoint
		dataHost, signedHost := tc.dataHost, tc.signedHost
		stats := &recording.AccessStats{}
		opts := &storeapi.Options{AccessRecording: stats, CheckPermissions: []storeapi.Permission{storeapi.AccessBuckets},
			HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, dataHost, req.URL.Host)
				require.Equal(t, http.MethodHead, req.Method)
				require.Contains(t, req.Header.Get("Authorization"), "Credential=test-id/")
				require.Equal(t, "test-token", req.Header.Get("X-Tos-Security-Token"))
				return response(200, ""), nil
			})},
		}
		s, err := NewTOSStorage(context.Background(), q, opts)
		require.NoError(t, err)
		require.EqualValues(t, 1, stats.Requests.Get.Load())
		for ttl, seconds := range map[time.Duration]string{time.Nanosecond: "1", 7 * 24 * time.Hour: "604800"} {
			signed, err := s.PresignFile(context.Background(), "a b", ttl)
			require.NoError(t, err)
			u, err := url.Parse(signed)
			require.NoError(t, err)
			require.Equal(t, signedHost, u.Host)
			require.Equal(t, "/backup/a b", u.Path)
			require.Equal(t, seconds, u.Query().Get("X-Tos-Expires"))
		}
		for _, ttl := range []time.Duration{-1, 0, 7*24*time.Hour + 1} {
			_, err := s.PresignFile(context.Background(), "a", ttl)
			require.Error(t, err)
		}
		s.Close()
	}
	t.Run("cancel during permission check", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := NewTOSStorage(ctx, testBackend(), &storeapi.Options{CheckPermissions: []storeapi.Permission{storeapi.AccessBuckets},
			HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				cancel()
				return nil, ctx.Err()
			})},
		})
		require.Equal(t, context.Canceled, err)
	})
	q := testBackend()
	opts := &storeapi.Options{CheckPermissions: []storeapi.Permission{storeapi.AccessBuckets}, HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(403, `{"Message":"test-secret"}`), nil
	})}}
	_, err := NewTOSStorage(context.Background(), q, opts)
	require.ErrorIs(t, err, berrors.ErrStorageInvalidPermission)
	require.NotContains(t, err.Error(), "test-secret")
	require.Equal(t, "test-token", q.SessionToken)
}
