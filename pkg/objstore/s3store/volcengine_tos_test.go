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

package s3store

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestAutoNewCredVolcengine(t *testing.T) {
	t.Setenv(volcengineOIDCTokenFileEnv, "")
	t.Setenv(volcengineOIDCRoleTRNEnv, "")
	t.Setenv(volcengineRegionEnv, "")
	t.Setenv(volcengineLegacyRegionEnv, "")

	t.Run("Volcengine TOS endpoint recognition", func(t *testing.T) {
		testCases := []struct {
			endpoint string
			expected bool
		}{
			{endpoint: "https://tos-s3-cn-beijing.volces.com", expected: true},
			{endpoint: "https://tos-s3-cn-beijing.ivolces.com", expected: true},
			{endpoint: "https://bucket.tos-s3-cn-beijing.ivolces.com", expected: true},
			{endpoint: "https://s3.example.com/volces.com", expected: false},
			{endpoint: "https://s3.example.com", expected: false},
		}
		for _, tc := range testCases {
			require.Equal(t, tc.expected, isVolcengineTOSEndpoint(tc.endpoint), tc.endpoint)
		}
	})

	t.Run("explicit credentials take precedence", func(t *testing.T) {
		provider, err := autoNewCred(&backuppb.S3{
			Endpoint:        "https://tos-s3-cn-beijing.ivolces.com",
			Provider:        "volcengine",
			Region:          "cn-beijing",
			AccessKey:       "explicit-id",
			SecretAccessKey: "explicit-key",
			SessionToken:    "explicit-token",
		})
		require.NoError(t, err)

		cred, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, "explicit-id", cred.AccessKeyID)
		require.Equal(t, "explicit-key", cred.SecretAccessKey)
		require.Equal(t, "explicit-token", cred.SessionToken)
	})

	t.Run("missing OIDC env falls back to default chain", func(t *testing.T) {
		provider, err := autoNewCred(&backuppb.S3{
			Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
			Provider: "volcengine",
			Region:   "cn-beijing",
		})
		require.NoError(t, err)
		require.Nil(t, provider)
	})

	t.Run("invalid region rejected before sending STS request", func(t *testing.T) {
		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("oidc-token"), 0o600))
		t.Setenv(volcengineOIDCTokenFileEnv, tokenFile)
		t.Setenv(volcengineOIDCRoleTRNEnv, "trn:iam::1234567890:role/metering")

		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
		}))
		t.Cleanup(server.Close)

		prevURL := volcengineOIDCSTSURL
		volcengineOIDCSTSURL = func(string) string { return server.URL }
		t.Cleanup(func() { volcengineOIDCSTSURL = prevURL })

		for _, badRegion := range []string{"evil.com#", "cn-beijing@evil.com", "cn-beijing/foo", "-cn-beijing"} {
			provider, err := autoNewCred(&backuppb.S3{
				Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
				Region:   badRegion,
			})
			require.ErrorContains(t, err, "invalid volcengine region")
			require.Nil(t, provider)
		}
		require.Zero(t, hits.Load())
	})
}

func TestVolcengineTOSEndpointOnlyEndToEnd(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(" oidc-token \n"), 0o600))
	t.Setenv(volcengineOIDCTokenFileEnv, tokenFile)
	t.Setenv(volcengineOIDCRoleTRNEnv, "trn:iam::1234567890:role/metering")
	t.Setenv(volcengineRegionEnv, "cn-beijing")
	t.Setenv(volcengineLegacyRegionEnv, "")

	stsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, r.ParseForm())
		require.Equal(t, "oidc-token", r.PostForm.Get("OIDCToken"))
		require.Equal(t, "trn:iam::1234567890:role/metering", r.PostForm.Get("RoleTrn"))

		resp := volcengineOIDCSTSResponse{}
		resp.Result.Credentials.AccessKeyID = "oidc-ak"
		resp.Result.Credentials.SecretAccessKey = "oidc-sk"
		resp.Result.Credentials.SessionToken = "oidc-sess"
		resp.Result.Credentials.Expiration = time.Now().Add(time.Hour).Format(time.RFC3339)
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(stsServer.Close)

	prevURL := volcengineOIDCSTSURL
	volcengineOIDCSTSURL = func(region string) string {
		require.Equal(t, "cn-beijing", region)
		return stsServer.URL
	}
	t.Cleanup(func() { volcengineOIDCSTSURL = prevURL })

	var (
		mu      sync.Mutex
		methods []string
		gotHost string
		gotPath string
		gotAuth string
		gotTok  string
	)
	tosServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		gotHost = r.Host
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotTok = r.Header.Get("X-Amz-Security-Token")
		mu.Unlock()
		_, _ = w.Write([]byte("hello-tos"))
	}))
	t.Cleanup(tosServer.Close)

	// Omit provider and region from the URI to verify endpoint-only TOS handling
	// shares virtual-host addressing, skips AWS HeadBucket, and signs with VOLCENGINE_REGION.
	rawURL := "s3://my-bucket/prefix?endpoint=https://tos-s3-cn-beijing.ivolces.com"
	s3Opts := s3like.S3BackendOptions{
		Endpoint:       "https://tos-s3-cn-beijing.ivolces.com",
		ForcePathStyle: true,
	}
	s3Opts.SetForcePathStyle(rawURL)
	backend := &backuppb.S3{Bucket: "my-bucket", Prefix: "prefix"}
	require.NoError(t, s3Opts.Apply(backend))

	store, err := NewS3Storage(t.Context(), backend, &storeapi.Options{
		HTTPClient: newRewriteHostHTTPClient(tosServer.URL),
	})
	require.NoError(t, err)
	defer store.Close()

	data, err := store.ReadFile(t.Context(), "hello.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("hello-tos"), data)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{http.MethodGet}, methods)
	require.Equal(t, "my-bucket.tos-s3-cn-beijing.ivolces.com", gotHost)
	require.Equal(t, "/prefix/hello.txt", gotPath)
	require.Contains(t, gotAuth, "/cn-beijing/s3/aws4_request")
	require.Equal(t, "oidc-sess", gotTok)
}

func TestVolcengineOIDCCredentialsProviderRefresh(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("oidc-token"), 0o600))

	// STS issues one credential and fails subsequent refreshes.
	newProvider := func(t *testing.T, validity time.Duration) (*volcengineOIDCCredentialsProvider, *atomic.Int32) {
		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) > 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			resp := volcengineOIDCSTSResponse{}
			resp.Result.Credentials.AccessKeyID = "oidc-ak"
			resp.Result.Credentials.SecretAccessKey = "oidc-sk"
			resp.Result.Credentials.SessionToken = "oidc-sess"
			resp.Result.Credentials.Expiration = time.Now().Add(validity).Format(time.RFC3339)
			require.NoError(t, json.NewEncoder(w).Encode(resp))
		}))
		t.Cleanup(server.Close)
		prevURL := volcengineOIDCSTSURL
		volcengineOIDCSTSURL = func(string) string { return server.URL }
		t.Cleanup(func() { volcengineOIDCSTSURL = prevURL })

		return &volcengineOIDCCredentialsProvider{
			region:    "cn-beijing",
			roleTRN:   "trn:iam::1234567890:role/metering",
			tokenFile: tokenFile,
		}, &hits
	}

	t.Run("concurrent cold calls share one exchange", func(t *testing.T) {
		provider, hits := newProvider(t, time.Hour)
		const workers = 4
		errs := make([]error, workers)
		aks := make([]string, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Go(func() {
				cred, err := provider.Retrieve(t.Context())
				aks[i], errs[i] = cred.AccessKeyID, err
			})
		}
		wg.Wait()
		for i := range workers {
			require.NoError(t, errs[i])
			require.Equal(t, "oidc-ak", aks[i])
		}
		require.Equal(t, int32(1), hits.Load())
	})

	t.Run("failed early refresh backs off while cached credentials are valid", func(t *testing.T) {
		provider, hits := newProvider(t, time.Minute)
		cred, err := provider.Retrieve(t.Context())
		require.NoError(t, err)

		got, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, cred, got)
		// Join the early refresh if it hasn't already finished.
		provider.mu.Lock()
		refresh := provider.inflight
		provider.mu.Unlock()
		if refresh != nil {
			<-refresh.done
		}
		cached, valid, retry := provider.current()
		require.True(t, valid)
		require.Equal(t, cred, cached)
		require.Nil(t, retry)
		require.Equal(t, int32(2), hits.Load())
	})
}

func TestVolcengineOIDCCredentialsProviderErrors(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("oidc-token"), 0o600))

	testCases := []struct {
		name    string
		resp    func() volcengineOIDCSTSResponse
		wantErr string
	}{
		{
			name: "incomplete credentials",
			resp: func() volcengineOIDCSTSResponse {
				var r volcengineOIDCSTSResponse
				r.Result.Credentials.AccessKeyID = "oidc-ak"
				return r
			},
			wantErr: "volcengine OIDC STS returned incomplete credentials",
		},
		{
			name: "missing expiration",
			resp: func() volcengineOIDCSTSResponse {
				var r volcengineOIDCSTSResponse
				r.Result.Credentials.AccessKeyID = "oidc-ak"
				r.Result.Credentials.SecretAccessKey = "oidc-sk"
				r.Result.Credentials.SessionToken = "oidc-sess"
				return r
			},
			wantErr: "volcengine OIDC STS returned empty expiration",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(tc.resp()))
			}))
			t.Cleanup(server.Close)

			prevURL := volcengineOIDCSTSURL
			volcengineOIDCSTSURL = func(string) string { return server.URL }
			t.Cleanup(func() { volcengineOIDCSTSURL = prevURL })

			provider := &volcengineOIDCCredentialsProvider{
				region:    "cn-beijing",
				roleTRN:   "trn:iam::1234567890:role/metering",
				tokenFile: tokenFile,
			}
			_, err := provider.Retrieve(t.Context())
			require.EqualError(t, err, tc.wantErr)
		})
	}
}
