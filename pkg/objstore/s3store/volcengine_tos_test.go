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
	"context"
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
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestAutoNewCredVolcengine(t *testing.T) {
	t.Setenv(volcengineOIDCTokenFileEnv, "")
	t.Setenv(volcengineOIDCRoleTRNEnv, "")
	t.Setenv(volcengineOIDCRolePolicyEnv, "")
	t.Setenv(volcengineOIDCSTSEndpointEnv, "")
	t.Setenv(volcengineRegionEnv, "")
	t.Setenv(volcengineIRSARegionEnv, "")

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

	t.Run("infer signing region from TOS S3 endpoint", func(t *testing.T) {
		testCases := []struct {
			endpoint string
			expected string
		}{
			{endpoint: "https://tos-s3-cn-beijing.volces.com", expected: "cn-beijing"},
			{endpoint: "https://tos-s3-cn-shanghai.ivolces.com", expected: "cn-shanghai"},
			{endpoint: "https://bucket.tos-s3-ap-southeast-1.ivolces.com:443", expected: "ap-southeast-1"},
			{endpoint: "tos-s3-CN-BEIJING.ivolces.com", expected: "cn-beijing"},
			{endpoint: "https://tos-cn-beijing.volces.com", expected: ""},
			{endpoint: "https://volces.com", expected: ""},
			{endpoint: "https://s3.example.com", expected: ""},
		}
		for _, tc := range testCases {
			require.Equal(t, tc.expected, inferVolcengineRegionFromEndpoint(tc.endpoint), tc.endpoint)
		}
	})

	t.Run("normalizeVolcengineTOS signing region precedence", func(t *testing.T) {
		t.Run("standard TOS endpoint beats VOLCENGINE_REGION", func(t *testing.T) {
			t.Setenv(volcengineRegionEnv, "cn-shanghai")
			t.Setenv(volcengineIRSARegionEnv, "cn-guangzhou")
			qs := &backuppb.S3{
				Endpoint:       "https://tos-s3-cn-beijing.ivolces.com",
				ForcePathStyle: true,
			}
			ok, err := normalizeVolcengineTOS(qs)
			require.NoError(t, err)
			require.True(t, ok)
			require.False(t, qs.ForcePathStyle)
			require.Equal(t, "cn-beijing", qs.Region)
		})

		t.Run("custom TOS endpoint falls back to VOLCENGINE_REGION", func(t *testing.T) {
			t.Setenv(volcengineRegionEnv, "cn-beijing")
			t.Setenv(volcengineIRSARegionEnv, "cn-shanghai")
			qs := &backuppb.S3{
				Provider:       "volcengine",
				Endpoint:       "https://tos.internal.example.com",
				ForcePathStyle: true,
			}
			ok, err := normalizeVolcengineTOS(qs)
			require.NoError(t, err)
			require.True(t, ok)
			require.False(t, qs.ForcePathStyle)
			require.Equal(t, "cn-beijing", qs.Region)
		})

		t.Run("custom TOS endpoint ignores generic REGION env", func(t *testing.T) {
			t.Setenv(volcengineRegionEnv, "")
			t.Setenv(volcengineIRSARegionEnv, "cn-shanghai")
			qs := &backuppb.S3{
				Provider:       "volcengine",
				Endpoint:       "https://tos.internal.example.com",
				ForcePathStyle: true,
				RoleArn:        "   ",
			}
			ok, err := normalizeVolcengineTOS(qs)
			require.NoError(t, err)
			require.True(t, ok)
			require.False(t, qs.ForcePathStyle)
			require.Empty(t, qs.Region)
			require.Empty(t, qs.RoleArn)
		})

		t.Run("non-TOS provider preserves role-arn", func(t *testing.T) {
			qs := &backuppb.S3{
				Provider: "aws",
				Endpoint: "https://s3.us-east-1.amazonaws.com",
				RoleArn:  "arn:aws:iam::123456789012:role/test-role",
			}
			ok, err := normalizeVolcengineTOS(qs)
			require.NoError(t, err)
			require.False(t, ok)
			require.Equal(t, "arn:aws:iam::123456789012:role/test-role", qs.RoleArn)
		})
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

	t.Run("role-arn is rejected for all TOS credential modes", func(t *testing.T) {
		testCases := []struct {
			name    string
			oidcEnv bool
			backend *backuppb.S3
		}{
			{
				name: "default credential chain",
				backend: &backuppb.S3{
					Bucket:   "test-bucket",
					Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
					Provider: "volcengine",
					Region:   "cn-beijing",
					RoleArn:  "trn:iam::1234567890:role/restricted",
				},
			},
			{
				name:    "pod OIDC env configured",
				oidcEnv: true,
				backend: &backuppb.S3{
					Bucket:   "test-bucket",
					Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
					Provider: "volcengine",
					Region:   "cn-beijing",
					RoleArn:  "trn:iam::1234567890:role/restricted",
				},
			},
			{
				name: "explicit static credentials",
				backend: &backuppb.S3{
					Bucket:          "test-bucket",
					Endpoint:        "https://tos-s3-cn-beijing.ivolces.com",
					Provider:        "volcengine",
					Region:          "cn-beijing",
					AccessKey:       "explicit-id",
					SecretAccessKey: "explicit-key",
					RoleArn:         "trn:iam::1234567890:role/restricted",
				},
			},
			{
				name: "shared config profile",
				backend: &backuppb.S3{
					Bucket:   "test-bucket",
					Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
					Provider: "volcengine",
					Region:   "cn-beijing",
					Profile:  "volcengine-profile",
					RoleArn:  "trn:iam::1234567890:role/restricted",
				},
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.oidcEnv {
					t.Setenv(volcengineOIDCTokenFileEnv, "/var/run/secrets/token")
					t.Setenv(volcengineOIDCRoleTRNEnv, "trn:iam::1234567890:role/pod-dfs")
				} else {
					t.Setenv(volcengineOIDCTokenFileEnv, "")
					t.Setenv(volcengineOIDCRoleTRNEnv, "")
				}
				storage, err := NewS3Storage(t.Context(), tc.backend, &storeapi.Options{})
				require.ErrorIs(t, err, berrors.ErrStorageInvalidConfig)
				require.ErrorContains(t, err, "volcengine TOS does not support role-arn")
				require.Nil(t, storage)
			})
		}
	})

	t.Run("partial OIDC env fails closed", func(t *testing.T) {
		t.Run("only token file set", func(t *testing.T) {
			t.Setenv(volcengineOIDCTokenFileEnv, "/var/run/secrets/token")
			t.Setenv(volcengineOIDCRoleTRNEnv, "")
			provider, err := autoNewCred(&backuppb.S3{
				Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
				Region:   "cn-beijing",
			})
			require.ErrorContains(t, err, "incomplete volcengine OIDC environment")
			require.ErrorContains(t, err, volcengineOIDCTokenFileEnv)
			require.ErrorContains(t, err, volcengineOIDCRoleTRNEnv)
			require.Nil(t, provider)
		})

		t.Run("only role TRN set", func(t *testing.T) {
			t.Setenv(volcengineOIDCTokenFileEnv, "")
			t.Setenv(volcengineOIDCRoleTRNEnv, "trn:iam::1234567890:role/metering")
			provider, err := autoNewCred(&backuppb.S3{
				Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
				Region:   "cn-beijing",
			})
			require.ErrorContains(t, err, "incomplete volcengine OIDC environment")
			require.Nil(t, provider)
		})
	})

	t.Run("STS endpoint resolution priority", func(t *testing.T) {
		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("oidc-token"), 0o600))
		t.Setenv(volcengineOIDCTokenFileEnv, tokenFile)
		t.Setenv(volcengineOIDCRoleTRNEnv, "trn:iam::1234567890:role/metering")

		t.Run("VOLCENGINE_OIDC_STS_ENDPOINT takes highest precedence", func(t *testing.T) {
			t.Setenv(volcengineOIDCSTSEndpointEnv, "sts.internal.example.com:8443")
			t.Setenv(volcengineIRSARegionEnv, "cn-shanghai")
			provider, err := autoNewCred(&backuppb.S3{
				Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
				Region:   "cn-beijing",
			})
			require.NoError(t, err)
			p := provider.(*volcengineOIDCCredentialsProvider)
			require.Equal(t, "https://sts.internal.example.com:8443/?"+volcengineOIDCSTSQuery, p.stsURL)
		})

		t.Run("VOLCENGINE_OIDC_STS_ENDPOINT accepts explicit https scheme", func(t *testing.T) {
			t.Setenv(volcengineOIDCSTSEndpointEnv, "https://sts.internal.example.com:8443/")
			provider, err := autoNewCred(&backuppb.S3{
				Provider: "volcengine",
			})
			require.NoError(t, err)
			p := provider.(*volcengineOIDCCredentialsProvider)
			require.Equal(t, "https://sts.internal.example.com:8443/?"+volcengineOIDCSTSQuery, p.stsURL)
		})

		t.Run("REGION takes precedence over backend region", func(t *testing.T) {
			t.Setenv(volcengineOIDCSTSEndpointEnv, "")
			t.Setenv(volcengineRegionEnv, "cn-guangzhou")
			t.Setenv(volcengineIRSARegionEnv, "cn-shanghai")
			provider, err := autoNewCred(&backuppb.S3{
				Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
				Region:   "cn-beijing",
			})
			require.NoError(t, err)
			p := provider.(*volcengineOIDCCredentialsProvider)
			require.Equal(t, "https://sts.cn-shanghai.volcengineapi.com/?"+volcengineOIDCSTSQuery, p.stsURL)
		})

		t.Run("backend region used when REGION is empty", func(t *testing.T) {
			t.Setenv(volcengineOIDCSTSEndpointEnv, "")
			t.Setenv(volcengineRegionEnv, "cn-guangzhou")
			t.Setenv(volcengineIRSARegionEnv, "")
			provider, err := autoNewCred(&backuppb.S3{
				Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
				Region:   " cn-beijing ",
			})
			require.NoError(t, err)
			p := provider.(*volcengineOIDCCredentialsProvider)
			require.Equal(t, "https://sts.cn-beijing.volcengineapi.com/?"+volcengineOIDCSTSQuery, p.stsURL)
		})

		t.Run("VOLCENGINE_REGION used when REGION and backend region are empty", func(t *testing.T) {
			t.Setenv(volcengineOIDCSTSEndpointEnv, "")
			t.Setenv(volcengineRegionEnv, "cn-beijing")
			t.Setenv(volcengineIRSARegionEnv, "")
			provider, err := autoNewCred(&backuppb.S3{
				Provider: "volcengine",
				Endpoint: "https://tos.internal.example.com",
			})
			require.NoError(t, err)
			p := provider.(*volcengineOIDCCredentialsProvider)
			require.Equal(t, "https://sts.cn-beijing.volcengineapi.com/?"+volcengineOIDCSTSQuery, p.stsURL)
		})

		t.Run("global STS default when no region is configured", func(t *testing.T) {
			t.Setenv(volcengineOIDCSTSEndpointEnv, "")
			t.Setenv(volcengineRegionEnv, "")
			t.Setenv(volcengineIRSARegionEnv, "")
			provider, err := autoNewCred(&backuppb.S3{
				Provider: "volcengine",
				Endpoint: "https://tos.internal.example.com",
			})
			require.NoError(t, err)
			p := provider.(*volcengineOIDCCredentialsProvider)
			require.Equal(t, "https://sts.volcengineapi.com/?"+volcengineOIDCSTSQuery, p.stsURL)
		})
	})

	t.Run("invalid region and STS endpoint rejected before sending STS request", func(t *testing.T) {
		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("oidc-token"), 0o600))
		t.Setenv(volcengineOIDCTokenFileEnv, tokenFile)
		t.Setenv(volcengineOIDCRoleTRNEnv, "trn:iam::1234567890:role/metering")

		for _, badRegion := range []string{"evil.com#", "cn-beijing@evil.com", "cn-beijing/foo", "-cn-beijing"} {
			t.Setenv(volcengineOIDCSTSEndpointEnv, "")
			t.Setenv(volcengineIRSARegionEnv, "")
			provider, err := autoNewCred(&backuppb.S3{
				Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
				Region:   badRegion,
			})
			require.ErrorContains(t, err, "invalid volcengine region")
			require.Nil(t, provider)

			t.Setenv(volcengineIRSARegionEnv, badRegion)
			provider, err = autoNewCred(&backuppb.S3{
				Endpoint: "https://tos-s3-cn-beijing.ivolces.com",
				Region:   "cn-beijing",
			})
			require.ErrorContains(t, err, "invalid volcengine region")
			require.Nil(t, provider)
		}

		t.Setenv(volcengineIRSARegionEnv, "")
		for _, badEndpoint := range []string{
			"http://sts.example.com",
			"http://127.0.0.1:18080/",
			"https://user:supersecret@sts.example.com",
			"ftp://sts.example.com",
			"sts.example.com/extra?foo=bar",
			"://bad-url",
		} {
			t.Setenv(volcengineOIDCSTSEndpointEnv, badEndpoint)
			provider, err := autoNewCred(&backuppb.S3{
				Provider: "volcengine",
				Region:   "cn-beijing",
			})
			require.ErrorContains(t, err, "invalid "+volcengineOIDCSTSEndpointEnv)
			require.NotContains(t, err.Error(), "supersecret")
			require.Nil(t, provider)
		}
	})
}

func TestVolcengineTOSEndpointOnlyEndToEnd(t *testing.T) {
	testCases := []struct {
		name            string
		sendCredentials bool
	}{
		{name: "without SendCredentials", sendCredentials: false},
		// SendCredentials retrieves from the AWS config credentials before the S3
		// client signs requests; both must share one OIDC exchange.
		{name: "with SendCredentials", sendCredentials: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testVolcengineTOSEndpointOnlyEndToEnd(t, tc.sendCredentials)
		})
	}
}

func testVolcengineTOSEndpointOnlyEndToEnd(t *testing.T, sendCredentials bool) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(" oidc-token \n"), 0o600))
	t.Setenv(volcengineOIDCTokenFileEnv, tokenFile)
	t.Setenv(volcengineOIDCRoleTRNEnv, "trn:iam::1234567890:role/metering")
	const sessionPolicy = `{"Statement":[{"Effect":"Allow","Action":["tos:GetObject"],"Resource":["trn:tos:::my-bucket/prefix/*"]}]}`
	t.Setenv(volcengineOIDCRolePolicyEnv, " "+sessionPolicy+" ")
	t.Setenv(volcengineRegionEnv, "")
	// Simulate VKE IRSA injecting REGION=cn-shanghai while accessing a cn-beijing
	// TOS bucket without explicit region/provider in the URI: S3 SigV4 signing
	// must use cn-beijing inferred from the TOS host, never REGION from env.
	t.Setenv(volcengineIRSARegionEnv, "cn-shanghai")

	var stsHits atomic.Int32
	stsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stsHits.Add(1)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "AssumeRoleWithOIDC", r.URL.Query().Get("Action"))
		require.Equal(t, "2018-01-01", r.URL.Query().Get("Version"))
		require.NoError(t, r.ParseForm())
		require.Equal(t, "oidc-token", r.PostForm.Get("OIDCToken"))
		require.Equal(t, "trn:iam::1234567890:role/metering", r.PostForm.Get("RoleTrn"))
		require.Equal(t, sessionPolicy, r.PostForm.Get("Policy"))

		resp := volcengineOIDCSTSResponse{}
		resp.Result.Credentials.AccessKeyID = "oidc-ak"
		resp.Result.Credentials.SecretAccessKey = "oidc-sk"
		resp.Result.Credentials.SessionToken = "oidc-sess"
		resp.Result.Credentials.Expiration = time.Now().Add(time.Hour).Format(time.RFC3339)
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(stsServer.Close)
	prevClientFactory := newVolcengineOIDCHTTPClient
	newVolcengineOIDCHTTPClient = stsServer.Client
	t.Cleanup(func() { newVolcengineOIDCHTTPClient = prevClientFactory })
	t.Setenv(volcengineOIDCSTSEndpointEnv, stsServer.URL)

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

	rawURL := "s3://my-bucket/prefix?endpoint=https://tos-s3-cn-beijing.ivolces.com"
	s3Opts := s3like.S3BackendOptions{
		Endpoint:       "https://tos-s3-cn-beijing.ivolces.com",
		ForcePathStyle: true,
	}
	s3Opts.SetForcePathStyle(rawURL)
	backend := &backuppb.S3{Bucket: "my-bucket", Prefix: "prefix"}
	require.NoError(t, s3Opts.Apply(backend))

	store, err := NewS3Storage(t.Context(), backend, &storeapi.Options{
		HTTPClient:      newRewriteHostHTTPClient(tosServer.URL),
		SendCredentials: sendCredentials,
	})
	require.NoError(t, err)
	defer store.Close()
	if sendCredentials {
		require.Equal(t, "oidc-ak", backend.AccessKey)
		require.Equal(t, "oidc-sess", backend.SessionToken)
	} else {
		require.Empty(t, backend.AccessKey)
	}

	data, err := store.ReadFile(t.Context(), "hello.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("hello-tos"), data)

	// Subsequent requests reuse cached credentials without another STS exchange.
	data, err = store.ReadFile(t.Context(), "hello.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("hello-tos"), data)
	require.Equal(t, int32(1), stsHits.Load())

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{http.MethodGet, http.MethodGet}, methods)
	require.Equal(t, "my-bucket.tos-s3-cn-beijing.ivolces.com", gotHost)
	require.Equal(t, "/prefix/hello.txt", gotPath)
	require.Contains(t, gotAuth, "/cn-beijing/s3/aws4_request")
	require.Equal(t, "oidc-sess", gotTok)
}

func TestVolcengineOIDCCredentialsProviderRefresh(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("oidc-token"), 0o600))

	newProvider := func(t *testing.T, handler http.HandlerFunc) (*volcengineOIDCCredentialsProvider, *httptest.Server) {
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		return &volcengineOIDCCredentialsProvider{
			stsURL:     server.URL,
			roleTRN:    "trn:iam::1234567890:role/metering",
			tokenFile:  tokenFile,
			httpClient: server.Client(),
			retryDelay: time.Millisecond,
		}, server
	}

	writeCred := func(t *testing.T, w http.ResponseWriter, ak string, validity time.Duration, useExpiredTime bool) {
		resp := volcengineOIDCSTSResponse{}
		resp.Result.Credentials.AccessKeyID = ak
		resp.Result.Credentials.SecretAccessKey = ak + "-sk"
		resp.Result.Credentials.SessionToken = ak + "-sess"
		exp := time.Now().Add(validity).Format(time.RFC3339)
		if useExpiredTime {
			resp.Result.Credentials.ExpiredTime = exp
		} else {
			resp.Result.Credentials.Expiration = exp
		}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}

	t.Run("concurrent cold calls share one exchange", func(t *testing.T) {
		var hits atomic.Int32
		provider, _ := newProvider(t, func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			writeCred(t, w, "oidc-ak", time.Hour, false)
		})

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

	t.Run("short-lived credential uses proportional refresh window", func(t *testing.T) {
		var hits atomic.Int32
		provider, _ := newProvider(t, func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			writeCred(t, w, "short-ak", time.Minute, true)
		})

		first, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, "short-ak", first.AccessKeyID)

		// Must not immediately trigger early refresh right after fetching a 1m credential.
		cached, valid, inflight := provider.current()
		require.True(t, valid)
		require.Equal(t, first, cached)
		require.Nil(t, inflight)
		require.Equal(t, int32(1), hits.Load())
	})

	t.Run("successful early refresh asynchronously replaces cached credentials", func(t *testing.T) {
		var hits atomic.Int32
		refreshStarted := make(chan struct{})
		releaseRefresh := make(chan struct{})
		provider, _ := newProvider(t, func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) == 1 {
				writeCred(t, w, "ak-v1", time.Hour, false)
				return
			}
			close(refreshStarted)
			<-releaseRefresh
			writeCred(t, w, "ak-v2", time.Hour, false)
		})

		v1, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, "ak-v1", v1.AccessKeyID)

		// Move into the early-refresh window.
		provider.mu.Lock()
		provider.refreshAt = time.Now().Add(-time.Second)
		provider.mu.Unlock()

		// Caller immediately gets still-valid v1 while refresh runs in background.
		got, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, "ak-v1", got.AccessKeyID)

		<-refreshStarted
		provider.mu.Lock()
		refresh := provider.inflight
		provider.mu.Unlock()
		require.NotNil(t, refresh)
		close(releaseRefresh)
		<-refresh.done

		v2, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, "ak-v2", v2.AccessKeyID)
		require.Equal(t, int32(2), hits.Load())
	})

	t.Run("failed early refresh backs off while cached credentials are valid", func(t *testing.T) {
		var hits atomic.Int32
		provider, _ := newProvider(t, func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) > 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			writeCred(t, w, "oidc-ak", time.Hour, false)
		})

		cred, err := provider.Retrieve(t.Context())
		require.NoError(t, err)

		provider.mu.Lock()
		provider.refreshAt = time.Now().Add(-time.Second)
		provider.mu.Unlock()

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

	t.Run("failed cold refresh does not back off without valid credentials", func(t *testing.T) {
		var hits atomic.Int32
		provider, _ := newProvider(t, func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) == 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			writeCred(t, w, "recovered-ak", time.Hour, false)
		})

		_, err := provider.Retrieve(t.Context())
		require.ErrorContains(t, err, "returned status 403")
		// The next caller retries STS immediately instead of failing for the backoff interval.
		cred, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, "recovered-ak", cred.AccessKeyID)
		require.Equal(t, int32(2), hits.Load())
	})

	t.Run("canceled waiter does not abort shared refresh", func(t *testing.T) {
		var hits atomic.Int32
		started := make(chan struct{})
		release := make(chan struct{})
		provider, _ := newProvider(t, func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			close(started)
			<-release
			writeCred(t, w, "shared-ak", time.Hour, false)
		})

		ctx1, cancel1 := context.WithCancel(t.Context())
		errCh1 := make(chan error, 1)
		go func() {
			_, err := provider.Retrieve(ctx1)
			errCh1 <- err
		}()

		<-started
		credCh2 := make(chan string, 1)
		errCh2 := make(chan error, 1)
		go func() {
			cred, err := provider.Retrieve(t.Context())
			credCh2 <- cred.AccessKeyID
			errCh2 <- err
		}()

		cancel1()
		require.ErrorIs(t, <-errCh1, context.Canceled)

		close(release)
		require.NoError(t, <-errCh2)
		require.Equal(t, "shared-ak", <-credCh2)
		require.Equal(t, int32(1), hits.Load())
	})

	t.Run("retryable STS statuses retry and recover", func(t *testing.T) {
		var hits atomic.Int32
		provider, _ := newProvider(t, func(w http.ResponseWriter, r *http.Request) {
			switch hits.Add(1) {
			case 1:
				w.WriteHeader(http.StatusTooManyRequests)
			case 2:
				w.WriteHeader(http.StatusServiceUnavailable)
			default:
				writeCred(t, w, "recovered-ak", time.Hour, false)
			}
		})

		cred, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, "recovered-ak", cred.AccessKeyID)
		require.Equal(t, int32(3), hits.Load())
	})

	t.Run("non-retryable STS status does not retry", func(t *testing.T) {
		var hits atomic.Int32
		provider, _ := newProvider(t, func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"ResponseMetadata":{"RequestId":"req-403","Error":{"Code":"AccessDenied","Message":"denied"}}}`))
		})

		_, err := provider.Retrieve(t.Context())
		require.ErrorContains(t, err, "status=403")
		require.ErrorContains(t, err, "AccessDenied")
		require.ErrorContains(t, err, "request-id=req-403")
		require.Equal(t, int32(1), hits.Load())
	})
}

func TestVolcengineOIDCCredentialsProviderErrors(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("oidc-token"), 0o600))

	testCases := []struct {
		name         string
		statusCode   int
		rawBody      string
		resp         func() volcengineOIDCSTSResponse
		wantErr      string
		forbidSubstr string
	}{
		{
			name: "incomplete credentials with request id",
			resp: func() volcengineOIDCSTSResponse {
				var r volcengineOIDCSTSResponse
				r.ResponseMetadata.RequestID = "req-incomplete"
				r.Result.Credentials.AccessKeyID = "oidc-ak"
				return r
			},
			wantErr: "volcengine OIDC STS returned incomplete credentials (request-id=req-incomplete)",
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
		{
			name: "expired credentials",
			resp: func() volcengineOIDCSTSResponse {
				var r volcengineOIDCSTSResponse
				r.Result.Credentials.AccessKeyID = "oidc-ak"
				r.Result.Credentials.SecretAccessKey = "oidc-sk"
				r.Result.Credentials.SessionToken = "oidc-sess"
				r.Result.Credentials.Expiration = time.Now().Add(-time.Minute).Format(time.RFC3339)
				return r
			},
			wantErr: "volcengine OIDC STS returned expired credentials",
		},
		{
			name: "http 200 with metadata error",
			resp: func() volcengineOIDCSTSResponse {
				var r volcengineOIDCSTSResponse
				r.ResponseMetadata.RequestID = "req-200-err"
				r.ResponseMetadata.Error = &struct {
					Code    string `json:"Code"`
					Message string `json:"Message"`
				}{
					Code:    "InvalidParameter",
					Message: "bad token",
				}
				return r
			},
			wantErr: "volcengine OIDC STS InvalidParameter: bad token (request-id=req-200-err)",
		},
		{
			name:         "non-200 non-JSON does not leak raw body",
			statusCode:   http.StatusBadRequest,
			rawBody:      "<html>sensitive-proxy-secret</html>",
			wantErr:      "volcengine OIDC STS returned status 400 (body-length=35)",
			forbidSubstr: "sensitive-proxy-secret",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.statusCode != 0 {
					w.WriteHeader(tc.statusCode)
				}
				if tc.rawBody != "" {
					_, _ = w.Write([]byte(tc.rawBody))
					return
				}
				require.NoError(t, json.NewEncoder(w).Encode(tc.resp()))
			}))
			t.Cleanup(server.Close)

			provider := &volcengineOIDCCredentialsProvider{
				stsURL:     server.URL,
				roleTRN:    "trn:iam::1234567890:role/metering",
				tokenFile:  tokenFile,
				httpClient: server.Client(),
				retryDelay: time.Millisecond,
			}
			_, err := provider.Retrieve(t.Context())
			require.EqualError(t, err, tc.wantErr)
			if tc.forbidSubstr != "" {
				require.NotContains(t, err.Error(), tc.forbidSubstr)
			}
		})
	}

	t.Run("missing token file", func(t *testing.T) {
		provider := &volcengineOIDCCredentialsProvider{
			stsURL:    "https://sts.volcengineapi.com/?" + volcengineOIDCSTSQuery,
			roleTRN:   "trn:iam::1234567890:role/metering",
			tokenFile: filepath.Join(t.TempDir(), "does-not-exist"),
		}
		_, err := provider.Retrieve(t.Context())
		require.ErrorContains(t, err, "read volcengine OIDC token file")
	})

	t.Run("STS redirects are not followed", func(t *testing.T) {
		var redirectTargetHits atomic.Int32
		targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			redirectTargetHits.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(targetServer.Close)

		stsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, targetServer.URL, http.StatusTemporaryRedirect)
		}))
		t.Cleanup(stsServer.Close)

		provider := &volcengineOIDCCredentialsProvider{
			stsURL:     stsServer.URL,
			roleTRN:    "trn:iam::1234567890:role/metering",
			tokenFile:  tokenFile,
			retryDelay: time.Millisecond,
		}
		_, err := provider.Retrieve(t.Context())
		require.EqualError(t, err, "volcengine OIDC STS returned status 307 (body-length=0)")
		require.Zero(t, redirectTargetHits.Load())
	})
}
