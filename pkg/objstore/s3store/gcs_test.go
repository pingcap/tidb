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

package s3store

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestIsGCSS3Compatible(t *testing.T) {
	require.True(t, isGCSS3Compatible(&backuppb.S3{
		Provider: "gcs",
		Endpoint: "http://127.0.0.1:9000",
	}))
	require.True(t, isGCSS3Compatible(&backuppb.S3{
		Provider: "ceph",
		Endpoint: "https://storage.googleapis.com",
	}))
	require.True(t, isGCSS3Compatible(&backuppb.S3{
		Endpoint: "https://storage.googleapis.com/",
	}))
	require.False(t, isGCSS3Compatible(&backuppb.S3{
		Provider: "ceph",
		Endpoint: "https://s3.example.com",
	}))
	require.False(t, isGCSS3Compatible(&backuppb.S3{
		Endpoint: "://bad-endpoint",
	}))
}

func TestGCSS3CompatibleSignerSkipsAcceptEncodingWithRetry(t *testing.T) {
	t.Setenv(gcsS3FaultRateEnv, "0")

	const listObjectsV2Response = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix></Prefix>
  <KeyCount>0</KeyCount>
  <MaxKeys>1</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>`

	type requestInfo struct {
		method        string
		signedHeaders string
		listType      string
		statusCode    int
		writeErr      error
	}

	var (
		mu          sync.Mutex
		getAttempts int
		requests    []requestInfo
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := requestInfo{
			method:        r.Method,
			signedHeaders: getSignedHeaders(r.Header.Get("Authorization")),
			listType:      r.URL.Query().Get("list-type"),
		}
		defer func() {
			mu.Lock()
			requests = append(requests, info)
			mu.Unlock()
		}()

		switch r.Method {
		case http.MethodHead:
			info.statusCode = http.StatusOK
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			mu.Lock()
			getAttempts++
			getAttempt := getAttempts
			mu.Unlock()

			if getAttempt == 1 {
				info.statusCode = http.StatusInternalServerError
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			info.statusCode = http.StatusOK
			w.Header().Set("Content-Type", "application/xml")
			_, info.writeErr = w.Write([]byte(listObjectsV2Response))
		default:
			info.statusCode = http.StatusNotFound
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	storage, err := NewS3Storage(context.Background(), &backuppb.S3{
		Bucket:          "bucket",
		Endpoint:        server.URL,
		Provider:        "gcs",
		ForcePathStyle:  true,
		AccessKey:       "access-key",
		SecretAccessKey: "secret-access-key",
	}, &storeapi.Options{
		CheckPermissions: []storeapi.Permission{storeapi.AccessBuckets, storeapi.ListObjects},
	})
	require.NoError(t, err)
	require.NotNil(t, storage)

	mu.Lock()
	observedRequests := append([]requestInfo(nil), requests...)
	mu.Unlock()
	require.Len(t, observedRequests, 3)

	var headSeen bool
	var listStatuses []int
	for _, req := range observedRequests {
		require.NoError(t, req.writeErr)
		require.NotEmpty(t, req.signedHeaders)
		require.NotContains(t, req.signedHeaders, "accept-encoding")
		require.Contains(t, req.signedHeaders, "amz-sdk-invocation-id")
		require.Contains(t, req.signedHeaders, "amz-sdk-request")
		require.Contains(t, req.signedHeaders, "host")
		require.Contains(t, req.signedHeaders, "x-amz-content-sha256")
		require.Contains(t, req.signedHeaders, "x-amz-date")

		switch req.method {
		case http.MethodHead:
			headSeen = true
			require.Equal(t, http.StatusOK, req.statusCode)
		case http.MethodGet:
			require.Equal(t, "2", req.listType)
			listStatuses = append(listStatuses, req.statusCode)
		default:
			require.Failf(t, "unexpected request method", "method: %s", req.method)
		}
	}
	require.True(t, headSeen)
	require.Equal(t, []int{http.StatusInternalServerError, http.StatusOK}, listStatuses)
}

func TestGCSS3CompatibleFaultInjectionRetries(t *testing.T) {
	const listObjectsV2Response = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix></Prefix>
  <KeyCount>0</KeyCount>
  <MaxKeys>1</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>`

	for _, mode := range []string{gcsS3FaultModeHTTP500, gcsS3FaultModeConnectionReset} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(gcsS3FaultRateEnv, "1")
			t.Setenv(gcsS3FaultModeEnv, mode)
			t.Setenv(gcsS3FaultMaxEnv, "1")

			type faultRequestInfo struct {
				method   string
				listType string
				writeErr error
			}

			var (
				mu       sync.Mutex
				requests []faultRequestInfo
			)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				info := faultRequestInfo{
					method:   r.Method,
					listType: r.URL.Query().Get("list-type"),
				}
				defer func() {
					mu.Lock()
					requests = append(requests, info)
					mu.Unlock()
				}()

				switch r.Method {
				case http.MethodHead:
					w.WriteHeader(http.StatusOK)
				case http.MethodGet:
					w.Header().Set("Content-Type", "application/xml")
					_, info.writeErr = w.Write([]byte(listObjectsV2Response))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			storage, err := NewS3Storage(context.Background(), &backuppb.S3{
				Bucket:          "bucket",
				Endpoint:        server.URL,
				Provider:        "gcs",
				ForcePathStyle:  true,
				AccessKey:       "access-key",
				SecretAccessKey: "secret-access-key",
			}, &storeapi.Options{
				CheckPermissions: []storeapi.Permission{storeapi.AccessBuckets, storeapi.ListObjects},
			})
			require.NoError(t, err)
			require.NotNil(t, storage)

			mu.Lock()
			observedRequests := append([]faultRequestInfo(nil), requests...)
			mu.Unlock()
			require.Len(t, observedRequests, 2)
			require.Equal(t, http.MethodHead, observedRequests[0].method)
			require.Equal(t, http.MethodGet, observedRequests[1].method)
			require.Equal(t, "2", observedRequests[1].listType)
			require.NoError(t, observedRequests[1].writeErr)
		})
	}
}

func getSignedHeaders(authorization string) string {
	for _, part := range strings.Split(authorization, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "SignedHeaders=") {
			return strings.TrimPrefix(part, "SignedHeaders=")
		}
	}
	return ""
}
