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

package objstore

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/ossstore"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/s3store"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

type rejectEndpointTransport struct {
	t *testing.T
}

func (r rejectEndpointTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.t.Error("malformed endpoints must be rejected before HTTP requests")
	return nil, errors.New("unexpected HTTP request")
}

func TestMalformedEndpointDoesNotLeakCredentials(t *testing.T) {
	for _, tc := range []struct {
		scheme            string
		rejectDuringParse bool
	}{
		{"azure", false},
		{"gcs", false},
		{"s3", true},
		{"ks3", true},
		{"oss", true},
	} {
		for _, endpoint := range []string{
			"https://FAKE_USER:FAKE_PASS@host/path%zz?sig=FAKE_SIGNATURE",
			"https://FAKE_USER:FAKE_PASS@host:badport/path?sig=FAKE_SIGNATURE",
			"https://FAKE_USER:FAKE_PASS@host/path\n?sig=FAKE_SIGNATURE",
		} {
			t.Run(tc.scheme+"/"+url.QueryEscape(endpoint), func(t *testing.T) {
				// Given a valid storage URL with an escaped, malformed endpoint.
				backend, err := ParseBackend(tc.scheme+"://bucket/prefix?account-name=fake&sas-token=FAKE_SAS&endpoint="+url.QueryEscape(endpoint), nil)
				if tc.rejectDuringParse {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()

					// When the parser accepts the backend, construct the real SDK-backed storage.
					_, err = New(ctx, backend, &storeapi.Options{
						NoCredentials: true,
						HTTPClient:    &http.Client{Transport: rejectEndpointTransport{t: t}},
					})
				}

				// Then no error representation can recover the endpoint credentials.
				require.Error(t, err)
				for _, output := range []string{err.Error(), fmt.Sprintf("%v", err), fmt.Sprintf("%+v", err)} {
					for _, secret := range []string{"FAKE_USER", "FAKE_PASS", "FAKE_SIGNATURE", "FAKE_SAS"} {
						require.NotContains(t, output, secret)
					}
				}
				require.ErrorIs(t, err, berrors.ErrStorageInvalidConfig)
			})
		}
	}
}

func TestMalformedS3BackendDoesNotLeakCredentials(t *testing.T) {
	for _, provider := range []string{"", "aws", "minio", s3like.KS3SDKProvider, s3like.OSSProvider} {
		for _, entry := range []string{"dispatcher", "constructor"} {
			for _, endpoint := range []string{
				"https://FAKE_USER:FAKE_PASS@host/path%zz?sig=FAKE_SIGNATURE",
				"https://FAKE_USER:FAKE_PASS@host:badport/path?sig=FAKE_SIGNATURE",
				"https://FAKE_USER:FAKE_PASS@host/path\n?sig=FAKE_SIGNATURE",
			} {
				t.Run(provider+"/"+entry+"/"+url.QueryEscape(endpoint), func(t *testing.T) {
					// Given a backend that bypasses ParseBackend's endpoint validation.
					backend := &backuppb.S3{
						Provider: provider, Endpoint: endpoint, Bucket: "bucket", Region: "us-east-1",
						AccessKey: "FAKE_ACCESS_KEY", SecretAccessKey: "FAKE_SECRET_KEY", ForcePathStyle: true,
					}
					original, err := backend.Marshal()
					require.NoError(t, err)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					opts := &storeapi.Options{HTTPClient: &http.Client{Transport: rejectEndpointTransport{t: t}}}

					// When constructing through either public entry point.
					if entry == "dispatcher" {
						_, err = New(ctx, &backuppb.StorageBackend{Backend: &backuppb.StorageBackend_S3{S3: backend}}, opts)
					} else if provider == s3like.KS3SDKProvider {
						_, err = s3store.NewKS3Storage(ctx, backend, opts)
					} else if provider == s3like.OSSProvider {
						_, err = ossstore.NewOSSStorage(ctx, backend, opts)
					} else {
						_, err = s3store.NewS3Storage(ctx, backend, opts)
					}

					// Then rejection occurs before SDK errors, logs or credential mutation.
					require.Error(t, err)
					for _, output := range []string{err.Error(), fmt.Sprintf("%v", err), fmt.Sprintf("%+v", err)} {
						for _, secret := range []string{"FAKE_USER", "FAKE_PASS", "FAKE_SIGNATURE", "FAKE_ACCESS_KEY", "FAKE_SECRET_KEY"} {
							require.NotContains(t, output, secret)
						}
					}
					require.ErrorIs(t, err, berrors.ErrStorageInvalidConfig)
					after, marshalErr := backend.Marshal()
					require.NoError(t, marshalErr)
					require.Equal(t, original, after, "invalid configuration must not mutate the backend")
				})
			}
		}
	}
}
