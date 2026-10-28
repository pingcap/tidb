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
	berrors "github.com/pingcap/tidb/br/pkg/errors"
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
	for _, scheme := range []string{"azure", "gcs"} {
		for _, endpoint := range []string{
			"https://FAKE_USER:FAKE_PASS@host/path%zz?sig=FAKE_SIGNATURE",
			"https://FAKE_USER:FAKE_PASS@host:badport/path?sig=FAKE_SIGNATURE",
			"https://FAKE_USER:FAKE_PASS@host/path\n?sig=FAKE_SIGNATURE",
		} {
			t.Run(scheme+"/"+url.QueryEscape(endpoint), func(t *testing.T) {
				// Given a valid storage URL with an escaped, malformed endpoint.
				backend, err := ParseBackend(scheme+"://bucket/prefix?account-name=fake&sas-token=FAKE_SAS&endpoint="+url.QueryEscape(endpoint), nil)
				require.NoError(t, err)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				// When constructing the real SDK-backed storage.
				_, err = New(ctx, backend, &storeapi.Options{
					NoCredentials: true,
					HTTPClient:    &http.Client{Transport: rejectEndpointTransport{t: t}},
				})

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
