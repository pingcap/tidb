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

package storage

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestExtractQueryParametersRejectsMalformedQuery(t *testing.T) {
	for _, query := range []string{
		"region=changed&access-key=FAKE_QUERY_AK;FAKE_QUERY_SK",
		"region=changed&access-key=FAKE_QUERY_AK&secret-access-key=FAKE_QUERY_SK%zz",
	} {
		t.Run(query, func(t *testing.T) {
			u, err := url.Parse("s3://bucket/prefix?" + query)
			require.NoError(t, err)
			opts := S3BackendOptions{Region: "original", AccessKey: "original"}
			original := opts

			err = ExtractQueryParameters(u, &opts)

			require.ErrorContains(t, err, "parse storage URL query failed: invalid format")
			require.NotContains(t, fmt.Sprintf("%+v", err), "FAKE_QUERY")
			require.Equal(t, original, opts, "invalid queries must not partially update options")
			require.Equal(t, query, u.RawQuery)
		})
	}
}

func TestBackendOptionsRedacted(t *testing.T) {
	opts := BackendOptions{
		S3: S3BackendOptions{
			AccessKey: "FAKE_AK", SecretAccessKey: "FAKE_SK", SessionToken: "FAKE_STS",
			Region: "us-east-1", Endpoint: "https://FAKE_USER:FAKE_PASS@s3.example.com/?sig=FAKE_SIGNATURE#FAKE_FRAGMENT",
		},
		GCS: GCSBackendOptions{Endpoint: "https://gcs.example.com/?sig=FAKE_SIGNATURE"},
		Azblob: AzblobBackendOptions{
			AccountName: "account", AccountKey: "FAKE_AZKEY", SASToken: "FAKE_SAS", EncryptionKey: "FAKE_ENCRYPTION_KEY",
			Endpoint: "https://azure.example.com/?sig=FAKE_SIGNATURE",
		},
	}
	original := opts

	redacted := opts.Redacted()

	require.Equal(t, ast.RedactedValue, redacted.S3.AccessKey)
	require.Equal(t, ast.RedactedValue, redacted.S3.SecretAccessKey)
	require.Equal(t, ast.RedactedValue, redacted.S3.SessionToken)
	require.Equal(t, ast.RedactedValue, redacted.Azblob.AccountKey)
	require.Equal(t, ast.RedactedValue, redacted.Azblob.SASToken)
	require.Equal(t, ast.RedactedValue, redacted.Azblob.EncryptionKey)
	require.Equal(t, "us-east-1", redacted.S3.Region)
	require.Equal(t, "account", redacted.Azblob.AccountName)
	require.Equal(t, "https://s3.example.com/", redacted.S3.Endpoint)
	require.Equal(t, "https://gcs.example.com/", redacted.GCS.Endpoint)
	require.Equal(t, "https://azure.example.com/", redacted.Azblob.Endpoint)
	require.Equal(t, original, opts)
	require.Equal(t, BackendOptions{}, BackendOptions{}.Redacted())

	opts.S3.Endpoint = "https://FAKE_USER:FAKE_PASS@host/path%zz?sig=FAKE_SIGNATURE"
	require.Equal(t, InvalidURLPlaceholder, opts.Redacted().S3.Endpoint)

	for _, tc := range []struct {
		endpoint string
		expected string
	}{
		{"", ""},
		{"https://host:9000/path", "https://host:9000/path"},
		{"http://FAKE_USER:FAKE_PASS@host:9000/path?sig=FAKE_TOKEN#FAKE_FRAGMENT", "http://host:9000/path"},
		{"FAKE_USER:FAKE_PASS@host:9000", InvalidURLPlaceholder},
		{"//FAKE_USER:FAKE_PASS@host:9000", InvalidURLPlaceholder},
		{"https:FAKE_USER:FAKE_PASS@host:9000", InvalidURLPlaceholder},
		{"https:///FAKE_USER:FAKE_PASS@host:9000", InvalidURLPlaceholder},
		{"https://FAKE_USER:FAKE_PASS@host:port", InvalidURLPlaceholder},
		{"https://FAKE_USER:FAKE_PASS@host%zz", InvalidURLPlaceholder},
		{"/local/path#FAKE_FRAGMENT", InvalidURLPlaceholder},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			opts := BackendOptions{}
			opts.S3.Endpoint = tc.endpoint
			opts.GCS.Endpoint = tc.endpoint
			opts.Azblob.Endpoint = tc.endpoint
			original := opts
			redacted := opts.Redacted()
			require.Equal(t, tc.expected, redacted.S3.Endpoint)
			require.Equal(t, tc.expected, redacted.GCS.Endpoint)
			require.Equal(t, tc.expected, redacted.Azblob.Endpoint)
			require.Equal(t, original, opts)
		})
	}
}

func TestParseBackendRejectsMalformedQuery(t *testing.T) {
	for _, scheme := range []string{"s3", "ks3", "gs", "gcs", "azure", "azblob"} {
		for _, query := range []string{"session-token=FAKE_QUERY_TOKEN;FAKE_QUERY_SECRET", "access-key=FAKE_QUERY_AK&secret-access-key=FAKE_QUERY_SK%zz"} {
			t.Run(scheme+"/"+query, func(t *testing.T) {
				_, err := ParseBackend(scheme+"://bucket/prefix?"+query, nil)
				require.ErrorContains(t, err, "parse storage URL query failed: invalid format")
				require.NotContains(t, fmt.Sprintf("%+v", err), "FAKE_QUERY")
			})
		}
	}
}

func TestStorageErrorsDoNotLeakCredentials(t *testing.T) {
	for _, rawURL := range []string{
		"s3://FAKE_USER:FAKE_PASS@bucket/path%zz?access-key=FAKE_AK&secret-access-key=FAKE_SK",
		"s3://bucket:port/path?access-key=FAKE_AK&secret-access-key=FAKE_SK",
		"s3:///path?access-key=FAKE_AK&secret-access-key=FAKE_SK&unknown-token=FAKE_TOKEN",
		"gcs:///path?credentials=FAKE_GCS",
		"azure:///path?sig=FAKE_SAS",
		"s3://FAKE_AK:FAKE_SK@/prefix",
		"gcs://FAKE_AK:FAKE_SK@/prefix",
		"azblob://FAKE_AK:FAKE_SK@/prefix",
		"s3://bucket/prefix?endpoint=" + url.QueryEscape("https://FAKE_USER:FAKE_PASS@host/path%zz?sig=FAKE_TOKEN"),
	} {
		t.Run(rawURL, func(t *testing.T) {
			_, err := ParseBackend(rawURL, nil)
			require.Error(t, err)
			for _, output := range []string{err.Error(), fmt.Sprintf("%+v", err)} {
				for _, secret := range []string{"FAKE_USER", "FAKE_PASS", "FAKE_AK", "FAKE_SK", "FAKE_TOKEN", "FAKE_GCS", "FAKE_SAS"} {
					require.NotContains(t, output, secret)
				}
			}
		})
	}
	for _, parse := range []func(string) error{
		func(raw string) error { _, err := ParseRawURL(raw); return err },
		func(raw string) error { _, err := IsLocalPath(raw); return err },
	} {
		err := parse("s3://FAKE_USER:FAKE_PASS@bucket/path%zz?access-key=FAKE_AK&secret-access-key=FAKE_SK")
		require.Error(t, err)
		for _, secret := range []string{"FAKE_USER", "FAKE_PASS", "FAKE_AK", "FAKE_SK"} {
			require.NotContains(t, fmt.Sprintf("%+v", err), secret)
		}
	}
}

func TestRedactURL(t *testing.T) {
	for _, tc := range []struct{ raw, expected string }{
		{"/local/path", "/local/path"},
		{"/local/path#name", "/local/path#name"},
		{"/data/my files/100%off", "/data/my files/100%off"},
		{"/local/path?token=SECRET#FRAGMENT", "/local/path"},
		{"file:///local/path?token=SECRET#FRAGMENT", "file:///local/path"},
		{"local://USER:PASS@/local/path?token=SECRET#FRAGMENT", "local:///local/path"},
		{"hdfs://USER:PASS@host/path?token=SECRET#FRAGMENT", "hdfs://host/path"},
		{"noop:///?token=SECRET#FRAGMENT", "noop:///"},
		{"gcs://USER:PASS@bucket/path?credentials=SECRET#FRAGMENT", "gcs://bucket/path"},
		{"gcs://bucket/path?credentials-file=SECRET&storage-class=STANDARD", "gcs://bucket/path?credentials-file=xxxxxx&storage-class=STANDARD"},
		{"gcs://bucket/path?credentials=SECRET%zz", "gcs://bucket/path"},
		{"https://host/path?", "https://host/path"},
		{"s3://bucket/prefix", "s3://bucket/prefix"},
		{"s3://bucket/prefix?access-key=AKID&secret-access-key=SKEY&region=us", "s3://bucket/prefix?access-key=xxxxxx&region=us&secret-access-key=xxxxxx"},
		{"s3://bucket/prefix?endpoint=https%3A%2F%2FUSER%3APASS%40host%2F%3Fsig%3DSECRET", "s3://bucket/prefix?endpoint=xxxxxx"},
		{`s3://bucket/prefix?sse-kms-context={"tenant":"a&region=SECRET"}`, "s3://bucket/prefix"},
		{"ks3://bucket/prefix?access-key=AKID&region=us", "ks3://bucket/prefix?access-key=xxxxxx&region=us"},
		{"azure://c/p?account-name=a&sas-token=sv=2020-08-04&ss=b&sig=SECRET", "azure://c/p?account-name=a&sas-token=xxxxxx"},
		{"s3://bucket:port/prefix?access-key=AKID&secret-access-key=SKEY", InvalidURLPlaceholder},
		{"s3://bucket%zz/prefix?access-key=AKID&secret-access-key=SKEY", InvalidURLPlaceholder},
		{"s3a://AKID:SKEY@bucket/prefix?token=SECRET", "s3a://bucket/prefix"},
		{"https://account.blob.core.windows.net/?sv=2022&sig=SECRET", "https://account.blob.core.windows.net/"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			require.Equal(t, tc.expected, RedactURL(tc.raw))
			require.Equal(t, ast.RedactURL(tc.raw), RedactURL(tc.raw))
		})
	}
}
