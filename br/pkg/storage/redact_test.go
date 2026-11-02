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
