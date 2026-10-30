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

package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRedactDiagnosticErrorText(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
		want string
	}{
		{
			name: "plain error",
			text: "Memory usage too high, usage: 1024 bytes, mem limit 2048 bytes",
			want: "Memory usage too high, usage: 1024 bytes, mem limit 2048 bytes",
		},
		{
			name: "s3 credentials",
			text: "download failed: s3://AKID:USERPASS@bucket/file?access-key=AKID&secret-access-key=SKEY&session-token=TOKEN",
			want: "download failed: s3://bucket/file",
		},
		{
			name: "azure sas",
			text: "azure://bucket/file?sas-token=SAS",
			want: "azure://bucket/file",
		},
		{
			name: "signed https endpoint",
			text: "https://account.blob.core.windows.net/file?sig=SAS&sv=2022",
			want: "https://account.blob.core.windows.net/file",
		},
		{
			name: "malformed URL",
			text: "download failed: s3://bucket:port/file?secret-access-key=SKEY",
			want: "download failed: " + objstore.InvalidURLPlaceholder,
		},
		{
			name: "quoted URL with spaces",
			text: `parse "s3://bucket/file?secret-access-key=secret with spaces": invalid format`,
			want: `parse "(storage URL and following text redacted)`,
		},
		{
			name: "unquoted userinfo with spaces",
			text: "download failed: s3://user:secret with spaces@bucket/file",
			want: "download failed: (storage URL and following text redacted)",
		},
		{
			name: "multiple URLs",
			text: "copy s3://bucket/file?secret-access-key=SKEY to azure://bucket/file?sas-token=SAS",
			want: "copy (storage URL and following text redacted)",
		},
		{
			name: "nested URL",
			text: "s3://bucket/file?endpoint=https://AKID:SKEY@endpoint",
			want: "(storage URL and following text redacted)",
		},
		{
			name: "percent encoded endpoint",
			text: "s3://bucket/file?endpoint=https%3A%2F%2FAKID%3ASKEY%40endpoint",
			want: "s3://bucket/file",
		},
		{
			name: "repeatedly percent encoded endpoint",
			text: "s3://bucket/file?endpoint=https%253A%252F%252FAKID%253ASKEY%2540endpoint",
			want: "s3://bucket/file",
		},
		{
			name: "percent encoded userinfo",
			text: "s3://%41%4B%49%44:%53%4B%45%59@bucket/file",
			want: "s3://bucket/file",
		},
		{
			name: "percent encoded userinfo delimiters",
			text: "s3://AKID:1234%2F5678%3F9%23@bucket/file",
			want: "s3://bucket/file",
		},
		{
			name: "truncated userinfo",
			text: "s3://AKID...",
			want: "(storage URL and following text redacted)",
		},
		{
			name: "userinfo with unescaped slash after numeric prefix",
			text: "download failed: s3://AKID:1234/5678@bucket/file",
			want: "download failed: (storage URL and following text redacted)",
		},
		{
			name: "userinfo with unescaped slash after empty prefix",
			text: "s3://AKID:/5678@bucket/file",
			want: "(storage URL and following text redacted)",
		},
		{
			name: "userinfo with unescaped question mark",
			text: "s3://AKID:1234?5678@bucket/file",
			want: "(storage URL and following text redacted)",
		},
		{
			name: "userinfo with unescaped fragment",
			text: "s3://AKID:1234#5678@bucket/file",
			want: "(storage URL and following text redacted)",
		},
		{
			name: "https userinfo with unescaped question mark",
			text: "https://AKID:1234?5678@bucket/file",
			want: "(storage URL and following text redacted)",
		},
		{
			name: "credential fragment",
			text: "https://host/path#access_token=SKEY",
			want: "https://host/path",
		},
		{
			name: "query and fragment",
			text: "s3://bucket/file?access-key=AKID#secret-access-key=SKEY",
			want: "s3://bucket/file",
		},
		{
			name: "URL containing newline",
			text: "s3://user:secret\nwith spaces@bucket/file",
			want: "(storage URL and following text redacted)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, redactDiagnosticErrorText(test.text))
		})
	}
}

func TestFailureSummaryRedactsStorageCredentials(t *testing.T) {
	longSecret := "LONG_SECRET_" + strings.Repeat("s", 5000)
	for _, rawURL := range []string{
		"s3://AKID:USERPASS@bucket/file?access-key=AKID&secret-access-key=SKEY&session-token=TOKEN",
		"azure://bucket/file?sas-token=SAS",
		"https://account.blob.core.windows.net/file?sig=SAS",
		"s3://bucket:port/file?secret-access-key=SKEY",
		`"s3://bucket/file?secret-access-key=SKEY with spaces": invalid format`,
		"s3://bucket/file?secret-access-key=SKEY then azure://bucket/file?sas-token=SAS",
		"s3://bucket/file?endpoint=https%3A%2F%2FAKID%3ASKEY%40endpoint",
		"s3://bucket/file?endpoint=https%253A%252F%252FAKID%253ASKEY%2540endpoint",
		"s3://%41%4B%49%44:%53%4B%45%59@bucket/file",
		"s3://AKID:1234/5678@bucket/file",
		"s3://AKID:/5678@bucket/file",
		"s3://AKID:1234?5678@bucket/file",
		"s3://AKID:1234#5678@bucket/file",
		"https://AKID:1234?5678@bucket/file",
		"s3://AKID" + strings.Repeat("s", 5000) + "@bucket/file",
		"https://host/path#access_token=SKEY",
		"s3://bucket/file?access-key=AKID#secret-access-key=SKEY",
		"s3://AKID:SKEY\nTOKEN@bucket/file",
		"s3://bucket/file?secret-access-key=" + longSecret,
	} {
		summary.InitCollector(false)
		t.Cleanup(func() { summary.InitCollector(false) })
		err := fmt.Errorf("%w: %s", berrors.ErrKVDownloadFailed, rawURL)
		original := err.Error()
		summary.ObserveError("tikv", "download_sst", 7, err)
		encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		buffer, encodeErr := encoder.EncodeEntry(zapcore.Entry{}, summary.FailureFields(err, redactDiagnosticErrorText))
		require.NoError(t, encodeErr)
		encoded := buffer.String()
		buffer.Free()
		for _, secret := range []string{"AKID", "USERPASS", "SKEY", "TOKEN", "SAS", "LONG_SECRET_", "%41%4B%49%44", "%53%4B%45%59", "5678"} {
			require.NotContains(t, encoded, secret)
		}
		var report map[string]any
		require.NoError(t, json.Unmarshal([]byte(encoded), &report))
		require.Equal(t, "BR:KV:ErrKVDownloadFailed", report["error-code"])
		require.NotContains(t, report, "errorVerbose")
		observations := report["observed-errors"].([]any)
		require.Len(t, observations, 1)
		observation := observations[0].(map[string]any)
		require.Equal(t, "BR:KV:ErrKVDownloadFailed", observation["error-code"])
		require.Equal(t, float64(7), observation["store-id"])
		require.Equal(t, original, err.Error())
		require.ErrorIs(t, err, berrors.ErrKVDownloadFailed)
	}
}
