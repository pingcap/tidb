// Copyright 2025 PingCAP, Inc.
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
package tls_test

import (
	"crypto/tls"
	"testing"

	tidbutil "github.com/pingcap/tidb/pkg/util/tls"
)

func RunVersionName(t *testing.T) {
	tests := []struct {
		version uint16
		name    string
	}{
		{tls.VersionSSL30, "SSLv3"},
		{tls.VersionTLS10, "TLS 1.0"},
		{tls.VersionTLS11, "TLS 1.1"},
		{tls.VersionTLS12, "TLSv1.2"},
		{tls.VersionTLS13, "TLSv1.3"},
		{tls.VersionTLS13 + 1, "0x0305"},
	}

	for _, tc := range tests {
		if n := tidbutil.VersionName(tc.version); n != tc.name {
			t.Fatalf("tls.VersionName(%d) expected %s, but got %s", tc.version, tc.name, n)
		}
	}
}
