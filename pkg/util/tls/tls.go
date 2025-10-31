// Copyright 2022 PingCAP, Inc.
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

package tls

import (
	"crypto/tls"

	"go.uber.org/atomic"
)

// RequireSecureTransport Process global variables
var RequireSecureTransport = atomic.NewBool(false)

// Taken from https://github.com/openssl/openssl/blob/c784a838e0947fcca761ee62def7d077dc06d37f/include/openssl/ssl.h#L141 .
// Update: remove tlsv1.0 and v1.1 support
var versionString = map[uint16]string{
	tls.VersionTLS12: "TLSv1.2",
	tls.VersionTLS13: "TLSv1.3",
}

// VersionName is like `tls.VersionName()`, but tries to match the names in MySQL/OpenSSL
func VersionName(version uint16) string {
	if tlsVersion, tlsVersionKnown := versionString[version]; tlsVersionKnown {
		return tlsVersion
	}
	return ""
}
