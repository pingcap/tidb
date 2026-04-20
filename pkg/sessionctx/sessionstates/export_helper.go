// Copyright 2025 PingCAP, Inc.
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

package sessionstates

import (
	"crypto/x509"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/util"
)

// Export helper for mega test framework
// This file exports internal symbols needed by test packages

// SetupSigningCertForTest installs a temporary signing cert for tests and restores the previous state on cleanup.
func SetupSigningCertForTest(t *testing.T) {
	t.Helper()

	globalSigningCert.RLock()
	oldCertPath := globalSigningCert.certPath
	oldKeyPath := globalSigningCert.keyPath
	oldCerts := append([]*certInfo(nil), globalSigningCert.certs...)
	globalSigningCert.RUnlock()

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	if err := util.CreateCertificates(certPath, keyPath, 2048, x509.RSA, x509.SHA256WithRSA); err != nil {
		t.Fatal(err)
	}

	globalSigningCert.Lock()
	globalSigningCert.certPath = certPath
	globalSigningCert.keyPath = keyPath
	globalSigningCert.certs = nil
	globalSigningCert.checkAndLoadCert()
	globalSigningCert.Unlock()

	t.Cleanup(func() {
		globalSigningCert.Lock()
		globalSigningCert.certPath = oldCertPath
		globalSigningCert.keyPath = oldKeyPath
		globalSigningCert.certs = oldCerts
		globalSigningCert.Unlock()
	})
}
