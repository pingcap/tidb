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

package sessionstates

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

var (
	mockNowOffset = "github.com/pingcap/tidb/pkg/sessionctx/sessionstates/mockNowOffset"
)

func TestSetCertAndKey(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "test1_cert.pem")
	keyPath := filepath.Join(tempDir, "test1_key.pem")
	createRSACert(t, certPath, keyPath)

	// no cert and no key
	_, err := CreateSessionToken("test_user", "%")
	require.ErrorContains(t, err, "no certificate or key file")
	// no cert
	SetKeyPath(keyPath)
	_, err = CreateSessionToken("test_user", "%")
	require.ErrorContains(t, err, "no certificate or key file")
	// no key
	SetKeyPath("")
	SetCertPath(certPath)
	_, err = CreateSessionToken("test_user", "%")
	require.ErrorContains(t, err, "no certificate or key file")
	// both configured
	SetKeyPath(keyPath)
	_, err = CreateSessionToken("test_user", "%")
	require.NoError(t, err)
	// When the key and cert don't match, it will still use the old pair.
	certPath2 := filepath.Join(tempDir, "test2_cert.pem")
	keyPath2 := filepath.Join(tempDir, "test2_key.pem")
	err = util.CreateCertificates(certPath2, keyPath2, 4096, x509.RSA, x509.UnknownSignatureAlgorithm)
	require.NoError(t, err)
	SetKeyPath(keyPath2)
	_, err = CreateSessionToken("test_user", "%")
	require.NoError(t, err)
}

func TestSignAlgo(t *testing.T) {
	tests := []struct {
		pubKeyAlgo x509.PublicKeyAlgorithm
		signAlgos  []x509.SignatureAlgorithm
		keySizes   []int
	}{
		{
			pubKeyAlgo: x509.RSA,
			signAlgos: []x509.SignatureAlgorithm{
				x509.SHA256WithRSA,
				x509.SHA384WithRSA,
				x509.SHA512WithRSA,
				x509.SHA256WithRSAPSS,
				x509.SHA384WithRSAPSS,
				x509.SHA512WithRSAPSS,
			},
			keySizes: []int{
				2048,
				4096,
			},
		},
		{
			pubKeyAlgo: x509.ECDSA,
			signAlgos: []x509.SignatureAlgorithm{
				x509.ECDSAWithSHA256,
				x509.ECDSAWithSHA384,
				x509.ECDSAWithSHA512,
			},
			keySizes: []int{
				4096,
			},
		},
		{
			pubKeyAlgo: x509.Ed25519,
			signAlgos: []x509.SignatureAlgorithm{
				x509.PureEd25519,
			},
			keySizes: []int{
				4096,
			},
		},
	}

	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "test1_cert.pem")
	keyPath := filepath.Join(tempDir, "test1_key.pem")
	SetKeyPath(keyPath)
	SetCertPath(certPath)
	for _, test := range tests {
		for _, signAlgo := range test.signAlgos {
			for _, keySize := range test.keySizes {
				msg := fmt.Sprintf("pubKeyAlgo: %s, signAlgo: %s, keySize: %d", test.pubKeyAlgo.String(),
					signAlgo.String(), keySize)
				err := util.CreateCertificates(certPath, keyPath, keySize, test.pubKeyAlgo, signAlgo)
				require.NoError(t, err, msg)
				ReloadSigningCert()
				token, tokenBytes := createNewToken(t, "test_user")
				err = ValidateSessionToken(tokenBytes, "test_user", "%")
				require.NoError(t, err, msg)

				// All signed fields must be protected, including those beyond the
				// first curve-order bytes that ECDSA takes from its digest input.
				token.Hostname = "localhost"
				tokenBytes, err = json.Marshal(token)
				require.NoError(t, err, msg)
				err = ValidateSessionToken(tokenBytes, "test_user", "localhost")
				require.ErrorContains(t, err, "verification", msg)

				token.Hostname = "%"
				token.ExpireTime = token.ExpireTime.Add(time.Hour)
				tokenBytes, err = json.Marshal(token)
				require.NoError(t, err, msg)
				err = ValidateSessionToken(tokenBytes, "test_user", "%")
				require.ErrorContains(t, err, "verification", msg)
			}
		}
	}
}

func TestVerifyToken(t *testing.T) {
	SetupSigningCertForTest(t)

	// check succeeds
	token, tokenBytes := createNewToken(t, "test_user")
	err := ValidateSessionToken(tokenBytes, "test_user", "%")
	require.NoError(t, err)
	// the token expires
	timeOffset := uint64(tokenLifetime + time.Minute)
	require.NoError(t, failpoint.Enable(mockNowOffset, fmt.Sprintf(`return(%d)`, timeOffset)))
	err = ValidateSessionToken(tokenBytes, "test_user", "%")
	require.NoError(t, failpoint.Disable(mockNowOffset))
	require.ErrorContains(t, err, "token expired")
	// the current user is different with the token
	err = ValidateSessionToken(tokenBytes, "another_user", "%")
	require.ErrorContains(t, err, "username does not match")
	// Case-distinct usernames identify different privilege records.
	err = ValidateSessionToken(tokenBytes, "TEST_USER", "%")
	require.ErrorContains(t, err, "username does not match")
	// The matched host is part of the signed account, not the peer address.
	err = ValidateSessionToken(tokenBytes, "test_user", "localhost")
	require.ErrorContains(t, err, "hostname does not match")
	// Changing a signed host invalidates the signature.
	token.Hostname = "localhost"
	tokenBytes2, err := json.Marshal(token)
	require.NoError(t, err)
	err = ValidateSessionToken(tokenBytes2, "test_user", "localhost")
	require.ErrorContains(t, err, "verification error")
	// Removing a signed host must not turn a bound token into a legacy token.
	token.Hostname = ""
	tokenBytes2, err = json.Marshal(token)
	require.NoError(t, err)
	require.ErrorContains(t, ValidateSessionToken(tokenBytes2, "test_user", "%"), "verification error")
	token.Hostname = "%"
	// forge the user name
	token.Username = "another_user"
	tokenBytes2, err = json.Marshal(token)
	require.NoError(t, err)
	err = ValidateSessionToken(tokenBytes2, "another_user", "%")
	require.ErrorContains(t, err, "verification error")
	// forge the expire time
	token.Username = "test_user"
	token.ExpireTime = time.Now().Add(-time.Minute)
	tokenBytes2, err = json.Marshal(token)
	require.NoError(t, err)
	err = ValidateSessionToken(tokenBytes2, "test_user", "%")
	require.ErrorContains(t, err, "verification error")

	// Older servers sign this format without a hostname field.
	legacyToken := struct {
		Username   string    `json:"username"`
		SignTime   time.Time `json:"sign-time"`
		ExpireTime time.Time `json:"expire-time"`
		Signature  []byte    `json:"signature,omitempty"`
	}{Username: "test_user", SignTime: time.Now(), ExpireTime: time.Now().Add(tokenLifetime)}
	legacyBytes, err := json.Marshal(legacyToken)
	require.NoError(t, err)
	legacyToken.Signature, err = globalSigningCert.sign(legacyBytes)
	require.NoError(t, err)
	legacyBytes, err = json.Marshal(legacyToken)
	require.NoError(t, err)
	t.Run("legacy token", func(t *testing.T) {
		for _, hostname := range []string{"", "%", "localhost"} {
			require.NoError(t, ValidateSessionToken(legacyBytes, "test_user", hostname))
		}
		require.ErrorContains(t, ValidateSessionToken(legacyBytes, "another_user", "%"), "username does not match")
		require.ErrorContains(t, ValidateSessionToken(legacyBytes, "TEST_USER", "%"), "username does not match")
	})

	t.Run("empty hostname", func(t *testing.T) {
		hostlessToken, err := CreateSessionToken("test_user", "")
		require.NoError(t, err)
		hostlessBytes, err := json.Marshal(hostlessToken)
		require.NoError(t, err)
		require.NotContains(t, string(hostlessBytes), `"hostname"`)
		require.NoError(t, ValidateSessionToken(hostlessBytes, "test_user", "%"))
	})
}

func TestLegacyECDSASessionToken(t *testing.T) {
	originalDisabled := vardef.IsLegacyECDSASessionTokenDisabled()
	t.Cleanup(func() { vardef.SetLegacyECDSASessionTokenDisabled(originalDisabled) })
	vardef.SetLegacyECDSASessionTokenDisabled(false)
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "cert.pem")
	keyPath := filepath.Join(tempDir, "key.pem")
	require.NoError(t, util.CreateCertificates(certPath, keyPath, 0, x509.ECDSA, x509.ECDSAWithSHA256))
	SetKeyPath(keyPath)
	SetCertPath(certPath)

	now := time.Now()
	token := &SessionToken{Username: "legacy_user", SignTime: now, ExpireTime: now.Add(tokenLifetime)}
	payload, err := json.Marshal(token)
	require.NoError(t, err)
	// Older TiDB versions pass the JSON directly to ECDSA instead of hashing it.
	key := globalSigningCert.certs[0].privKey.(*ecdsa.PrivateKey)
	token.Signature, err = ecdsa.SignASN1(rand.Reader, key, payload)
	require.NoError(t, err)
	tokenBytes, err := json.Marshal(token)
	require.NoError(t, err)
	require.NoError(t, ValidateSessionToken(tokenBytes, token.Username, "%"))

	// Legacy signatures do not authenticate timestamps beyond the curve-order
	// prefix. This compatibility path must be disabled after the upgrade.
	token.ExpireTime = token.ExpireTime.Add(time.Hour)
	tamperedBytes, err := json.Marshal(token)
	require.NoError(t, err)
	require.NoError(t, ValidateSessionToken(tamperedBytes, token.Username, "%"))
	vardef.SetLegacyECDSASessionTokenDisabled(true)
	require.ErrorContains(t, ValidateSessionToken(tokenBytes, token.Username, "%"), "verification")
	require.ErrorContains(t, ValidateSessionToken(tamperedBytes, token.Username, "%"), "verification")

	// New tokens always hash the complete payload, even during an upgrade.
	for _, disabled := range []bool{false, true} {
		vardef.SetLegacyECDSASessionTokenDisabled(disabled)
		modernToken, modernBytes := createNewToken(t, token.Username)
		require.NoError(t, ValidateSessionToken(modernBytes, token.Username, "%"))
		signature := modernToken.Signature
		modernToken.Signature = nil
		payload, err := json.Marshal(modernToken)
		require.NoError(t, err)
		require.False(t, ecdsa.VerifyASN1(&key.PublicKey, payload, signature))
	}

	// Verification must try older certificates after the newest one fails,
	// for both signature formats while legacy verification is enabled.
	_, modernBytes := createNewToken(t, token.Username)
	newCertPath := filepath.Join(tempDir, "new_cert.pem")
	newKeyPath := filepath.Join(tempDir, "new_key.pem")
	require.NoError(t, util.CreateCertificates(newCertPath, newKeyPath, 0, x509.ECDSA, x509.ECDSAWithSHA256))
	SetKeyPath(newKeyPath)
	SetCertPath(newCertPath)
	for _, disabled := range []bool{false, true} {
		vardef.SetLegacyECDSASessionTokenDisabled(disabled)
		require.NoError(t, ValidateSessionToken(modernBytes, token.Username, "%"))
		if disabled {
			require.ErrorContains(t, ValidateSessionToken(tokenBytes, token.Username, "%"), "verification")
		} else {
			require.NoError(t, ValidateSessionToken(tokenBytes, token.Username, "%"))
		}
	}
}

func TestStarterSessionTokenLifetime(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("starter deploy mode is only available for nextgen TiDB")
	}

	original := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(original))
	})

	require.Equal(t, starterTokenLifetime, currentTokenLifetime())
	require.Equal(t, starterLoadCertInterval, GetLoadCertInterval())
	require.Equal(t, starterOldCertValidTime, currentOldCertValidTime())

	SetupSigningCertForTest(t)
	_, tokenBytes := createNewToken(t, "test_user")

	timeOffset := uint64(tokenLifetime + time.Minute)
	require.NoError(t, failpoint.Enable(mockNowOffset, fmt.Sprintf(`return(%d)`, timeOffset)))
	err := ValidateSessionToken(tokenBytes, "test_user", "%")
	require.NoError(t, failpoint.Disable(mockNowOffset))
	require.NoError(t, err)

	timeOffset = uint64(starterTokenLifetime + time.Minute)
	require.NoError(t, failpoint.Enable(mockNowOffset, fmt.Sprintf(`return(%d)`, timeOffset)))
	err = ValidateSessionToken(tokenBytes, "test_user", "%")
	require.NoError(t, failpoint.Disable(mockNowOffset))
	require.ErrorContains(t, err, "token expired")
}

func TestCertExpire(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "test1_cert.pem")
	keyPath := filepath.Join(tempDir, "test1_key.pem")
	createRSACert(t, certPath, keyPath)
	SetKeyPath(keyPath)
	SetCertPath(certPath)

	_, tokenBytes := createNewToken(t, "test_user")
	err := ValidateSessionToken(tokenBytes, "test_user", "%")
	require.NoError(t, err)
	// replace the cert, but the old cert is still valid for a while
	certPath2 := filepath.Join(tempDir, "test2_cert.pem")
	keyPath2 := filepath.Join(tempDir, "test2_key.pem")
	createRSACert(t, certPath2, keyPath2)
	SetKeyPath(keyPath2)
	SetCertPath(certPath2)
	err = ValidateSessionToken(tokenBytes, "test_user", "%")
	require.NoError(t, err)
	// the old cert expires and the original token is invalid
	timeOffset := uint64(LoadCertInterval)
	require.NoError(t, failpoint.Enable(mockNowOffset, fmt.Sprintf(`return(%d)`, timeOffset)))
	ReloadSigningCert()
	timeOffset += uint64(oldCertValidTime + time.Minute)
	require.NoError(t, failpoint.Enable(mockNowOffset, fmt.Sprintf(`return(%d)`, timeOffset)))
	err = ValidateSessionToken(tokenBytes, "test_user", "%")
	require.ErrorContains(t, err, "verification error")
	// the new cert is not rotated but is reloaded
	_, tokenBytes = createNewToken(t, "test_user")
	ReloadSigningCert()
	err = ValidateSessionToken(tokenBytes, "test_user", "%")
	require.NoError(t, err)
	// the cert is rotated but is still valid
	createRSACert(t, certPath2, keyPath2)
	timeOffset += uint64(LoadCertInterval)
	require.NoError(t, failpoint.Enable(mockNowOffset, fmt.Sprintf(`return(%d)`, timeOffset)))
	ReloadSigningCert()
	err = ValidateSessionToken(tokenBytes, "test_user", "%")
	require.ErrorContains(t, err, "token expired")
	// after some time, it's not valid
	timeOffset += uint64(oldCertValidTime + time.Minute)
	require.NoError(t, failpoint.Enable(mockNowOffset, fmt.Sprintf(`return(%d)`, timeOffset)))
	err = ValidateSessionToken(tokenBytes, "test_user", "%")
	require.NoError(t, failpoint.Disable(mockNowOffset))
	require.ErrorContains(t, err, "verification error")
}

func TestLoadAndReadConcurrently(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "test1_cert.pem")
	keyPath := filepath.Join(tempDir, "test1_key.pem")
	createRSACert(t, certPath, keyPath)
	SetKeyPath(keyPath)
	SetCertPath(certPath)

	deadline := time.Now().Add(5 * time.Second)
	var wg util.WaitGroupWrapper
	// the writer
	wg.Run(func() {
		for time.Now().Before(deadline) {
			createRSACert(t, certPath, keyPath)
			time.Sleep(time.Second)
		}
	})
	// the loader
	for range 2 {
		wg.Run(func() {
			for time.Now().Before(deadline) {
				ReloadSigningCert()
				time.Sleep(500 * time.Millisecond)
			}
		})
	}
	// the reader
	for i := range 3 {
		id := i
		wg.Run(func() {
			username := fmt.Sprintf("test_user_%d", id)
			for time.Now().Before(deadline) {
				_, tokenBytes := createNewToken(t, username)
				time.Sleep(10 * time.Millisecond)
				err := ValidateSessionToken(tokenBytes, username, "%")
				require.NoError(t, err)
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	wg.Wait()
}

func createNewToken(t *testing.T, username string) (*SessionToken, []byte) {
	token, err := CreateSessionToken(username, "%")
	require.NoError(t, err)
	tokenBytes, err := json.Marshal(token)
	require.NoError(t, err)
	return token, tokenBytes
}

func createRSACert(t *testing.T, certPath, keyPath string) {
	err := util.CreateCertificates(certPath, keyPath, 4096, x509.RSA, x509.UnknownSignatureAlgorithm)
	require.NoError(t, err)
}

// SetupSigningCertForTest sets signing cert.
func SetupSigningCertForTest(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "test1_cert.pem")
	keyPath := filepath.Join(tempDir, "test1_key.pem")
	createRSACert(t, certPath, keyPath)
	SetKeyPath(keyPath)
	SetCertPath(certPath)
}
