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
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
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

	// Reject correctly signed legacy tokens without a host binding.
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
	require.Error(t, ValidateSessionToken(legacyBytes, "test_user", "%"))

	// Even a newly signed token with an empty host cannot authorize an account.
	hostlessToken, err := CreateSessionToken("test_user", "")
	require.NoError(t, err)
	hostlessBytes, err := json.Marshal(hostlessToken)
	require.NoError(t, err)
	require.ErrorContains(t, ValidateSessionToken(hostlessBytes, "test_user", ""), "hostname does not match")
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

// frozenLegacyToken and its codec deliberately duplicate the pre-V2 wire contract
// (c3b8eb1972^). They must not call the production token/signature helpers.
type frozenLegacyToken struct {
	Username   string    `json:"username"`
	SignTime   time.Time `json:"sign-time"`
	ExpireTime time.Time `json:"expire-time"`
	Signature  []byte    `json:"signature,omitempty"`
}

func frozenLegacySign(t *testing.T, cert *x509.Certificate, key crypto.Signer, username string) []byte {
	t.Helper()
	token := frozenLegacyToken{Username: username, SignTime: time.Now(), ExpireTime: time.Now().Add(time.Minute)}
	content, err := json.Marshal(token)
	require.NoError(t, err)
	var opts crypto.SignerOpts
	switch key.(type) {
	case *rsa.PrivateKey:
		var hash crypto.Hash
		switch cert.SignatureAlgorithm {
		case x509.SHA256WithRSA, x509.SHA256WithRSAPSS:
			hash = crypto.SHA256
		case x509.SHA384WithRSA, x509.SHA384WithRSAPSS:
			hash = crypto.SHA384
		case x509.SHA512WithRSA, x509.SHA512WithRSAPSS:
			hash = crypto.SHA512
		default:
			t.Fatalf("unsupported frozen RSA algorithm: %s", cert.SignatureAlgorithm)
		}
		h := hash.New()
		_, err = h.Write(content)
		require.NoError(t, err)
		content = h.Sum(nil)
		opts = hash
		switch cert.SignatureAlgorithm {
		case x509.SHA256WithRSAPSS, x509.SHA384WithRSAPSS, x509.SHA512WithRSAPSS:
			opts = &rsa.PSSOptions{Hash: hash, SaltLength: rsa.PSSSaltLengthEqualsHash}
		}
	case ed25519.PrivateKey:
		opts = crypto.Hash(0)
	case *ecdsa.PrivateKey:
		// Historical ECDSA signs the raw JSON as a digest, including truncation.
	default:
		t.Fatalf("unsupported frozen signer: %T", key)
	}
	token.Signature, err = key.Sign(rand.Reader, content, opts)
	require.NoError(t, err)
	encoded, err := json.Marshal(token)
	require.NoError(t, err)
	return encoded
}

func frozenLegacyValidate(cert *x509.Certificate, encoded []byte, username string) error {
	var token frozenLegacyToken
	if err := json.Unmarshal(encoded, &token); err != nil {
		return err
	}
	signature := token.Signature
	token.Signature = nil
	content, err := json.Marshal(token)
	if err != nil {
		return err
	}
	if key, ok := cert.PublicKey.(*ecdsa.PublicKey); ok {
		if !ecdsa.VerifyASN1(key, content, signature) {
			return fmt.Errorf("frozen ECDSA verification failure")
		}
	} else if err = cert.CheckSignature(cert.SignatureAlgorithm, content, signature); err != nil {
		return err
	}
	now := time.Now()
	if now.After(token.ExpireTime) || token.SignTime.Add(time.Minute).Before(now) {
		return fmt.Errorf("frozen token expired")
	}
	if !strings.EqualFold(username, token.Username) {
		return fmt.Errorf("frozen username mismatch")
	}
	return nil
}

func installTokenTestSigner(t *testing.T, key crypto.Signer, algorithm x509.SignatureAlgorithm) *x509.Certificate {
	t.Helper()
	cert := &x509.Certificate{PublicKey: key.Public(), SignatureAlgorithm: algorithm}
	globalSigningCert.Lock()
	oldCerts := globalSigningCert.certs
	globalSigningCert.certs = []*certInfo{{cert: cert, privKey: key, expireTime: time.Now().Add(time.Hour)}}
	globalSigningCert.Unlock()
	t.Cleanup(func() {
		globalSigningCert.Lock()
		globalSigningCert.certs = oldCerts
		globalSigningCert.Unlock()
	})
	return cert
}

func TestSessionTokenCompatibility(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	tests := []struct {
		name      string
		key       crypto.Signer
		algorithm x509.SignatureAlgorithm
	}{
		{"rsa256", rsaKey, x509.SHA256WithRSA},
		{"rsa384", rsaKey, x509.SHA384WithRSA},
		{"rsa512", rsaKey, x509.SHA512WithRSA},
		{"pss256", rsaKey, x509.SHA256WithRSAPSS},
		{"pss384", rsaKey, x509.SHA384WithRSAPSS},
		{"pss512", rsaKey, x509.SHA512WithRSAPSS},
		{"ed25519", edKey, x509.PureEd25519},
	}
	for _, curve := range []elliptic.Curve{elliptic.P256(), elliptic.P384(), elliptic.P521()} {
		key, err := ecdsa.GenerateKey(curve, rand.Reader)
		require.NoError(t, err)
		tests = append(tests, struct {
			name      string
			key       crypto.Signer
			algorithm x509.SignatureAlgorithm
		}{curve.Params().Name, key, x509.ECDSAWithSHA256})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cert := installTokenTestSigner(t, test.key, test.algorithm)
			oldToken := frozenLegacySign(t, cert, test.key, "test_user")
			require.NoError(t, frozenLegacyValidate(cert, oldToken, "test_user"))
			require.NoError(t, ValidateLegacySessionToken(oldToken, "test_user"), "old -> bridge")
			require.True(t, IsLegacySessionToken(oldToken))
			require.NoError(t, ValidateLegacySessionToken(oldToken, "TEST_USER"), "legacy preserves login username semantics")
			require.Error(t, ValidateLegacySessionToken(oldToken, "other"))
			require.Error(t, ValidateSessionToken(oldToken, "test_user", "%"), "strict mode rejects old tokens")
			// Reissue after each hop rather than forwarding one fixture repeatedly.
			for range 3 {
				bridgeToken, err := CreateLegacySessionToken("test_user")
				require.NoError(t, err)
				bridgeBytes, err := json.Marshal(bridgeToken)
				require.NoError(t, err)
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(bridgeBytes, &fields))
				require.Len(t, fields, 4)
				require.Contains(t, fields, "username")
				require.Contains(t, fields, "sign-time")
				require.Contains(t, fields, "expire-time")
				require.Contains(t, fields, "signature")
				require.NoError(t, frozenLegacyValidate(cert, bridgeBytes, "test_user"), "bridge -> old")
				require.NoError(t, ValidateLegacySessionToken(bridgeBytes, "test_user"), "bridge -> bridge")
				oldToken = frozenLegacySign(t, cert, test.key, "test_user")
				require.NoError(t, ValidateLegacySessionToken(oldToken, "test_user"), "old reissue -> bridge")
			}
			token, tokenBytes := createNewToken(t, "test_user")
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(tokenBytes, &fields))
			require.JSONEq(t, `2`, string(fields["version"]))
			require.NoError(t, ValidateSessionToken(tokenBytes, "test_user", "%"))
			require.False(t, IsLegacySessionToken(tokenBytes))
			require.Error(t, ValidateLegacySessionToken(tokenBytes, "test_user"))
			// ECDSA must authenticate the whole payload for every supported curve.
			for _, field := range []string{"username", "hostname", "sign-time", "expire-time", "version"} {
				t.Run("tamper_"+field, func(t *testing.T) {
					var modified map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(tokenBytes, &modified))
					username, host := "test_user", "%"
					var replacement any
					switch field {
					case "username":
						replacement, username = "other", "other"
					case "hostname":
						replacement, host = "localhost", "localhost"
					case "sign-time":
						replacement = token.SignTime.Add(time.Second)
					case "expire-time":
						replacement = token.ExpireTime.Add(time.Hour)
					case "version":
						replacement = 3
					}
					modified[field], err = json.Marshal(replacement)
					require.NoError(t, err)
					encoded, err := json.Marshal(modified)
					require.NoError(t, err)
					require.Error(t, ValidateSessionToken(encoded, username, host))
					require.Error(t, ValidateLegacySessionToken(encoded, username), "invalid V2 must never downgrade")
				})
			}
			anonymous, err := CreateSessionToken("", "%")
			require.NoError(t, err)
			anonymousBytes, err := json.Marshal(anonymous)
			require.NoError(t, err)
			require.NoError(t, ValidateSessionToken(anonymousBytes, "", "%"))
		})
	}
}

func TestLegacySessionTokenRejectsV2Markers(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := installTokenTestSigner(t, key, x509.ECDSAWithSHA256)
	oldToken := frozenLegacySign(t, cert, key, "test_user")
	for _, marker := range []string{"hostname", "HOSTNAME", "Hostname", "version", "VERSION", "Version"} {
		for _, value := range []string{`null`, `""`, `0`, `2`, `"bad"`, `{}`, `[]`, `false`} {
			t.Run(marker+"="+value, func(t *testing.T) {
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(oldToken, &fields))
				fields[marker] = json.RawMessage(value)
				encoded, err := json.Marshal(fields)
				require.NoError(t, err)
				// The old verifier ignores added markers. The bridge must not reinterpret
				// malformed V2 payloads as legacy, even when the legacy signature is valid.
				require.NoError(t, frozenLegacyValidate(cert, encoded, "test_user"))
				require.False(t, IsLegacySessionToken(encoded))
				require.Error(t, ValidateLegacySessionToken(encoded, "test_user"))
				require.Error(t, ValidateSessionToken(encoded, "test_user", "%"))
			})
		}
	}
	for _, malformed := range []string{`null`, `[]`, `"token"`, `{`} {
		require.False(t, IsLegacySessionToken([]byte(malformed)))
		require.Error(t, ValidateLegacySessionToken([]byte(malformed), "test_user"))
	}
}
