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
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Token-based authentication is used in session migration. We don't use typical authentication because the proxy
// cannot store the user passwords for security issues.
//
// The process of token-based authentication:
// 1. Before migrating the session, the proxy requires a token from server A.
// 2. Server A generates a token and signs it with a private key defined in the certificate.
// 3. The proxy authenticates with server B and sends the signed token as the password.
// 4. Server B checks the signature with the public key defined in the certificate and then verifies the token.
//
// The highlight is that the certificates on all the servers should be the same all the time.
// However, the certificates should be rotated periodically. Just in case of using different certificates to
// sign and check, a server should keep the old certificate for a while. A server will try both
// the 2 certificates to check the signature.
const (
	// A token needs a lifetime to avoid brute force attack.
	tokenLifetime = time.Minute
	// LoadCertInterval is the interval of reloading the certificate. The certificate should be rotated periodically.
	LoadCertInterval = 10 * time.Minute
	// After a certificate is replaced, it's still valid for oldCertValidTime.
	// oldCertValidTime must be a little longer than LoadCertInterval, because the previous server may
	// sign with the old cert but the new server checks with the new cert.
	// - server A loads the old cert at 00:00:00.
	// - the cert is rotated at 00:00:01 on all servers.
	// - server B loads the new cert at 00:00:02.
	// - server A signs token with the old cert at 00:10:00.
	// - server B reloads the same new cert again at 00:10:01, and it has 3 certs now.
	// - server B receives the token at 00:10:02, so the old cert should be valid for more than 10m after replacement.
	oldCertValidTime = 15 * time.Minute
)

// SessionToken represents the token used to authenticate with the new server.
type SessionToken struct {
	Username   string    `json:"username"`
	SignTime   time.Time `json:"sign-time"`
	ExpireTime time.Time `json:"expire-time"`
	Signature  []byte    `json:"signature,omitempty"`
}

// CreateSessionToken creates a token for the proxy.
func CreateSessionToken(username string) (*SessionToken, error) {
	now := getNow()
	token := &SessionToken{
		Username:   username,
		SignTime:   now,
		ExpireTime: now.Add(tokenLifetime),
	}
	tokenBytes, err := json.Marshal(token)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if token.Signature, err = globalSigningCert.sign(tokenBytes); err != nil {
		return nil, ErrCannotMigrateSession.GenWithStackByArgs(err.Error())
	}
	return token, nil
}

// ValidateSessionToken validates the token sent from the proxy.
func ValidateSessionToken(tokenBytes []byte, username string) (err error) {
	var token SessionToken
	if err = json.Unmarshal(tokenBytes, &token); err != nil {
		return errors.Trace(err)
	}
	signature := token.Signature
	// Clear the signature and marshal it again to get the original content.
	token.Signature = nil
	if tokenBytes, err = json.Marshal(token); err != nil {
		return errors.Trace(err)
	}
	if err = globalSigningCert.checkSignature(tokenBytes, signature); err != nil {
		return ErrCannotMigrateSession.GenWithStackByArgs(err.Error())
	}
	now := getNow()
	if now.After(token.ExpireTime) {
		return ErrCannotMigrateSession.GenWithStackByArgs("token expired", token.ExpireTime.String())
	}
	// An attacker may forge a very long lifetime to brute force, so we also need to check `SignTime`.
	// However, we need to be tolerant of these problems:
	// - The `tokenLifetime` may change between TiDB versions, so we can't check `token.SignTime.Add(tokenLifetime).Equal(token.ExpireTime)`
	// - There may exist time bias between TiDB instances, so we can't check `now.After(token.SignTime)`
	if token.SignTime.Add(tokenLifetime).Before(now) {
		return ErrCannotMigrateSession.GenWithStackByArgs("token lifetime is too long", token.SignTime.String())
	}
	if !strings.EqualFold(username, token.Username) {
		return ErrCannotMigrateSession.GenWithStackByArgs("username does not match", username, token.Username)
	}
	return nil
}

// SetKeyPath sets the path of key.pem and force load the certificate again.
func SetKeyPath(keyPath string) {
	globalSigningCert.setKeyPath(keyPath)
}

// SetCertPath sets the path of key.pem and force load the certificate again.
func SetCertPath(certPath string) {
	globalSigningCert.setCertPath(certPath)
}

// ReloadSigningCert is used to load the certificate periodically in a separate goroutine.
// It's impossible to know when the old certificate should expire without this goroutine:
// - If the certificate is rotated a minute ago, the old certificate should be still valid for a while.
// - If the certificate is rotated a month ago, the old certificate should expire for safety.
func ReloadSigningCert() {
	globalSigningCert.lockAndLoad()
}

var globalSigningCert signingCert

// signingCert represents the parsed certificate used for token-based auth.
type signingCert struct {
	sync.RWMutex
	certPath string
	keyPath  string
	// The cert file may happen to be rotated between signing and checking, so we keep the old cert for a while.
	// certs contain all the certificates that are not expired yet.
	certs []*certInfo
}

type certInfo struct {
	cert       *x509.Certificate
	privKey    crypto.PrivateKey
	expireTime time.Time
}

func (sc *signingCert) setCertPath(certPath string) {
	sc.Lock()
	if certPath != sc.certPath {
		sc.certPath = certPath
		// It may fail expectedly because the key path is not set yet.
		sc.checkAndLoadCert()
	}
	sc.Unlock()
}

func (sc *signingCert) setKeyPath(keyPath string) {
	sc.Lock()
	if keyPath != sc.keyPath {
		sc.keyPath = keyPath
		// It may fail expectedly because the cert path is not set yet.
		sc.checkAndLoadCert()
	}
	sc.Unlock()
}

func (sc *signingCert) lockAndLoad() {
	sc.Lock()
	sc.checkAndLoadCert()
	sc.Unlock()
}

func (sc *signingCert) checkAndLoadCert() {
	if len(sc.certPath) == 0 || len(sc.keyPath) == 0 {
		return
	}
	if err := sc.loadCert(); err != nil {
		logutil.BgLogger().Warn("loading signing cert failed",
			zap.String("cert path", sc.certPath),
			zap.String("key path", sc.keyPath),
			zap.Error(err))
	} else {
		logutil.BgLogger().Info("signing cert is loaded successfully",
			zap.String("cert path", sc.certPath),
			zap.String("key path", sc.keyPath))
	}
}

// loadCert loads the cert and adds it into the cert list.
func (sc *signingCert) loadCert() error {
	tlsCert, err := tls.LoadX509KeyPair(sc.certPath, sc.keyPath)
	if err != nil {
		return errors.Wrapf(err, "load x509 failed, cert path: %s, key path: %s", sc.certPath, sc.keyPath)
	}
	var cert *x509.Certificate
	if tlsCert.Leaf != nil {
		cert = tlsCert.Leaf
	} else {
		if cert, err = x509.ParseCertificate(tlsCert.Certificate[0]); err != nil {
			return errors.Wrapf(err, "parse x509 cert failed, cert path: %s, key path: %s", sc.certPath, sc.keyPath)
		}
	}

	// Rotate certs. Ensure that the expireTime of certs is in descending order.
	now := getNow()
	newCerts := make([]*certInfo, 0, len(sc.certs)+1)
	newCerts = append(newCerts, &certInfo{
		cert:       cert,
		privKey:    tlsCert.PrivateKey,
		expireTime: now.Add(LoadCertInterval + oldCertValidTime),
	})
	for i := 0; i < len(sc.certs); i++ {
		// Discard the certs that are already expired.
		if now.After(sc.certs[i].expireTime) {
			break
		}
		newCerts = append(newCerts, sc.certs[i])
	}
	sc.certs = newCerts
	return nil
}

// sign generates a signature with the content and the private key.
func (sc *signingCert) sign(content []byte) ([]byte, error) {
	var (
		signer crypto.Signer
		opts   crypto.SignerOpts
	)
	sc.RLock()
	defer sc.RUnlock()
	if len(sc.certs) == 0 {
		return nil, errors.New("no certificate or key file to sign the data")
	}
	// Always sign the token with the latest cert.
	certInfo := sc.certs[0]
	switch key := certInfo.privKey.(type) {
	case ed25519.PrivateKey:
		signer = key
		opts = crypto.Hash(0)
	case *rsa.PrivateKey:
		signer = key
		var pssHash crypto.Hash
		switch certInfo.cert.SignatureAlgorithm {
		case x509.SHA256WithRSAPSS:
			pssHash = crypto.SHA256
		case x509.SHA384WithRSAPSS:
			pssHash = crypto.SHA384
		case x509.SHA512WithRSAPSS:
			pssHash = crypto.SHA512
		}
		if pssHash != 0 {
			h := pssHash.New()
			h.Write(content)
			content = h.Sum(nil)
			opts = &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: pssHash}
			break
		}
		switch certInfo.cert.SignatureAlgorithm {
		case x509.SHA256WithRSA:
			hashed := sha256.Sum256(content)
			content = hashed[:]
			opts = crypto.SHA256
		case x509.SHA384WithRSA:
			hashed := sha512.Sum384(content)
			content = hashed[:]
			opts = crypto.SHA384
		case x509.SHA512WithRSA:
			hashed := sha512.Sum512(content)
			content = hashed[:]
			opts = crypto.SHA512
		default:
			return nil, errors.Errorf("not supported private key type '%s' for signing", certInfo.cert.SignatureAlgorithm.String())
		}
	case *ecdsa.PrivateKey:
		signer = key
	default:
		return nil, errors.Errorf("not supported private key type '%s' for signing", certInfo.cert.SignatureAlgorithm.String())
	}
	return signer.Sign(rand.Reader, content, opts)
}

// checkSignature checks the signature and the content.
func (sc *signingCert) checkSignature(content, signature []byte) error {
	sc.RLock()
	defer sc.RUnlock()
	now := getNow()
	var err error
	for _, certInfo := range sc.certs {
		// The expireTime is in descending order. So if the first one is expired, we skip the following.
		if now.After(certInfo.expireTime) {
			break
		}
		switch certInfo.privKey.(type) {
		// ESDSA is special: `PrivateKey.Sign` doesn't match with `Certificate.CheckSignature`.
		case *ecdsa.PrivateKey:
			if !ecdsa.VerifyASN1(certInfo.cert.PublicKey.(*ecdsa.PublicKey), content, signature) {
				err = errors.New("x509: ECDSA verification failure")
			}
		default:
			err = certInfo.cert.CheckSignature(certInfo.cert.SignatureAlgorithm, content, signature)
		}
		if err == nil {
			return nil
		}
	}
	// no certs (possible) or all certs are expired (impossible)
	if err == nil {
		return errors.Errorf("no valid certificate to check the signature, cached certificates: %d", len(sc.certs))
	}
	return err
}

func getNow() time.Time {
	now := time.Now()
	failpoint.Inject("mockNowOffset", func(val failpoint.Value) {
		if s := uint64(val.(int)); s != 0 {
			now = now.Add(time.Duration(s))
		}
	})
	return now
}
