package sessionstates

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
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

const (
	// A token needs a lifetime to avoid brute force attack.
	tokenLifetime = time.Minute
	// Reload the certificate periodically because it may be replaced.
	certLifetime = 30 * 24 * time.Hour
	// After the new certificate takes effect, the old certificate can still take effect for a while.
	oldCertValidTime = time.Minute
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
	now := time.Now()
	token := &SessionToken{
		Username:   username,
		SignTime:   now,
		ExpireTime: now.Add(tokenLifetime),
	}
	tokenBytes, err := json.Marshal(token)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if token.Signature, err = globalSigningCert.Sign(tokenBytes); err != nil {
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
	if err = globalSigningCert.CheckSignature(tokenBytes, signature); err != nil {
		return ErrCannotMigrateSession.GenWithStackByArgs(err.Error())
	}
	now := time.Now()
	failpoint.Inject("mockValidateTokenTime", func(val failpoint.Value) {
		if s := val.(string); len(s) > 0 {
			if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
				now = t
			}
		}
	})
	if now.After(token.ExpireTime) {
		return ErrCannotMigrateSession.GenWithStackByArgs("token expired")
	}
	if token.SignTime.Add(tokenLifetime).Before(now) {
		return ErrCannotMigrateSession.GenWithStackByArgs("token lifetime is too long")
	}
	if !strings.EqualFold(username, token.Username) {
		return ErrCannotMigrateSession.GenWithStackByArgs("username does not match")
	}
	return nil
}

func SetKeyPath(keyPath string) {
	globalSigningCert.setKeyPath(keyPath)
}

func SetCertPath(certPath string) {
	globalSigningCert.setCertPath(certPath)
}

var globalSigningCert SigningCert

// SigningCert represents the parsed certificate used for token-based auth.
type SigningCert struct {
	sync.RWMutex
	certPath   string
	keyPath    string
	expireTime time.Time
	cert       *x509.Certificate
	privKey    crypto.PrivateKey
	// The cert file may happen to be replaced between signing and checking, so we keep the old cert for a while.
	lastCertExpireTime time.Time
	lastCert           *x509.Certificate
}

// We cannot guarantee that the cert and key are set at the same time because they are set in the system variables.
func (sc *SigningCert) setCertPath(certPath string) {
	sc.Lock()
	sc.certPath = certPath
	// It may fail expectedly because the key path is not set yet.
	_ = sc.checkAndLoadCert(true)
	sc.Unlock()
}

func (sc *SigningCert) setKeyPath(keyPath string) {
	sc.Lock()
	sc.keyPath = keyPath
	// It may fail expectedly because the cert path is not set yet.
	_ = sc.checkAndLoadCert(true)
	sc.Unlock()
}

func (sc *SigningCert) checkAndLoadCert(force bool) error {
	if len(sc.certPath) == 0 || len(sc.keyPath) == 0 {
		return nil
	}
	now := time.Now()
	if sc.lastCert != nil && now.After(sc.lastCertExpireTime) {
		sc.lastCert = nil
	}
	if force || now.After(sc.expireTime) {
		if err := sc.loadCert(); err != nil {
			logutil.BgLogger().Warn("load cert failed", zap.Error(err))
			return err
		}
	}
	return nil
}

func (sc *SigningCert) loadCert() error {
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

	if sc.cert != nil {
		sc.lastCert = sc.cert
		sc.lastCertExpireTime = sc.expireTime.Add(oldCertValidTime)
		sc.expireTime = sc.expireTime.Add(certLifetime)
	} else {
		// If the cert has never been loaded before.
		sc.expireTime = time.Now().Add(certLifetime)
	}
	sc.cert = cert
	sc.privKey = tlsCert.PrivateKey
	return nil
}

// Sign generates a signature with the content and the private key.
func (sc *SigningCert) Sign(content []byte) ([]byte, error) {
	sc.Lock()
	err := sc.checkAndLoadCert(false)
	sc.Unlock()
	if err != nil {
		return nil, err
	}

	var (
		signer crypto.Signer
		opts   crypto.SignerOpts
	)
	sc.RLock()
	defer sc.RUnlock()
	if sc.cert == nil {
		return nil, errors.New("no certificate or key file to sign the data")
	}
	switch sc.privKey.(type) {
	case ed25519.PrivateKey:
		signer = sc.privKey.(ed25519.PrivateKey)
	case *rsa.PrivateKey:
		signer = sc.privKey.(*rsa.PrivateKey)
		var pssHash crypto.Hash
		switch sc.cert.SignatureAlgorithm {
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
		} else {
			hashed := sha256.Sum256(content)
			content = hashed[:]
			opts = crypto.SHA256
		}
	case *ecdsa.PrivateKey:
		signer = sc.privKey.(*ecdsa.PrivateKey)
	default:
		return nil, errors.Errorf("not supported private key type '%s' for signing", sc.cert.SignatureAlgorithm.String())
	}
	return signer.Sign(rand.Reader, content, opts)
}

// CheckSignature checks the signature and the content.
func (sc *SigningCert) CheckSignature(content, signature []byte) error {
	sc.Lock()
	err := sc.checkAndLoadCert(false)
	sc.Unlock()
	if err != nil {
		return err
	}

	sc.RLock()
	defer sc.RUnlock()
	if sc.cert == nil {
		return errors.New("no certificate or key file to check the signature")
	}
	err = sc.cert.CheckSignature(sc.cert.SignatureAlgorithm, content, signature)
	// The content might be signed with the older certificate, so we also try the old one.
	if err != nil && sc.lastCert != nil && time.Now().Before(sc.lastCertExpireTime) {
		return sc.cert.CheckSignature(sc.cert.SignatureAlgorithm, content, signature)
	}
	return err
}
