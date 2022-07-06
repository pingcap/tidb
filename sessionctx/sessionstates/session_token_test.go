package sessionstates

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestSetCertAndKey(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "test1_cert.pem")
	keyPath := filepath.Join(tempDir, "test1_key.pem")
	err := util.CreateTLSCertificates(certPath, keyPath, 4096)
	require.NoError(t, err)

	// no cert and no key
	_, err = CreateSessionToken("test_user")
	require.ErrorContains(t, err, "no certificate or key file")
	// no cert
	SetKeyPath(keyPath)
	_, err = CreateSessionToken("test_user")
	require.ErrorContains(t, err, "no certificate or key file")
	// no key
	SetKeyPath("")
	SetCertPath(certPath)
	_, err = CreateSessionToken("test_user")
	require.ErrorContains(t, err, "no certificate or key file")
	// both configured
	SetKeyPath(keyPath)
	_, err = CreateSessionToken("test_user")
	require.NoError(t, err)
	// When the key and cert don't match, it will still use the old pair.
	certPath2 := filepath.Join(tempDir, "test2_cert.pem")
	keyPath2 := filepath.Join(tempDir, "test2_key.pem")
	err = util.CreateTLSCertificates(certPath2, keyPath2, 4096)
	require.NoError(t, err)
	SetKeyPath(keyPath2)
	_, err = CreateSessionToken("test_user")
	require.NoError(t, err)
}

func TestSignAndCheck(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "test1_cert.pem")
	keyPath := filepath.Join(tempDir, "test1_key.pem")
	err := util.CreateTLSCertificates(certPath, keyPath, 4096)
	require.NoError(t, err)
	SetKeyPath(keyPath)
	SetCertPath(certPath)

	// check succeeds
	token, err := CreateSessionToken("test_user")
	require.NoError(t, err)
	tokenBytes, err := json.Marshal(token)
	require.NoError(t, err)
	err = ValidateSessionToken(tokenBytes, "test_user")
	require.NoError(t, err)
	// the token expires
	tm := time.Now().Add(time.Hour).String()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/sessionctx/sessionstates/session_token/mockValidateTokenTime", fmt.Sprintf("return('%s')", tm)))
	err = ValidateSessionToken(tokenBytes, "test_user")
	require.ErrorContains(t, err, "token expired")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/sessionctx/sessionstates/session_token/mockValidateTokenTime"))
	// wrong user name
	err = ValidateSessionToken(tokenBytes, "another_user")
	require.ErrorContains(t, err, "username does not match")
	// forge the user name
	token.Username = "another_user"
	tokenBytes, err = json.Marshal(token)
	require.NoError(t, err)
	err = ValidateSessionToken(tokenBytes, "another_user")
	require.ErrorContains(t, err, "verification error")
	// forge the expire time
	token.Username = "test_user"
	token.ExpireTime = time.Now().Add(-time.Minute)
	tokenBytes, err = json.Marshal(token)
	require.NoError(t, err)
	err = ValidateSessionToken(tokenBytes, "test_user")
	require.ErrorContains(t, err, "verification error")
}
