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
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

var (
	mockValidateTokenTime = "github.com/pingcap/tidb/sessionctx/sessionstates/mockValidateTokenTime"
	mockLoadCertTime      = "github.com/pingcap/tidb/sessionctx/sessionstates/mockLoadCertTime"
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
	tm := time.Now().Add(time.Hour).Format(time.RFC3339)
	require.NoError(t, failpoint.Enable(mockValidateTokenTime, fmt.Sprintf(`return("%s")`, tm)))
	err = ValidateSessionToken(tokenBytes, "test_user")
	require.NoError(t, failpoint.Disable(mockValidateTokenTime))
	require.ErrorContains(t, err, "token expired")
	// wrong user name
	err = ValidateSessionToken(tokenBytes, "another_user")
	require.ErrorContains(t, err, "username does not match")
	// forge the user name
	token.Username = "another_user"
	tokenBytes2, err := json.Marshal(token)
	require.NoError(t, err)
	err = ValidateSessionToken(tokenBytes2, "another_user")
	require.ErrorContains(t, err, "verification error")
	// forge the expire time
	token.Username = "test_user"
	token.ExpireTime = time.Now().Add(-time.Minute)
	tokenBytes2, err = json.Marshal(token)
	require.NoError(t, err)
	err = ValidateSessionToken(tokenBytes2, "test_user")
	require.ErrorContains(t, err, "verification error")
	// replace the cert, but the old cert is still valid for a while
	certPath2 := filepath.Join(tempDir, "test2_cert.pem")
	keyPath2 := filepath.Join(tempDir, "test2_key.pem")
	err = util.CreateTLSCertificates(certPath2, keyPath2, 4096)
	require.NoError(t, err)
	SetKeyPath(keyPath2)
	SetCertPath(certPath2)
	err = ValidateSessionToken(tokenBytes, "test_user")
	require.NoError(t, err)
	// the old cert expires
	require.NoError(t, failpoint.Enable(mockLoadCertTime, fmt.Sprintf(`return("%s")`, tm)))
	err = ValidateSessionToken(tokenBytes, "test_user")
	require.NoError(t, failpoint.Disable(mockLoadCertTime))
	require.ErrorContains(t, err, "verification error")
	// the new cert expires but is not overwritten
	token, err = CreateSessionToken("test_user")
	require.NoError(t, err)
	tokenBytes, err = json.Marshal(token)
	require.NoError(t, err)
	tm = time.Now().Add(50 * 24 * time.Hour).Format(time.RFC3339)
	require.NoError(t, failpoint.Enable(mockLoadCertTime, fmt.Sprintf(`return("%s")`, tm)))
	err = ValidateSessionToken(tokenBytes, "test_user")
	require.NoError(t, failpoint.Disable(mockLoadCertTime))
	require.NoError(t, err)
	// the cert expires again and is overwritten
	err = util.CreateTLSCertificates(certPath2, keyPath2, 4096)
	require.NoError(t, err)
	tm = time.Now().Add(100 * 24 * time.Hour).Format(time.RFC3339)
	require.NoError(t, failpoint.Enable(mockLoadCertTime, fmt.Sprintf(`return("%s")`, tm)))
	err = ValidateSessionToken(tokenBytes, "test_user")
	require.NoError(t, failpoint.Disable(mockLoadCertTime))
	require.ErrorContains(t, err, "verification error")
}
