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

package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/util/pkdbcredential"
	"github.com/stretchr/testify/require"
)

const testPasswordKey = "00112233445566778899aabbccddeeff"

func createEncryptedPasswordFiles(t *testing.T, password string) (string, string) {
	t.Helper()

	dir := t.TempDir()
	keyFile := filepath.Join(dir, "password.key")
	encryptedFile := filepath.Join(dir, "password.enc")
	require.NoError(t, os.WriteFile(keyFile, []byte(testPasswordKey), 0o600))
	require.NoError(t, pkdbcredential.CreateEncryptedPasswordFile(strings.NewReader(password), keyFile, encryptedFile))
	return encryptedFile, keyFile
}

func newCredentialTestGlobalConfig(t *testing.T) *config.GlobalConfig {
	t.Helper()

	globalCfg := config.NewGlobalConfig()
	globalCfg.TiDB.Host = "test.invalid"
	globalCfg.TiDB.Port = 4000
	globalCfg.TiDB.User = "lightning"
	globalCfg.TiDB.PdAddr = "test.invalid:2379"
	globalCfg.Mydumper.SourceDir = "file://."
	globalCfg.TikvImporter.Backend = config.BackendLocal
	globalCfg.TikvImporter.SortedKVDir = t.TempDir()
	return globalCfg
}

func TestRunOnceResolvesEncryptedPasswordBeforeAdjust(t *testing.T) {
	const password = "run-once-password-canary"
	encryptedFile, keyFile := createEncryptedPasswordFiles(t, password)
	globalCfg := newCredentialTestGlobalConfig(t)
	globalCfg.Mydumper.SourceDir = filepath.Join(t.TempDir(), "missing")

	lightning := New(globalCfg)
	taskCfg := config.NewConfig()
	require.NoError(t, taskCfg.LoadFromGlobal(globalCfg))
	taskCfg.TiDB.PasswordEncryptedFile = encryptedFile
	taskCfg.TiDB.PasswordKeyFile = keyFile

	err := lightning.RunOnceWithOptions(context.Background(), taskCfg)
	require.Error(t, err)
	require.Equal(t, password, taskCfg.TiDB.Psw)
	require.NotContains(t, err.Error(), password)
}

func TestPostTaskResolvesEncryptedPasswordBeforeEnqueue(t *testing.T) {
	const password = "server-password-canary"
	encryptedFile, keyFile := createEncryptedPasswordFiles(t, password)
	lightning := New(newCredentialTestGlobalConfig(t))
	lightning.taskCfgs = config.NewConfigList()

	body := fmt.Sprintf(`
[tidb]
password-encrypted-file = %q
password-key-file = %q
[checkpoint]
enable = true
driver = "mysql"
`, encryptedFile, keyFile)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(body))
	lightning.handlePostTask(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotContains(t, recorder.Body.String(), password)
	require.NotContains(t, recorder.Body.String(), testPasswordKey)

	popCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taskCfg, err := lightning.taskCfgs.Pop(popCtx)
	require.NoError(t, err)
	require.Equal(t, password, taskCfg.TiDB.Psw)
	require.NotNil(t, taskCfg.Checkpoint.MySQLParam)
	require.Equal(t, password, taskCfg.Checkpoint.MySQLParam.Password)
}

func TestPostTaskRejectsPasswordSourceConflictWithoutEnqueue(t *testing.T) {
	const password = "plaintext-password-canary"
	globalCfg := newCredentialTestGlobalConfig(t)
	globalCfg.TiDB.Psw = password
	lightning := New(globalCfg)
	lightning.taskCfgs = config.NewConfigList()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`
[tidb]
password-encrypted-file = "missing-password.enc"
password-key-file = "missing-password.key"
`))
	lightning.handlePostTask(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "ErrPasswordSourceConflict")
	require.NotContains(t, recorder.Body.String(), password)
	require.Empty(t, lightning.taskCfgs.AllIDs())
}
