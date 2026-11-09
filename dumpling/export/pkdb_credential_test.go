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

package export

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/pkdbcredential"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	testPasswordKey      = "00112233445566778899aabbccddeeff"
	testWrongPasswordKey = "11223344556677889900aabbccddeeff"
)

type credentialTestRegistry struct {
	registerCalls int
}

func (r *credentialTestRegistry) Register(prometheus.Collector) error {
	r.registerCalls++
	return nil
}

func (r *credentialTestRegistry) MustRegister(collectors ...prometheus.Collector) {
	r.registerCalls += len(collectors)
}

func (*credentialTestRegistry) Unregister(prometheus.Collector) bool {
	return true
}

func writePasswordKey(t *testing.T, key string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "password.key")
	require.NoError(t, os.WriteFile(path, []byte(key), 0o600))
	return path
}

func createEncryptedPassword(t *testing.T, password, keyFile string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "password.enc")
	require.NoError(t, pkdbcredential.CreateEncryptedPasswordFile(strings.NewReader(password), keyFile, path))
	return path
}

func TestEncryptedPasswordFlags(t *testing.T) {
	conf := parseConfigFromArgsForTest(t,
		"--password-encrypted-file", "relative-password.enc",
		"--password-key-file", "relative-password.key",
	)

	require.Empty(t, conf.Password)
	require.Equal(t, "relative-password.enc", conf.PasswordEncryptedFile)
	require.Equal(t, "relative-password.key", conf.PasswordKeyFile)
}

func TestNewDumperResolvesEncryptedPasswordBeforeConfigAdjustment(t *testing.T) {
	const password = "dumpling-password-canary"
	keyFile := writePasswordKey(t, testPasswordKey)
	encryptedFile := createEncryptedPassword(t, password, keyFile)
	conf := DefaultConfig()
	conf.PasswordEncryptedFile = encryptedFile
	conf.PasswordKeyFile = keyFile
	conf.FileType = "unsupported"

	dumper, err := NewDumper(context.Background(), conf)
	require.Nil(t, dumper)
	require.EqualError(t, err, "unknown config.FileType 'unsupported'")
	require.Equal(t, password, conf.Password)
	require.Equal(t, password, conf.GetDriverConfig("").Passwd)

	envelope, err := os.ReadFile(encryptedFile)
	require.NoError(t, err)
	rendered := conf.String()
	require.NotContains(t, rendered, password)
	require.NotContains(t, rendered, testPasswordKey)
	require.NotContains(t, rendered, hex.EncodeToString(envelope))
}

func TestNewDumperRejectsInvalidPasswordSourcesBeforeInitialization(t *testing.T) {
	testCases := []struct {
		name                    string
		password                string
		passwordEncryptedFile   string
		passwordKeyFile         string
		expectedCredentialError *errors.Error
	}{
		{
			name:                    "conflict takes precedence over incomplete source",
			password:                "plaintext-password-canary",
			passwordEncryptedFile:   "missing-password.enc",
			expectedCredentialError: pkdbcredential.ErrPasswordSourceConflict,
		},
		{
			name:                    "incomplete encrypted source",
			passwordEncryptedFile:   "missing-password.enc",
			expectedCredentialError: pkdbcredential.ErrIncompleteEncryptedSource,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := &credentialTestRegistry{}
			conf := DefaultConfig()
			conf.Password = testCase.password
			conf.PasswordEncryptedFile = testCase.passwordEncryptedFile
			conf.PasswordKeyFile = testCase.passwordKeyFile
			conf.PromRegistry = registry
			conf.OutputDirPath = filepath.Join(t.TempDir(), "output")
			conf.LogFile = filepath.Join(t.TempDir(), "dumpling.log")

			dumper, err := NewDumper(context.Background(), conf)
			require.Nil(t, dumper)
			require.Error(t, err)
			require.True(t, testCase.expectedCredentialError.Equal(err))
			require.Equal(t, testCase.password, conf.Password)
			require.Zero(t, registry.registerCalls)
			_, statErr := os.Stat(conf.OutputDirPath)
			require.ErrorIs(t, statErr, os.ErrNotExist)
			_, statErr = os.Stat(conf.LogFile)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestNewDumperRejectsWrongPasswordKeyWithoutLeakingSecrets(t *testing.T) {
	const password = "wrong-key-password-canary"
	keyFile := writePasswordKey(t, testPasswordKey)
	encryptedFile := createEncryptedPassword(t, password, keyFile)
	wrongKeyFile := writePasswordKey(t, testWrongPasswordKey)
	registry := &credentialTestRegistry{}
	conf := DefaultConfig()
	conf.PasswordEncryptedFile = encryptedFile
	conf.PasswordKeyFile = wrongKeyFile
	conf.PromRegistry = registry

	dumper, err := NewDumper(context.Background(), conf)
	require.Nil(t, dumper)
	require.Error(t, err)
	require.True(t, pkdbcredential.ErrAuthenticationFailed.Equal(err))
	require.Empty(t, conf.Password)
	require.Zero(t, registry.registerCalls)

	envelope, readErr := os.ReadFile(encryptedFile)
	require.NoError(t, readErr)
	require.NotContains(t, err.Error(), password)
	require.NotContains(t, err.Error(), testPasswordKey)
	require.NotContains(t, err.Error(), testWrongPasswordKey)
	require.NotContains(t, err.Error(), hex.EncodeToString(envelope))
}
