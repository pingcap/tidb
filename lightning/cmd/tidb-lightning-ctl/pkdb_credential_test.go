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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/util/pkdbcredential"
	"github.com/stretchr/testify/require"
)

const plaintextPassword = "plaintext-password-canary"

func runWithArgs(t *testing.T, args ...string) error {
	t.Helper()

	originalArgs := os.Args
	t.Cleanup(func() { os.Args = originalArgs })
	os.Args = append([]string{"tidb-lightning-ctl"}, args...)
	return run()
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "tidb-lightning.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestInheritedMySQLCheckpointOperationRejectsPasswordSourceConflict(t *testing.T) {
	configFile := writeConfig(t, `
[checkpoint]
driver = "mysql"
`)
	err := runWithArgs(t,
		"--config", configFile,
		"--checkpoint-dump", t.TempDir(),
		"--tidb-password", plaintextPassword,
		"--tidb-password-encrypted-file", "missing-password.enc",
		"--tidb-password-key-file", "missing-password.key",
	)

	require.Error(t, err)
	require.True(t, pkdbcredential.ErrPasswordSourceConflict.Equal(err))
	require.NotContains(t, err.Error(), plaintextPassword)
}

func TestCheckpointOperationWithIndependentPasswordSkipsResolution(t *testing.T) {
	testCases := []struct {
		name   string
		config string
	}{
		{
			name: "file checkpoint",
			config: `
[tikv-importer]
backend = "no_such_backend"
`,
		},
		{
			name: "explicit MySQL checkpoint DSN",
			config: `
[checkpoint]
driver = "mysql"
dsn = "root@tcp(127.0.0.1:4000)/"
[tikv-importer]
backend = "no_such_backend"
`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			args := []string{
				"--checkpoint-dump", t.TempDir(),
				"--tidb-password", plaintextPassword,
				"--tidb-password-encrypted-file", "missing-password.enc",
				"--tidb-password-key-file", "missing-password.key",
			}
			args = append(args, "--config", writeConfig(t, testCase.config))

			err := runWithArgs(t, args...)
			require.EqualError(t, err, "[Lightning:Config:ErrInvalidConfig]unsupported `tikv-importer.backend` (no_such_backend)")
			require.NotContains(t, err.Error(), plaintextPassword)
		})
	}
}

func TestDestroyErrorRejectsPasswordSourceConflict(t *testing.T) {
	err := runWithArgs(t,
		"--checkpoint-error-destroy", "all",
		"--tidb-password", plaintextPassword,
		"--tidb-password-encrypted-file", "missing-password.enc",
		"--tidb-password-key-file", "missing-password.key",
	)

	require.Error(t, err)
	require.True(t, pkdbcredential.ErrPasswordSourceConflict.Equal(err))
	require.NotContains(t, err.Error(), plaintextPassword)
}

func TestRemoveCheckpointRejectsPasswordSourceConflict(t *testing.T) {
	for _, driver := range []string{"file", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			configFile := writeConfig(t, `
[checkpoint]
driver = "`+driver+`"
dsn = "root@tcp(127.0.0.1:4000)/"
`)
			err := runWithArgs(t,
				"--config", configFile,
				"--checkpoint-remove", "all",
				"--tidb-password", plaintextPassword,
				"--tidb-password-encrypted-file", "missing-password.enc",
				"--tidb-password-key-file", "missing-password.key",
			)

			require.Error(t, err)
			require.True(t, pkdbcredential.ErrPasswordSourceConflict.Equal(err))
			require.NotContains(t, err.Error(), plaintextPassword)
		})
	}
}
