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

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryptedPasswordConfigMerge(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "tidb-lightning.toml")
	require.NoError(t, os.WriteFile(configFile, []byte(`
[tidb]
password-encrypted-file = "toml-password.enc"
password-key-file = "toml-password.key"
`), 0o600))

	globalCfg, err := LoadGlobalConfig([]string{
		"--config", configFile,
		"--tidb-password-encrypted-file", "cli-password.enc",
		"--tidb-password-key-file", "",
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "cli-password.enc", globalCfg.TiDB.PasswordEncryptedFile)
	require.Equal(t, "toml-password.key", globalCfg.TiDB.PasswordKeyFile)

	taskCfg := NewConfig()
	require.NoError(t, taskCfg.LoadFromGlobal(globalCfg))
	require.Equal(t, "cli-password.enc", taskCfg.TiDB.PasswordEncryptedFile)
	require.Equal(t, "toml-password.key", taskCfg.TiDB.PasswordKeyFile)

	taskCfg.TiDB.Psw = "plaintext-password-canary"
	rendered := taskCfg.String()
	require.NotContains(t, rendered, taskCfg.TiDB.Psw)
	require.Contains(t, rendered, `"password-encrypted-file":"cli-password.enc"`)
	require.Contains(t, rendered, `"password-key-file":"toml-password.key"`)
}
