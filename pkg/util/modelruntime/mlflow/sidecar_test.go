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

package mlflow

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSidecarConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()
	require.NotEmpty(t, cfg.Python)
	require.Greater(t, cfg.Workers, 0)
}

func TestWriteSidecarScript(t *testing.T) {
	dir := t.TempDir()
	path, err := writeSidecarScript(dir)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestSidecarPoolCloseCleansSocketDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mlflow.sock"), []byte("x"), 0o644))

	pool := &SidecarPool{
		started:   true,
		socketDir: dir,
		sockets:   []string{filepath.Join(dir, "mlflow.sock")},
	}
	require.NoError(t, pool.Close())
	_, err := os.Stat(dir)
	require.True(t, os.IsNotExist(err))
	require.NoError(t, pool.Close())
}
