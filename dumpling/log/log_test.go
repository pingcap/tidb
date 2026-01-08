// Copyright 2025 PingCAP, Inc.
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

package log

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitLogNoPermission(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("Skip test when running as root user")
	}

	tmpDir := t.TempDir()

	conf := &Config{
		Level:  "debug",
		File:   tmpDir + "/test.log",
		Format: "text",
	}

	logger, _, err := InitAppLogger(conf)
	require.NoError(t, err)
	require.NotNil(t, logger)

	// Directory permission denied
	require.NoError(t, os.Chmod(tmpDir, 0))
	_, _, err = InitAppLogger(conf)
	require.Contains(t, err.Error(), "permission denied")
	require.NoError(t, os.Chmod(tmpDir, 0755))

	// Directory exists but doesn't allow file creation
	readOnlyDirPath := filepath.Join(tmpDir, "readonly-dir")
	require.NoError(t, os.Mkdir(readOnlyDirPath, 0755))
	require.NoError(t, os.Chmod(readOnlyDirPath, 0555)) // Read-only directory
	conf.File = readOnlyDirPath + "/test.log"
	_, _, err = InitAppLogger(conf)
	require.ErrorContains(t, err, "permission denied")
	require.NoError(t, os.Chmod(readOnlyDirPath, 0755))

	// Using a directory as log file
	dirLogPath := filepath.Join(tmpDir, "dir-as-log")
	require.NoError(t, os.Mkdir(dirLogPath, 0755))
	conf.File = dirLogPath
	_, _, err = InitAppLogger(conf)
	require.ErrorContains(t, err, "can't use directory as log file name")

	// File exists but is not writable
	filePath := filepath.Join(tmpDir, "readonly.log")
	file, err := os.Create(filePath)
	require.NoError(t, err)
	file.Close()
	require.NoError(t, os.Chmod(filePath, 0444))
	conf.File = filePath
	_, _, err = InitAppLogger(conf)
	require.ErrorContains(t, err, "permission denied")
	require.NoError(t, os.Chmod(filePath, 0644))

	// Ensure parent directory is created successfully
	nestedPath := filepath.Join(tmpDir, "nested/path/to")
	conf.File = nestedPath + "/test.log"
	_, _, err = InitAppLogger(conf)
	require.NoError(t, err)
	_, err = os.Stat(nestedPath)
	require.NoError(t, err)
}
