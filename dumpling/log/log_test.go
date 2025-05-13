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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitLogNoPermission(t *testing.T) {
	tmpDir := t.TempDir()
	conf := &Config{
		Level:  "debug",
		File:   tmpDir + "/test.log",
		Format: "text",
	}

	logger, _, err := InitAppLogger(conf)
	require.NoError(t, err)
	require.NotNil(t, logger)

	err = os.Chmod(tmpDir, 0)
	require.NoError(t, err)

	l, _, err := InitAppLogger(conf)
	if err == nil && os.Geteuid() == 0 {
		// current user is root, so we can write to the file
		l.Info("test")
		err = l.Sync()
		require.NoError(t, err)
		require.FileExists(t, tmpDir+"/test.log")
		return
	}
	require.ErrorContains(t, err, "permission denied")
}
