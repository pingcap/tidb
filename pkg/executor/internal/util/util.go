// Copyright 2023 PingCAP, Inc.
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

package util

import (
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/stretchr/testify/require"
)

// GetFunctionName returns the function name
func GetFunctionName() string {
	pc, _, _, _ := runtime.Caller(1)
	return path.Base(runtime.FuncForPC(pc).Name())
}

// CheckNoLeakFiles checks if there are file leaks
func CheckNoLeakFiles(t *testing.T, fileNamePrefixForTest string) {
	path := config.GetGlobalConfig().TempStoragePath
	log.Info(fmt.Sprintf("path: %s", path))
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			require.False(t, strings.HasPrefix(d.Name(), fileNamePrefixForTest))
		}
		return nil
	})
	require.NoError(t, err)
}
