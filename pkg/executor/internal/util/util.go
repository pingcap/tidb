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
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// CheckNoLeakFiles checks if there are file leaks
func CheckNoLeakFiles(t *testing.T) {
	log.Info(fmt.Sprintf("path: %s", config.GetGlobalConfig().TempStoragePath))

	err := filepath.WalkDir(config.GetGlobalConfig().TempStoragePath, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			require.False(t, strings.HasPrefix(d.Name(), chunk.DefaultChunkDataInDiskByChunksPath))
		}
		return nil
	})
	require.NoError(t, err)
}
