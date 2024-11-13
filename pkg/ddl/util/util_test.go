// Copyright 2024 PingCAP, Inc.
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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFolderNotEmpty(t *testing.T) {
	tmp := t.TempDir()
	require.False(t, FolderNotEmpty(tmp))
	require.False(t, FolderNotEmpty(filepath.Join(tmp, "not-exist")))

	f, err := os.Create(filepath.Join(tmp, "test-file"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, FolderNotEmpty(tmp))
}
