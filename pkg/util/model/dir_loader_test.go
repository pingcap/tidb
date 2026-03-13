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

package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadDirectoryArtifact(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "MLmodel"), []byte("ok"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "data.bin"), []byte("data"), 0o644))

	sum := sha256.Sum256([]byte("MLmodel\nok\ndata.bin\ndata\n"))
	checksum := "sha256:" + hex.EncodeToString(sum[:])

	loader := NewDirectoryLoader(DirectoryLoaderOptions{})
	artifact, err := loader.Load(context.Background(), ArtifactMeta{
		ModelID:  1,
		Version:  1,
		Engine:   "MLFLOW",
		Location: "file://" + root,
		Checksum: checksum,
	})
	require.NoError(t, err)
	require.NotEmpty(t, artifact.LocalPath)
}

func TestSafeMaterializePathRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	_, err := safeMaterializePath(root, "../outside")
	require.Error(t, err)

	_, err = safeMaterializePath(root, "a/../../outside")
	require.Error(t, err)

	_, err = safeMaterializePath(root, "/abs/path")
	require.Error(t, err)
}
