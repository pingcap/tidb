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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestArtifactLoaderFileSuccess(t *testing.T) {
	ctx := context.Background()
	data := []byte("model-bytes")
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "m1.onnx")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	sum := sha256.Sum256(data)
	checksum := "sha256:" + hex.EncodeToString(sum[:])

	loader := NewArtifactLoader(LoaderOptions{Timeout: time.Second})
	artifact, err := loader.Load(ctx, ArtifactMeta{
		ModelID:  1,
		Version:  1,
		Engine:   "ONNX",
		Location: "file://" + path,
		Checksum: checksum,
	})
	require.NoError(t, err)
	require.Equal(t, data, artifact.Bytes)
}

func TestArtifactLoaderBadChecksum(t *testing.T) {
	ctx := context.Background()
	data := []byte("model-bytes")
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "m1.onnx")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	loader := NewArtifactLoader(LoaderOptions{Timeout: time.Second})
	_, err := loader.Load(ctx, ArtifactMeta{
		ModelID:  1,
		Version:  1,
		Engine:   "ONNX",
		Location: "file://" + path,
		Checksum: "sha256:deadbeef",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum")
}

func TestArtifactLoaderInvalidScheme(t *testing.T) {
	ctx := context.Background()
	sum := sha256.Sum256(nil)
	checksum := "sha256:" + hex.EncodeToString(sum[:])
	loader := NewArtifactLoader(LoaderOptions{Timeout: time.Second})
	_, err := loader.Load(ctx, ArtifactMeta{
		ModelID:  1,
		Version:  1,
		Engine:   "ONNX",
		Location: "http://example.com/m1.onnx",
		Checksum: checksum,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "scheme")
}

func TestArtifactCacheGetOrLoadUsesCache(t *testing.T) {
	cache := NewArtifactCache(CacheOptions{
		Capacity: 2,
		TTL:      time.Minute,
	})
	loader := &countingLoader{artifact: Artifact{Meta: ArtifactMeta{ModelID: 1, Version: 1}}}

	meta := ArtifactMeta{ModelID: 1, Version: 1}
	ctx := context.Background()
	_, err := cache.GetOrLoad(ctx, meta, loader)
	require.NoError(t, err)
	_, err = cache.GetOrLoad(ctx, meta, loader)
	require.NoError(t, err)
	require.Equal(t, 1, loader.calls)
}

func TestArtifactCacheTTLExpires(t *testing.T) {
	now := time.Date(2026, 2, 20, 0, 0, 0, 0, time.UTC)
	cache := NewArtifactCache(CacheOptions{
		Capacity: 1,
		TTL:      time.Second,
		Now:      func() time.Time { return now },
	})
	loader := &countingLoader{artifact: Artifact{Meta: ArtifactMeta{ModelID: 1, Version: 1}}}
	meta := ArtifactMeta{ModelID: 1, Version: 1}

	ctx := context.Background()
	_, err := cache.GetOrLoad(ctx, meta, loader)
	require.NoError(t, err)
	now = now.Add(2 * time.Second)
	_, err = cache.GetOrLoad(ctx, meta, loader)
	require.NoError(t, err)
	require.Equal(t, 2, loader.calls)
}

func TestStatementCacheUsesStatementCache(t *testing.T) {
	stmtCache := NewStatementCache()
	loader := &countingLoader{artifact: Artifact{Meta: ArtifactMeta{ModelID: 2, Version: 3}}}
	meta := ArtifactMeta{ModelID: 2, Version: 3}
	ctx := context.Background()

	_, err := GetArtifact(ctx, meta, stmtCache, nil, loader)
	require.NoError(t, err)
	_, err = GetArtifact(ctx, meta, stmtCache, nil, loader)
	require.NoError(t, err)
	require.Equal(t, 1, loader.calls)
}

type countingLoader struct {
	calls    int
	artifact Artifact
}

func (l *countingLoader) Load(ctx context.Context, meta ArtifactMeta) (Artifact, error) {
	l.calls++
	if l.artifact.Meta.ModelID == 0 {
		l.artifact.Meta = meta
	}
	return l.artifact, nil
}

func TestSplitLocationFile(t *testing.T) {
	base, name, err := splitLocation("file:///tmp/model.onnx")
	require.NoError(t, err)
	require.Equal(t, "file:///tmp/", base)
	require.Equal(t, "model.onnx", name)
}

func TestSplitLocationS3(t *testing.T) {
	base, name, err := splitLocation("s3://bucket/path/to/model.onnx?region=us-east-1")
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/path/to?region=us-east-1", base)
	require.Equal(t, "model.onnx", name)
}

func TestChecksumFormatValidation(t *testing.T) {
	_, err := parseSHA256Checksum("sha256:zz")
	require.Error(t, err)
	_, err = parseSHA256Checksum("deadbeef")
	require.Error(t, err)

	sum := sha256.Sum256([]byte("ok"))
	_, err = parseSHA256Checksum("sha256:" + hex.EncodeToString(sum[:]))
	require.NoError(t, err)
}

func TestArtifactLocationErrors(t *testing.T) {
	_, _, err := splitLocation("file://")
	require.Error(t, err)
	_, _, err = splitLocation("s3://bucket/")
	require.Error(t, err)
	_, _, err = splitLocation("local:///tmp/model.onnx")
	require.Error(t, err)
	_, _, err = splitLocation("/tmp/model.onnx")
	require.Error(t, err)
}

func TestArtifactLoaderTimeout(t *testing.T) {
	ctx := context.Background()
	sum := sha256.Sum256(nil)
	checksum := "sha256:" + hex.EncodeToString(sum[:])
	loader := NewArtifactLoader(LoaderOptions{Timeout: time.Nanosecond})
	_, err := loader.Load(ctx, ArtifactMeta{
		ModelID:  1,
		Version:  1,
		Engine:   "ONNX",
		Location: "file:///does/not/exist.onnx",
		Checksum: checksum,
	})
	require.Error(t, err)
	require.Contains(t, fmt.Sprintf("%v", err), "load model")
}
