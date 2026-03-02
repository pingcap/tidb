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
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// DirectoryLoaderOptions configures the directory artifact loader.
type DirectoryLoaderOptions struct {
	Timeout time.Duration
}

// DirectoryLoader loads model artifacts stored as a directory tree.
type DirectoryLoader struct {
	timeout time.Duration
}

// NewDirectoryLoader constructs a new directory loader.
func NewDirectoryLoader(opts DirectoryLoaderOptions) *DirectoryLoader {
	return &DirectoryLoader{timeout: opts.Timeout}
}

// Load fetches and validates a model directory and materializes it locally.
func (l *DirectoryLoader) Load(ctx context.Context, meta ArtifactMeta) (Artifact, error) {
	if meta.Location == "" {
		return Artifact{}, errors.New("model location is required")
	}
	expected, err := parseSHA256Checksum(meta.Checksum)
	if err != nil {
		return Artifact{}, errors.Trace(err)
	}
	baseURI, err := normalizeDirLocation(meta.Location)
	if err != nil {
		return Artifact{}, errors.Trace(err)
	}
	if l.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.timeout)
		defer cancel()
	}
	storage, err := objstore.NewFromURL(ctx, baseURI)
	if err != nil {
		return Artifact{}, errors.Annotate(err, "load model storage")
	}
	defer storage.Close()

	entries, err := listFiles(ctx, storage)
	if err != nil {
		return Artifact{}, err
	}
	manifestHash, err := hashManifest(ctx, storage, entries)
	if err != nil {
		return Artifact{}, err
	}
	if !bytesEqual(manifestHash, expected) {
		return Artifact{}, errors.New("model checksum mismatch")
	}

	localDir, err := materializeDir(ctx, storage, entries)
	if err != nil {
		return Artifact{}, err
	}
	return Artifact{Meta: meta, LocalPath: localDir, FetchedAt: time.Now()}, nil
}

func normalizeDirLocation(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", errors.Trace(err)
	}
	if u.Scheme == "" {
		return "", errors.New("location must include scheme")
	}
	if u.Path == "" || u.Path == "/" {
		return "", errors.New("location must include a directory path")
	}
	switch u.Scheme {
	case "file":
		if u.Host != "" && u.Host != "localhost" {
			return "", errors.New("file URI host must be empty or localhost")
		}
	case "s3", "ks3", "oss":
		if u.Host == "" {
			return "", errors.New("s3 URI must include bucket")
		}
	default:
		return "", errors.New("unsupported location scheme")
	}
	return raw, nil
}

func listFiles(ctx context.Context, storage storeapi.Storage) ([]string, error) {
	var paths []string
	err := storage.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, _ int64) error {
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, errors.Annotate(err, "walk model directory")
	}
	sort.Strings(paths)
	return paths, nil
}

func hashManifest(ctx context.Context, storage storeapi.Storage, paths []string) ([]byte, error) {
	h := sha256.New()
	for _, p := range paths {
		data, err := storage.ReadFile(ctx, p)
		if err != nil {
			return nil, errors.Annotate(err, "read model file")
		}
		_, _ = io.WriteString(h, p)
		_, _ = io.WriteString(h, "\n")
		_, _ = h.Write(data)
		_, _ = io.WriteString(h, "\n")
	}
	return h.Sum(nil), nil
}

func materializeDir(ctx context.Context, storage storeapi.Storage, paths []string) (string, error) {
	root, err := os.MkdirTemp("", "tidb-mlflow-")
	if err != nil {
		return "", errors.Trace(err)
	}
	for _, p := range paths {
		outPath, err := safeMaterializePath(root, p)
		if err != nil {
			_ = os.RemoveAll(root)
			return "", err
		}
		data, err := storage.ReadFile(ctx, p)
		if err != nil {
			_ = os.RemoveAll(root)
			return "", errors.Annotate(err, "read model file")
		}
		if err := os.MkdirAll(filepath.Dir(outPath), 0o750); err != nil {
			_ = os.RemoveAll(root)
			return "", errors.Trace(err)
		}
		if err := os.WriteFile(outPath, data, 0o644); err != nil {
			_ = os.RemoveAll(root)
			return "", errors.Trace(err)
		}
	}
	return root, nil
}

func safeMaterializePath(root, pathInStore string) (string, error) {
	clean := path.Clean(pathInStore)
	if clean == "." || clean == "" || strings.HasPrefix(clean, "/") || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", errors.Errorf("invalid model path: %s", pathInStore)
	}
	outPath := filepath.Join(root, filepath.FromSlash(clean))
	rel, err := filepath.Rel(root, outPath)
	if err != nil {
		return "", errors.Trace(err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.Errorf("invalid model path: %s", pathInStore)
	}
	return outPath, nil
}
