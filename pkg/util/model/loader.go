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
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore"
)

// LoaderOptions configures the artifact loader.
type LoaderOptions struct {
	Timeout time.Duration
}

// ArtifactLoaderImpl loads artifacts from external storage.
type ArtifactLoaderImpl struct {
	timeout time.Duration
}

// NewArtifactLoader constructs a new loader.
func NewArtifactLoader(opts LoaderOptions) *ArtifactLoaderImpl {
	return &ArtifactLoaderImpl{timeout: opts.Timeout}
}

// Load fetches and validates a model artifact.
func (l *ArtifactLoaderImpl) Load(ctx context.Context, meta ArtifactMeta) (Artifact, error) {
	if meta.Location == "" {
		return Artifact{}, errors.New("model location is required")
	}
	expected, err := parseSHA256Checksum(meta.Checksum)
	if err != nil {
		return Artifact{}, errors.Trace(err)
	}
	baseURI, objectName, err := splitLocation(meta.Location)
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
	data, err := storage.ReadFile(ctx, objectName)
	if err != nil {
		return Artifact{}, errors.Annotate(err, "load model artifact")
	}
	actual := sha256.Sum256(data)
	if !bytesEqual(actual[:], expected) {
		return Artifact{}, errors.New("model checksum mismatch")
	}
	return Artifact{Meta: meta, Bytes: data, LocalPath: "", FetchedAt: time.Now()}, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func parseSHA256Checksum(checksum string) ([]byte, error) {
	const prefix = "sha256:"
	if !strings.HasPrefix(checksum, prefix) {
		return nil, errors.New("checksum must use sha256:<hex> format")
	}
	hexStr := strings.TrimPrefix(checksum, prefix)
	if len(hexStr) != sha256.Size*2 {
		return nil, errors.New("checksum must be a 64-char sha256 hex string")
	}
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, errors.New("checksum must be valid hex")
	}
	return decoded, nil
}

func splitLocation(raw string) (base string, name string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	if u.Scheme == "" {
		return "", "", errors.New("location must include scheme")
	}
	switch u.Scheme {
	case "file":
		if u.Host != "" && u.Host != "localhost" {
			return "", "", errors.New("file URI host must be empty or localhost")
		}
		if u.Path == "" || u.Path == "/" {
			return "", "", errors.New("file URI must include a file path")
		}
		dir, name := filepath.Split(u.Path)
		if name == "" {
			return "", "", errors.New("file URI must point to a file")
		}
		base := "file://" + dir
		return base, name, nil
	case "s3", "ks3", "oss":
		if u.Host == "" {
			return "", "", errors.New("s3 URI must include bucket")
		}
		fullPath := strings.TrimPrefix(u.Path, "/")
		if fullPath == "" {
			return "", "", errors.New("s3 URI must include object path")
		}
		dir, name := path.Split(fullPath)
		if name == "" {
			return "", "", errors.New("s3 URI must point to a file")
		}
		base := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
		dir = strings.TrimSuffix(dir, "/")
		if dir != "" {
			base = base + "/" + dir
		}
		if u.RawQuery != "" {
			base = base + "?" + u.RawQuery
		}
		return base, name, nil
	default:
		return "", "", errors.New("unsupported location scheme")
	}
}
