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

// revive:disable-next-line:file-header
package repo

import (
	"context"
	"path"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
)

const pathSeparator = "/"

type prefixedStorage struct {
	base   storage.Storage
	prefix storage.Prefix
}

func NewPrefixedStorage(base storage.Storage, prefix string) storage.Storage {
	return &prefixedStorage{
		base:   base,
		prefix: storage.NewPrefix(prefix),
	}
}

func (s *prefixedStorage) WriteFile(
	ctx context.Context,
	name string,
	data []byte,
) error {
	return s.base.WriteFile(ctx, s.fullPath(name), data)
}

func (s *prefixedStorage) ReadFile(
	ctx context.Context,
	name string,
) ([]byte, error) {
	return s.base.ReadFile(ctx, s.fullPath(name))
}

func (s *prefixedStorage) FileExists(
	ctx context.Context,
	name string,
) (bool, error) {
	return s.base.FileExists(ctx, s.fullPath(name))
}

func (s *prefixedStorage) DeleteFile(ctx context.Context, name string) error {
	return s.base.DeleteFile(ctx, s.fullPath(name))
}

func (s *prefixedStorage) Open(
	ctx context.Context,
	name string,
	option *storage.ReaderOption,
) (storage.ExternalFileReader, error) {
	return s.base.Open(ctx, s.fullPath(name), option)
}

func (s *prefixedStorage) DeleteFiles(
	ctx context.Context,
	names []string,
) error {
	prefixed := make([]string, 0, len(names))
	for _, name := range names {
		prefixed = append(prefixed, s.fullPath(name))
	}
	return s.base.DeleteFiles(ctx, prefixed)
}

func (s *prefixedStorage) WalkDir(
	ctx context.Context,
	opt *storage.WalkOption,
	fn func(filePath string, size int64) error,
) error {
	walkOpt := &storage.WalkOption{}
	if opt != nil {
		*walkOpt = *opt
	}
	walkOpt.SubDir = s.fullPath(walkOpt.SubDir)
	return s.base.WalkDir(
		ctx,
		walkOpt,
		func(filePath string, size int64) error {
			return fn(s.trimPath(filePath), size)
		},
	)
}

func (s *prefixedStorage) URI() string {
	base := strings.TrimRight(s.base.URI(), pathSeparator)
	if s.prefix.String() == "" {
		return base
	}
	return base + pathSeparator +
		strings.TrimRight(s.prefix.String(), pathSeparator)
}

func (s *prefixedStorage) Create(
	ctx context.Context,
	name string,
	option *storage.WriterOption,
) (storage.ExternalFileWriter, error) {
	return s.base.Create(ctx, s.fullPath(name), option)
}

func (s *prefixedStorage) Rename(
	ctx context.Context,
	oldFileName, newFileName string,
) error {
	return s.base.Rename(ctx, s.fullPath(oldFileName), s.fullPath(newFileName))
}

func (s *prefixedStorage) Features() storage.Features {
	return storage.FeatureOf(s.base)
}

func (s *prefixedStorage) Close() {
	s.base.Close()
}

func (s *prefixedStorage) fullPath(name string) string {
	return s.prefix.ObjectKey(name)
}

func (s *prefixedStorage) trimPath(name string) string {
	prefix := s.prefix.String()
	trimmed := strings.TrimPrefix(name, prefix)
	return strings.TrimPrefix(trimmed, path.Clean(pathSeparator))
}
