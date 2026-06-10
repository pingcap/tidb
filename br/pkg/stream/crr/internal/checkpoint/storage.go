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

package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"path"
	"strings"

	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/stream/backupmetas"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const (
	metaSuffix       = ".meta"
	maxStoreIDSuffix = "FFFFFFFFFFFFFFFF~"
)

var errStopWalkIteration = errors.New("stop walk iteration")

type parsedMetaFile struct {
	path    string
	flushTS uint64
	storeID uint64
}

type loadedMetaFile struct {
	path          string
	flushTS       uint64
	storeID       uint64
	dataFilePaths []string
}

type walkEntry struct {
	path string
	size int64
}

func walkDirSeq(
	ctx context.Context,
	storage UpstreamStorageReader,
	opt *storeapi.WalkOption,
) iter.Seq2[walkEntry, error] {
	return func(yield func(walkEntry, error) bool) {
		err := storage.WalkDir(ctx, opt, func(filePath string, size int64) error {
			if !yield(walkEntry{path: filePath, size: size}, nil) {
				return errStopWalkIteration
			}
			return nil
		})
		if err == nil || errors.Is(err, errStopWalkIteration) {
			return
		}
		var zero walkEntry
		yield(zero, fmt.Errorf("walk upstream backupmeta prefix: %w", err))
	}
}

func (c *Calculator) newMetaFileSeq(ctx context.Context) iter.Seq2[parsedMetaFile, error] {
	walkOpt := &storeapi.WalkOption{SubDir: stream.GetStreamBackupMetaPrefix()}
	if startAfter := metaScanStartAfter(c.state.syncedTS); startAfter != "" {
		walkOpt.StartAfter = startAfter
	}

	return func(yield func(parsedMetaFile, error) bool) {
		failpoint.InjectCall("before-list-meta")
		for entry, err := range walkDirSeq(ctx, c.deps.Upstream, walkOpt) {
			if err != nil {
				var zero parsedMetaFile
				yield(zero, err)
				return
			}
			if !strings.HasSuffix(entry.path, metaSuffix) {
				continue
			}
			baseName := strings.TrimSuffix(path.Base(entry.path), metaSuffix)
			parsed, err := backupmetas.ParseName(baseName)
			if err != nil {
				var zero parsedMetaFile
				yield(zero, fmt.Errorf("parse backupmeta name %s: %w", entry.path, err))
				return
			}
			if parsed.FlushTS <= c.state.syncedTS {
				continue
			}
			if !yield(parsedMetaFile{
				path:    entry.path,
				flushTS: parsed.FlushTS,
				storeID: parsed.StoreID,
			}, nil) {
				return
			}
		}
	}
}

func loadMetaFile(
	ctx context.Context,
	storage UpstreamStorageReader,
	metaFile parsedMetaFile,
) (loadedMetaFile, error) {
	metaBytes, err := storage.ReadFile(ctx, metaFile.path)
	if err != nil {
		return loadedMetaFile{}, fmt.Errorf("read upstream backupmeta %s: %w", metaFile.path, err)
	}

	meta, err := parseBackupMetadata(metaBytes)
	if err != nil {
		return loadedMetaFile{}, fmt.Errorf("parse backupmeta %s: %w", metaFile.path, err)
	}

	storeID, err := resolveStoreID(metaFile.storeID, meta.GetStoreId(), metaFile.path)
	if err != nil {
		return loadedMetaFile{}, err
	}

	return loadedMetaFile{
		path:          metaFile.path,
		flushTS:       metaFile.flushTS,
		storeID:       storeID,
		dataFilePaths: extractDataFilePaths(meta),
	}, nil
}

func parseBackupMetadata(rawMeta []byte) (*backuppb.Metadata, error) {
	return (*stream.MetadataHelper).ParseToMetadata(nil, rawMeta)
}

func resolveStoreID(nameStoreID uint64, contentStoreID int64, metaPath string) (uint64, error) {
	if contentStoreID <= 0 {
		return 0, fmt.Errorf("backupmeta %s contains invalid store id %d", metaPath, contentStoreID)
	}
	storeID := uint64(contentStoreID)
	if nameStoreID != 0 && nameStoreID != storeID {
		return 0, fmt.Errorf(
			"backupmeta %s has mismatched store id between name (%d) and content (%d)",
			metaPath, nameStoreID, storeID,
		)
	}
	return storeID, nil
}

func extractDataFilePaths(meta *backuppb.Metadata) []string {
	paths := make([]string, 0, len(meta.FileGroups))
	for _, group := range meta.FileGroups {
		if group.Path != "" {
			paths = append(paths, group.Path)
			continue
		}
		for _, file := range group.DataFilesInfo {
			if file.Path != "" {
				paths = append(paths, file.Path)
			}
		}
	}
	return paths
}

func validateIncrementalMetaScanStorage(rawURI string) error {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return fmt.Errorf("parse upstream storage uri %q: %w", rawURI, err)
	}
	switch parsed.Scheme {
	case "s3", "file", "gcs":
		return nil
	case "":
		return fmt.Errorf("upstream storage uri %q has empty scheme", rawURI)
	default:
		return fmt.Errorf(
			"crr checkpoint calculator requires StartAfter-capable upstream storage, got %s",
			rawURI,
		)
	}
}

func metaScanStartAfter(syncedTS uint64) string {
	if syncedTS == 0 {
		return ""
	}
	return path.Join(
		stream.GetStreamBackupMetaPrefix(),
		fmt.Sprintf("%016X%s", syncedTS, maxStoreIDSuffix),
	)
}
