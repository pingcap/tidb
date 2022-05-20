// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"strconv"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type StreamMetadataSet struct {
	metadata map[string]*backuppb.Metadata
	// The metadata after changed that needs to be write back.
	writeback map[string]*backuppb.Metadata

	BeforeDoWriteBack func(path string, last, current *backuppb.Metadata) (skip bool)
}

// LoadFrom loads data from an external storage into the stream metadata set.
func (ms *StreamMetadataSet) LoadFrom(ctx context.Context, s storage.ExternalStorage) error {
	ms.metadata = map[string]*backuppb.Metadata{}
	ms.writeback = map[string]*backuppb.Metadata{}
	opt := &storage.WalkOption{SubDir: GetStreamBackupMetaPrefix()}
	return s.WalkDir(ctx, opt, func(path string, size int64) error {
		// Maybe load them lazily for preventing out of memory?
		bs, err := s.ReadFile(ctx, path)
		if err != nil {
			return errors.Annotatef(err, "failed to read file %s", path)
		}
		var meta backuppb.Metadata
		if err := meta.Unmarshal(bs); err != nil {
			return errors.Annotatef(err, "failed to unmarshal file %s, maybe corrupted", path)
		}
		ms.metadata[path] = &meta
		return nil
	})
}

func (ms *StreamMetadataSet) iterateDataFiles(f func(d *backuppb.DataFileInfo) (shouldBreak bool)) {
	for _, m := range ms.metadata {
		for _, d := range m.Files {
			if f(d) {
				return
			}
		}
	}
}

// IterateFilesFullyBefore runs the function over all files contain data before the timestamp only.
//   0                                          before
//   |------------------------------------------|
//    |-file1---------------| <- File contains records in this TS range would be found.
//                                  |-file2--------------| <- File contains any record out of this won't be found.
// This function would call the `f` over file1 only.
func (ms *StreamMetadataSet) IterateFilesFullyBefore(before uint64, f func(d *backuppb.DataFileInfo) (shouldBreak bool)) {
	ms.iterateDataFiles(func(d *backuppb.DataFileInfo) (shouldBreak bool) {
		if d.MaxTs >= before {
			return false
		}
		return f(d)
	})
}

// RemoveDataBefore would find files contains only records before the timestamp, mark them as removed from meta,
// and returning their information.
func (ms *StreamMetadataSet) RemoveDataBefore(from uint64) []*backuppb.DataFileInfo {
	removed := []*backuppb.DataFileInfo{}
	for metaPath, m := range ms.metadata {
		remainedDataFiles := make([]*backuppb.DataFileInfo, 0)
		// can we assume those files are sorted to avoid traversing here? (by what?)
		for _, d := range m.Files {
			if d.MaxTs < from {
				removed = append(removed, d)
			} else {
				remainedDataFiles = append(remainedDataFiles, d)
			}
		}
		if len(remainedDataFiles) != len(m.Files) {
			mCopy := *m
			mCopy.Files = remainedDataFiles
			ms.WriteBack(metaPath, &mCopy)
		}
	}
	return removed
}

func (ms *StreamMetadataSet) WriteBack(path string, file *backuppb.Metadata) {
	ms.writeback[path] = file
}

func (ms *StreamMetadataSet) doWriteBackForFile(ctx context.Context, s storage.ExternalStorage, path string) error {
	data, ok := ms.writeback[path]
	if !ok {
		return errors.Annotatef(berrors.ErrInvalidArgument, "There is no write back for path %s", path)
	}
	// If the metadata file contains no data file, remove it due to it is meanless.
	if len(data.Files) == 0 {
		if err := s.DeleteFile(ctx, path); err != nil {
			return errors.Annotatef(err, "failed to remove the empty meta %s", path)
		}
		return nil
	}

	bs, err := data.Marshal()
	if err != nil {
		return errors.Annotatef(err, "failed to marshal the file %s", path)
	}
	return truncateAndWrite(ctx, s, path, bs)
}

func (ms *StreamMetadataSet) DoWriteBack(ctx context.Context, s storage.ExternalStorage) error {
	for path := range ms.writeback {
		if ms.BeforeDoWriteBack != nil && ms.BeforeDoWriteBack(path, ms.metadata[path], ms.writeback[path]) {
			return nil
		}
		err := ms.doWriteBackForFile(ctx, s, path)
		// NOTE: Maybe we'd better roll back all writebacks? (What will happen if roll back fails too?)
		if err != nil {
			return errors.Annotatef(err, "failed to write back file %s", path)
		}

		delete(ms.writeback, path)
	}
	return nil
}

func truncateAndWrite(ctx context.Context, s storage.ExternalStorage, path string, data []byte) error {
	switch s.(type) {
	// Performance hack: the `Write` implemention for S3 and local would truncate the file if it exists.
	case *storage.S3Storage, *storage.LocalStorage:
		if err := s.WriteFile(ctx, path, data); err != nil {
			return errors.Annotatef(err, "failed to save the file %s to %s", path, s.URI())
		}
	default:
		if err := swapAndOverrideFile(ctx, s, path, data); err != nil {
			return errors.Annotatef(err, "failed during rewriting the file at %s in %s", path, s.URI())
		}
	}
	return nil
}

// swapAndOverrideFile is a slow but safe way for overriding a file in the external storage.
// Because there isn't formal definition of `WriteFile` over a existing file, this should be safe in generic external storage.
// It moves the origin file to a swap file and did the file write, finally remove the swap file.
func swapAndOverrideFile(ctx context.Context, s storage.ExternalStorage, path string, data []byte) error {
	ok, err := s.FileExists(ctx, path)
	if err != nil {
		return err
	}
	if !ok {
		return errors.Annotate(berrors.ErrInvalidArgument, "the origin file doesn't exist")
	}

	backup := path + ".override_swap"
	if err := s.Rename(ctx, path, backup); err != nil {
		return err
	}
	if err := s.WriteFile(ctx, path, data); err != nil {
		return err
	}
	if err := s.DeleteFile(ctx, backup); err != nil {
		return err
	}
	return nil
}

const (
	// TruncateSafePointFileName is the filename that the ts(the log have been truncated) is saved into.
	TruncateSafePointFileName = "v1_stream_trancate_safepoint.txt"
)

// GetTSFromFile gets the current truncate safepoint.
// truncate safepoint is the TS used for last truncating:
// which means logs before this TS would probably be deleted or incomplete.
func GetTSFromFile(
	ctx context.Context,
	s storage.ExternalStorage,
	filename string,
) (uint64, error) {
	exists, err := s.FileExists(ctx, filename)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	data, err := s.ReadFile(ctx, filename)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return 0, errors.Annotatef(berrors.ErrInvalidMetaFile, "failed to parse the truncate safepoint")
	}
	return value, nil
}

// SetTSToFile overrides the current truncate safepoint.
// truncate safepoint is the TS used for last truncating:
// which means logs before this TS would probably be deleted or incomplete.
func SetTSToFile(
	ctx context.Context,
	s storage.ExternalStorage,
	safepoint uint64,
	filename string,
) error {
	content := strconv.FormatUint(safepoint, 10)
	return truncateAndWrite(ctx, s, filename, []byte(content))
}
