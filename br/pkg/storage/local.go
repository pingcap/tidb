// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	localDirPerm  os.FileMode = 0o777
	localFilePerm os.FileMode = 0o644
	// LocalURIPrefix represents the local storage prefix.
	LocalURIPrefix = "file://"
)

// LocalStorage represents local file system storage.
//
// export for using in tests.
type LocalStorage struct {
	base string
}

// DeleteFile deletes the file.
func (l *LocalStorage) DeleteFile(_ context.Context, name string) *Error {
	path := filepath.Join(l.base, name)
	return NewSimpleError(os.Remove(path))
}

// WriteFile writes data to a file to storage.
func (l *LocalStorage) WriteFile(_ context.Context, name string, data []byte) *Error {
	// because `os.WriteFile` is not atomic, directly write into it may reset the file
	// to an empty file if write is not finished.
	tmpPath := filepath.Join(l.base, name) + ".tmp." + uuid.NewString()
	if err := os.WriteFile(tmpPath, data, localFilePerm); err != nil {
		path := filepath.Dir(tmpPath)
		log.Info("failed to write file, try to mkdir the path", zap.String("path", path))
		exists, existErr := pathExists(path)
		if existErr != nil {
			return NewSimpleError(errors.Annotatef(err, "after failed to write file, failed to check path exists : %v", existErr))
		}
		if exists {
			return NewSimpleError(errors.Trace(err))
		}
		if mkdirErr := mkdirAll(path); mkdirErr != nil {
			return NewSimpleError(errors.Annotatef(err, "after failed to write file, failed to mkdir : %v", mkdirErr))
		}
		if err := os.WriteFile(tmpPath, data, localFilePerm); err != nil {
			return NewSimpleError(errors.Trace(err))
		}
	}
	if err := os.Rename(tmpPath, filepath.Join(l.base, name)); err != nil {
		return NewSimpleError(errors.Trace(err))
	}
	return nil
}

// ReadFile reads the file from the storage and returns the contents.
func (l *LocalStorage) ReadFile(_ context.Context, name string) ([]byte, *Error) {
	path := filepath.Join(l.base, name)
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, NewSimpleError(errors.Trace(err))
	}
	return bs, nil
}

// FileExists implement ExternalStorage.FileExists.
func (l *LocalStorage) FileExists(_ context.Context, name string) (bool, *Error) {
	path := filepath.Join(l.base, name)
	return pathExists(path)
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (l *LocalStorage) WalkDir(_ context.Context, opt *WalkOption, fn func(string, int64) *Error) *Error {
	if opt == nil {
		opt = &WalkOption{}
	}
	base := filepath.Join(l.base, opt.SubDir)
	err := filepath.Walk(base, func(path string, f os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			// if path not exists, we should return nil to continue.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		if f == nil || f.IsDir() {
			return nil
		}
		// in mac osx, the path parameter is absolute path; in linux, the path is relative path to execution base dir,
		// so use Rel to convert to relative path to l.base
		path, _ = filepath.Rel(l.base, path)

		if !strings.HasPrefix(path, opt.ObjPrefix) {
			return nil
		}

		size := f.Size()
		// if not a regular file, we need to use os.stat to get the real file size
		if !f.Mode().IsRegular() {
			stat, err := os.Stat(filepath.Join(l.base, path))
			if err != nil {
				return errors.Trace(err)
			}
			size = stat.Size()
		}
		return fn(path, size)
	})
	return NewSimpleError(errors.Trace(err))
}

// URI returns the base path as an URI with a file:/// prefix.
func (l *LocalStorage) URI() string {
	return LocalURIPrefix + "/" + l.base
}

// Open a Reader by file path, path is a relative path to base path.
func (l *LocalStorage) Open(_ context.Context, path string) (ExternalFileReader, *Error) {
	//nolint: gosec
	reader, err := os.Open(filepath.Join(l.base, path))
	return reader, NewSimpleError(err)
}

// Create implements ExternalStorage interface.
func (l *LocalStorage) Create(_ context.Context, name string) (ExternalFileWriter, *Error) {
	file, err := os.Create(filepath.Join(l.base, name))
	if err != nil {
		return nil, NewSimpleError(errors.Trace(err))
	}
	buf := bufio.NewWriter(file)
	return newFlushStorageWriter(buf, buf, file), nil
}

// Rename implements ExternalStorage interface.
func (l *LocalStorage) Rename(_ context.Context, oldFileName, newFileName string) *Error {
	return NewSimpleError(errors.Trace(os.Rename(filepath.Join(l.base, oldFileName), filepath.Join(l.base, newFileName))))
}

func pathExists(_path string) (bool, *Error) {
	_, err := os.Stat(_path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, NewSimpleError(errors.Trace(err))
	}
	return true, nil
}

// NewLocalStorage return a LocalStorage at directory `base`.
//
// export for test.
func NewLocalStorage(base string) (*LocalStorage, *Error) {
	ok, err := pathExists(base)
	if err != nil {
		return nil, NewSimpleError(errors.Trace(err))
	}
	if !ok {
		err := mkdirAll(base)
		if err != nil {
			return nil, NewSimpleError(errors.Trace(err))
		}
	}
	return &LocalStorage{base: base}, nil
}
