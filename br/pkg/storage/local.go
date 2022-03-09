// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
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

func (l *LocalStorage) DeleteFile(ctx context.Context, name string) error {
	path := filepath.Join(l.base, name)
	return os.Remove(path)
}

// WriteFile writes data to a file to storage.
func (l *LocalStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	// because `os.WriteFile` is not atomic, directly write into it may reset the file
	// to an empty file if write is not finished.
	tmpPath := filepath.Join(l.base, name) + ".tmp"
	if err := os.WriteFile(tmpPath, data, localFilePerm); err != nil {
		return errors.Trace(err)
	}
	if err := os.Rename(tmpPath, filepath.Join(l.base, name)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ReadFile reads the file from the storage and returns the contents.
func (l *LocalStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	path := filepath.Join(l.base, name)
	return os.ReadFile(path)
}

// FileExists implement ExternalStorage.FileExists.
func (l *LocalStorage) FileExists(ctx context.Context, name string) (bool, error) {
	path := filepath.Join(l.base, name)
	return pathExists(path)
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (l *LocalStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	if opt == nil {
		opt = &WalkOption{}
	}
	base := filepath.Join(l.base, opt.SubDir)
	return filepath.Walk(base, func(path string, f os.FileInfo, err error) error {
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
}

// URI returns the base path as an URI with a file:/// prefix.
func (l *LocalStorage) URI() string {
	return LocalURIPrefix + "/" + l.base
}

// Open a Reader by file path, path is a relative path to base path.
func (l *LocalStorage) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	return os.Open(filepath.Join(l.base, path))
}

// Create implements ExternalStorage interface.
func (l *LocalStorage) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	file, err := os.Create(filepath.Join(l.base, name))
	if err != nil {
		return nil, errors.Trace(err)
	}
	buf := bufio.NewWriter(file)
	return newFlushStorageWriter(buf, buf, file), nil
}

// Rename implements ExternalStorage interface.
func (l *LocalStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	return errors.Trace(os.Rename(filepath.Join(l.base, oldFileName), filepath.Join(l.base, newFileName)))
}

func pathExists(_path string) (bool, error) {
	_, err := os.Stat(_path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// NewLocalStorage return a LocalStorage at directory `base`.
//
// export for test.
func NewLocalStorage(base string) (*LocalStorage, error) {
	ok, err := pathExists(base)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		err := mkdirAll(base)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return &LocalStorage{base: base}, nil
}
