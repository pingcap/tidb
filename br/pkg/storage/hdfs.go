// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
)

// HDFSStorage represents HDFS storage.
type HDFSStorage struct {
	remote string
}

func NewHDFSStorage(remote string) *HDFSStorage {
	return &HDFSStorage{
		remote: remote,
	}
}

func getHdfsBin() (string, error) {
	hadoop_home, ok := os.LookupEnv("HADOOP_HOME")
	if !ok {
		return "", errors.Annotatef(berrors.ErrStorageInvalidConfig, "please specify environment variable HADOOP_HOME")
	}
	return filepath.Join(hadoop_home, "bin/hdfs"), nil
}

// WriteFile writes a complete file to storage, similar to os.WriteFile
func (s *HDFSStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	bin, err := getHdfsBin()
	if err != nil {
		return err
	}
	file_path := fmt.Sprintf("%s/%s", s.remote, name)
	cmd := exec.Command(bin, "dfs", "-put", "-", file_path)

	buf := bytes.Buffer{}
	buf.Write(data)
	cmd.Stdin = &buf

	out, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Annotate(err, string(out))
	}
	return nil
}

// ReadFile reads a complete file from storage, similar to os.ReadFile
func (s *HDFSStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	panic("not implemented")
}

// FileExists return true if file exists
func (s *HDFSStorage) FileExists(ctx context.Context, name string) (bool, error) {
	bin, err := getHdfsBin()
	if err != nil {
		return false, err
	}
	file_path := fmt.Sprintf("%s/%s", s.remote, name)
	cmd := exec.Command(bin, "dfs", "-ls", file_path)
	out, err := cmd.CombinedOutput()
	if _, ok := err.(*exec.ExitError); ok {
		return false, nil
	}
	if err != nil {
		return false, errors.Annotate(err, string(out))
	}
	return true, nil
}

// DeleteFile delete the file in storage
func (s *HDFSStorage) DeleteFile(ctx context.Context, name string) error {
	panic("not implemented")
}

// Open a Reader by file path. path is relative path to storage base path
func (s *HDFSStorage) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	panic("not implemented")
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The argument `path` is the file path that can be used in `Open`
// function; the argument `size` is the size in byte of the file determined
// by path.
func (s *HDFSStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(path string, size int64) error) error {
	panic("not implemented")
}

// URI returns the base path as a URI
func (s *HDFSStorage) URI() string {
	return s.remote
}

// Create opens a file writer by path. path is relative path to storage base path
func (s *HDFSStorage) Create(ctx context.Context, path string) (ExternalFileWriter, error) {
	panic("not implemented")
}
