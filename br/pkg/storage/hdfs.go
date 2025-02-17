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

// NewHDFSStorage creates a new HDFS storage.
func NewHDFSStorage(remote string) *HDFSStorage {
	return &HDFSStorage{
		remote: remote,
	}
}

func getHdfsBin() (string, error) {
	hadoopHome, ok := os.LookupEnv("HADOOP_HOME")
	if !ok {
		return "", errors.Annotatef(berrors.ErrEnvNotSpecified, "please specify environment variable HADOOP_HOME")
	}
	return filepath.Join(hadoopHome, "bin/hdfs"), nil
}

func getLinuxUser() (string, bool) {
	return os.LookupEnv("HADOOP_LINUX_USER")
}

func dfsCommand(args ...string) (*exec.Cmd, error) {
	bin, err := getHdfsBin()
	if err != nil {
		return nil, err
	}
	cmd := []string{}
	user, ok := getLinuxUser()
	if ok {
		cmd = append(cmd, "sudo", "-u", user)
	}
	cmd = append(cmd, bin, "dfs")
	cmd = append(cmd, args...)
	//nolint:gosec
	return exec.Command(cmd[0], cmd[1:]...), nil
}

// WriteFile writes a complete file to storage, similar to os.WriteFile
func (s *HDFSStorage) WriteFile(_ context.Context, name string, data []byte) error {
	filePath := fmt.Sprintf("%s/%s", s.remote, name)
	cmd, err := dfsCommand("-put", "-", filePath)
	if err != nil {
		return err
	}

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
func (*HDFSStorage) ReadFile(_ context.Context, _ string) ([]byte, error) {
	return nil, errors.Annotatef(berrors.ErrUnsupportedOperation, "currently HDFS backend only support rawkv backup")
}

// FileExists return true if file exists
func (s *HDFSStorage) FileExists(_ context.Context, name string) (bool, error) {
	filePath := fmt.Sprintf("%s/%s", s.remote, name)
	cmd, err := dfsCommand("-ls", filePath)
	if err != nil {
		return false, err
	}
	out, err := cmd.CombinedOutput()
	if _, ok := err.(*exec.ExitError); ok {
		// Successfully exit with non-zero value
		return false, nil
	}
	if err != nil {
		return false, errors.Annotate(err, string(out))
	}
	// Successfully exit with zero value
	return true, nil
}

// DeleteFile delete the file in storage
func (*HDFSStorage) DeleteFile(_ context.Context, _ string) error {
	return errors.Annotatef(berrors.ErrUnsupportedOperation, "currently HDFS backend only support rawkv backup")
}

// DeleteFiles deletes files in storage
func (*HDFSStorage) DeleteFiles(_ context.Context, _ []string) error {
	return errors.Annotatef(berrors.ErrUnsupportedOperation, "currently HDFS backend only support rawkv backup")
}

// Open a Reader by file path. path is relative path to storage base path
func (*HDFSStorage) Open(_ context.Context, _ string, _ *ReaderOption) (ExternalFileReader, error) {
	return nil, errors.Annotatef(berrors.ErrUnsupportedOperation, "currently HDFS backend only support rawkv backup")
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The argument `path` is the file path that can be used in `Open`
// function; the argument `size` is the size in byte of the file determined
// by path.
func (*HDFSStorage) WalkDir(_ context.Context, _ *WalkOption, _ func(path string, size int64) error) error {
	return errors.Annotatef(berrors.ErrUnsupportedOperation, "currently HDFS backend only support rawkv backup")
}

// URI returns the base path as a URI
func (s *HDFSStorage) URI() string {
	return s.remote
}

// Create opens a file writer by path. path is relative path to storage base path
func (*HDFSStorage) Create(_ context.Context, _ string, _ *WriterOption) (ExternalFileWriter, error) {
	return nil, errors.Annotatef(berrors.ErrUnsupportedOperation, "currently HDFS backend only support rawkv backup")
}

// Rename a file name from oldFileName to newFileName.
func (*HDFSStorage) Rename(_ context.Context, _, _ string) error {
	return errors.Annotatef(berrors.ErrUnsupportedOperation, "currently HDFS backend only support rawkv backup")
}

// Close implements ExternalStorage interface.
func (*HDFSStorage) Close() {}
