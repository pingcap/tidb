// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
)

type noopStorage struct{}

// DeleteFile delete the file in storage
func (s *noopStorage) DeleteFile(ctx context.Context, name string) error {
	return nil
}

// WriteFile file to storage.
func (*noopStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	return nil
}

// ReadFile storage file.
func (*noopStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return []byte{}, nil
}

// FileExists return true if file exists.
func (*noopStorage) FileExists(ctx context.Context, name string) (bool, error) {
	return false, nil
}

// Open a Reader by file path.
func (*noopStorage) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	return noopReader{}, nil
}

// WalkDir traverse all the files in a dir.
func (*noopStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	return nil
}

func (s *noopStorage) URI() string {
	return "noop:///"
}

// Create implements ExternalStorage interface.
func (*noopStorage) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	return &noopWriter{}, nil
}

// Rename implements ExternalStorage interface.
func (*noopStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	return nil
}

func newNoopStorage() *noopStorage {
	return &noopStorage{}
}

type noopReader struct{}

func (noopReader) Read(p []byte) (n int, err error) {
	return len(p), nil
}

func (noopReader) Close() error {
	return nil
}

func (noopReader) Seek(offset int64, whence int) (int64, error) {
	return offset, nil
}

type noopWriter struct{}

func (noopWriter) Write(ctx context.Context, p []byte) (int, error) {
	return len(p), nil
}

func (noopWriter) Close(ctx context.Context) error {
	return nil
}
