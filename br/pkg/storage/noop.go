// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
)

type noopStorage struct{}

// DeleteFile delete the file in storage
func (*noopStorage) DeleteFile(_ context.Context, _ string) error {
	return nil
}

// WriteFile file to storage.
func (*noopStorage) WriteFile(_ context.Context, _ string, _ []byte) error {
	return nil
}

// ReadFile storage file.
func (*noopStorage) ReadFile(_ context.Context, _ string) ([]byte, error) {
	return []byte{}, nil
}

// FileExists return true if file exists.
func (*noopStorage) FileExists(_ context.Context, _ string) (bool, error) {
	return false, nil
}

// Open a Reader by file path.
func (*noopStorage) Open(_ context.Context, _ string) (ExternalFileReader, error) {
	return noopReader{}, nil
}

// WalkDir traverse all the files in a dir.
func (*noopStorage) WalkDir(_ context.Context, _ *WalkOption, _ func(string, int64) error) error {
	return nil
}

func (*noopStorage) URI() string {
	return "noop:///"
}

// Create implements ExternalStorage interface.
func (*noopStorage) Create(_ context.Context, _ string) (ExternalFileWriter, error) {
	return &noopWriter{}, nil
}

// Rename implements ExternalStorage interface.
func (*noopStorage) Rename(_ context.Context, _, _ string) error {
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

func (noopReader) Seek(offset int64, _ int) (int64, error) {
	return offset, nil
}

type noopWriter struct{}

func (noopWriter) Write(_ context.Context, p []byte) (int, error) {
	return len(p), nil
}

func (noopWriter) Close(_ context.Context) error {
	return nil
}
