// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package pkdbspfreshbulk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

// streamReadStorage isolates S3 ReadFile's io.ReadAll allocation policy from
// network noise. Open retains the same immutable data without copying it.
type streamReadStorage struct {
	storage.ExternalStorage
	data []byte
}

func (s *streamReadStorage) ReadFile(context.Context, string) ([]byte, error) {
	return io.ReadAll(bytes.NewReader(s.data))
}

func (s *streamReadStorage) Open(context.Context, string, *storage.ReaderOption) (storage.ExternalFileReader, error) {
	return &sizedObjectReader{Reader: bytes.NewReader(s.data), size: int64(len(s.data))}, nil
}

type sizedObjectReader struct {
	*bytes.Reader
	size int64
}

func (r *sizedObjectReader) GetFileSize() (int64, error) { return r.size, nil }
func (*sizedObjectReader) Close() error                  { return nil }

// BenchmarkReadObject measures bounded allocation on a self-contained object.
func BenchmarkReadObject(b *testing.B) {
	ctx := context.Background()
	key := "object"
	input := make([]byte, 64<<20)
	for i := range input {
		input[i] = byte(i * 17)
	}
	store := &streamReadStorage{data: input}
	data, err := ReadObject(ctx, store, key)
	require.NoError(b, err)
	expected := sha256.Sum256(data)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		data, err := ReadObject(ctx, store, key)
		require.NoError(b, err)
		require.Equal(b, expected, sha256.Sum256(data))
	}
	b.StopTimer()
	b.Logf("object_sha256=%x", expected)
}

// Compare the final read/authenticate contract with identical immutable input.
func BenchmarkPinnedObjectPipeline(b *testing.B) {
	data := bytes.Repeat([]byte{17}, 64<<20)
	ref := NewManifestRef("s3://bucket/object", data)
	store := &streamReadStorage{data: data}
	for _, pipeline := range []bool{false, true} {
		b.Run(fmt.Sprintf("pipeline=%t", pipeline), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for range b.N {
				reader, err := OpenObject(context.Background(), store, "object")
				require.NoError(b, err)
				var got []byte
				if pipeline {
					got, err = reader.ReadPinned(context.Background(), ref)
				} else {
					got, err = reader.ReadAll(context.Background())
					if err == nil {
						err = ref.VerifyBytes(got)
					}
				}
				require.NoError(b, reader.Close())
				require.NoError(b, err)
				require.Len(b, got, len(data))
			}
		})
	}
}
