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
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestImmutableObjectDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()
	const key = "build-demo/base.spfmvp"
	first := []byte("first")
	if err := WriteImmutableObject(ctx, store, key, first); err != nil {
		t.Fatal(err)
	}
	if err := WriteImmutableObject(ctx, store, key, []byte("second")); err == nil {
		t.Fatal("expected second immutable write to fail")
	}
	stored, err := ReadObject(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, first) {
		t.Fatalf("immutable object changed to %q", stored)
	}
}

type changingObjectStorage struct {
	storage.ExternalStorage
	data   []byte
	size   int64
	closed bool
}

type closingObjectReader struct {
	*sizedObjectReader
	closed *bool
}

func (r *closingObjectReader) Close() error { *r.closed = true; return nil }

func (s *changingObjectStorage) Open(context.Context, string, *storage.ReaderOption) (storage.ExternalFileReader, error) {
	return &closingObjectReader{
		sizedObjectReader: &sizedObjectReader{Reader: bytes.NewReader(s.data), size: s.size},
		closed:            &s.closed,
	}, nil
}

func TestReadObjectRequiresExactSizeAndClosesReader(t *testing.T) {
	for _, tc := range []struct {
		name    string
		data    string
		size    int64
		wantErr string
	}{
		{"exact", "hello", 5, ""},
		{"empty", "", 0, ""},
		{"short", "hello", 6, "unexpected EOF"},
		{"growing", "hello", 4, "changed length"},
		{"invalid size", "hello", -1, "memory bounds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &changingObjectStorage{data: []byte(tc.data), size: tc.size}
			got, err := ReadObject(context.Background(), store, "object")
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.data, string(got))
			} else {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, got)
			}
			require.True(t, store.closed)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &changingObjectStorage{data: []byte("hello"), size: 5}
	_, err := ReadObject(ctx, store, "object")
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, store.closed)
}

func TestReadPinnedAuthenticatesAndJoinsEveryRead(t *testing.T) {
	for _, size := range []int{0, 31, objectHashBlockBytes, objectHashBlockBytes*2 + 17} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := bytes.Repeat([]byte{23}, size)
			ref := NewManifestRef("s3://bucket/object", data)
			for _, mode := range []string{"exact", "bad sha", "truncated", "growing", "cancelled"} {
				t.Run(mode, func(t *testing.T) {
					store := &changingObjectStorage{data: data, size: int64(len(data))}
					pin := ref
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					switch mode {
					case "bad sha":
						pin.SHA256 = strings.Repeat("0", 64)
					case "truncated":
						store.size++
					case "growing":
						store.data = append(bytes.Clone(data), 1)
					case "cancelled":
						cancel()
					}
					reader, err := OpenObject(ctx, store, "object")
					require.NoError(t, err)
					got, err := reader.ReadPinned(ctx, pin)
					require.NoError(t, reader.Close())
					require.True(t, store.closed)
					if mode == "exact" {
						require.NoError(t, err)
						require.Equal(t, data, got)
					} else {
						require.Error(t, err)
						require.Nil(t, got)
						if mode == "bad sha" {
							require.ErrorContains(t, err, "SHA-256")
						}
						if mode == "cancelled" {
							require.ErrorIs(t, err, context.Canceled)
						}
					}
				})
			}
		})
	}
}

func TestSortedRunPrefetchAuthenticatesSameStream(t *testing.T) {
	for _, large := range []bool{false, true} {
		header, records := goSortedRunSample()
		if large {
			records[0].Value = bytes.Repeat([]byte{7}, objectHashBlockBytes+17)
		}
		encoded, err := EncodeSortedRun(header, records)
		require.NoError(t, err)
		for _, mode := range []string{"exact", "bad sha", "truncated", "cancelled"} {
			store := &changingObjectStorage{data: encoded, size: int64(len(encoded))}
			if mode == "truncated" {
				store.data = encoded[:len(encoded)-1]
			}
			reader, err := OpenObject(context.Background(), store, "run")
			require.NoError(t, err)
			count, err := reader.PrefetchSortedRunRecordCount(context.Background())
			require.NoError(t, err)
			require.EqualValues(t, len(records), count)
			ref := NewManifestRef("s3://bucket/run", encoded)
			if mode == "bad sha" {
				ref.SHA256 = strings.Repeat("0", 64)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if mode == "cancelled" {
				cancel()
			}
			actual, err := reader.ReadPinned(ctx, ref)
			cancel()
			require.NoError(t, reader.Close())
			require.True(t, store.closed)
			if mode == "exact" {
				require.NoError(t, err)
				require.Equal(t, encoded, actual)
				_, decoded, err := DecodeSortedRun(actual)
				require.NoError(t, err)
				require.Equal(t, records, decoded)
			} else {
				require.Error(t, err)
				require.Nil(t, actual)
			}
		}
	}
}

func TestSortedRunPrefetchRejectsInvalidAdmissionMetadata(t *testing.T) {
	header, records := goSortedRunSample()
	encoded, err := EncodeSortedRun(header, records)
	require.NoError(t, err)
	for _, mode := range []string{"magic", "kind", "header size", "payload size", "zero count", "huge count"} {
		data := bytes.Clone(encoded)
		switch mode {
		case "magic":
			data[0] = 0
		case "kind":
			binary.LittleEndian.PutUint16(data[8:10], KindBaseFragment)
		case "header size":
			binary.LittleEndian.PutUint32(data[10:14], maxHeaderLen+1)
		case "payload size":
			binary.LittleEndian.PutUint64(data[14:22], ^uint64(0))
		default:
			env, err := decodeEnvelope(encoded)
			require.NoError(t, err)
			bad := header
			if mode == "zero count" {
				bad.RecordCount = 0
			} else {
				bad.RecordCount = ^uint64(0)
			}
			data = rawEnvelopeJSON(t, KindSortedRun, bad, env.payload)
		}
		store := &changingObjectStorage{data: data, size: int64(len(data))}
		reader, err := OpenObject(context.Background(), store, "run")
		require.NoError(t, err)
		_, err = reader.PrefetchSortedRunRecordCount(context.Background())
		require.Error(t, err, mode)
		require.NoError(t, reader.Close())
	}
}
