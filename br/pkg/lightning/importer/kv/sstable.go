// Copyright 2022 PingCAP, Inc.
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

package kv

import (
	"os"

	"github.com/cockroachdb/pebble/sstable"
)

// SSTMeta represents the metadata of a sstable file.
type SSTMeta struct {
	Path string
	Properties
}

// SSTReader reads key-value pairs from a sstable file.
type SSTReader struct {
	r        *sstable.Reader
	iter     sstable.Iterator
	seeked   bool
	startKey []byte
	closed   bool
}

// NewSSTReader creates a new SSTReader from a sstable file.
func NewSSTReader(path string, kr KeyRange) (*SSTReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := sstable.NewReader(f, sstable.ReaderOptions{})
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	iter, err := r.NewIter(nil, kr.EndKey)
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	return &SSTReader{
		r:    r,
		iter: iter,
	}, nil
}

func (sr *SSTReader) Read() (key, val []byte, err error) {
	var internalKey *sstable.InternalKey
	if !sr.seeked {
		sr.seeked = true
		internalKey, val = sr.iter.SeekGE(sr.startKey)
	} else {
		internalKey, val = sr.iter.Next()
	}
	if internalKey == nil {
		return nil, nil, sr.iter.Error()
	} else {
		return internalKey.UserKey, val, sr.iter.Error()
	}
}

func (sr *SSTReader) Close() error {
	if sr.closed {
		return nil
	}
	sr.closed = true
	err := sr.iter.Close()
	if rerr := sr.r.Close(); err == nil {
		err = rerr
	}
	return err
}

// SSTWriter is a writer to write key-value pairs to a sstable file.
type SSTWriter struct {
	path string
	w    *sstable.Writer
}

// NewSSTWriter creates a new SSTWriter that writes key-value pairs to a sstable file.
func NewSSTWriter(path string) (*SSTWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := sstable.NewWriter(f, sstable.WriterOptions{})
	return &SSTWriter{path: path, w: w}, nil
}

func (sw *SSTWriter) Write(key, val []byte) error {
	return sw.w.Set(key, val)
}

func (sw *SSTWriter) Close() error {
	return sw.w.Close()
}
