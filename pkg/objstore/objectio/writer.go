// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"bytes"
	"context"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/recording"
)

// EmptyFlusher empty Flusher.
type EmptyFlusher struct{}

// Flush do flush.
func (*EmptyFlusher) Flush() error {
	return nil
}

type interceptBuffer interface {
	io.WriteCloser
	compressedio.Flusher
	Len() int
	Cap() int
	Bytes() []byte
	Reset()
	Compressed() bool
}

func newInterceptBuffer(chunkSize int, compressType compressedio.CompressType) interceptBuffer {
	if compressType == compressedio.NoCompression {
		return newPlainBuffer(chunkSize)
	}
	return compressedio.NewBuffer(chunkSize, compressType)
}

type plainBuffer struct {
	*bytes.Buffer
}

func (*plainBuffer) Flush() error {
	return nil
}

func (*plainBuffer) Close() error {
	return nil
}

func (*plainBuffer) Compressed() bool {
	return false
}

func newPlainBuffer(chunkSize int) *plainBuffer {
	return &plainBuffer{bytes.NewBuffer(make([]byte, 0, chunkSize))}
}

// BufferedWriter is a buffered writer
type BufferedWriter struct {
	buf       interceptBuffer
	writer    Writer
	accessRec *recording.AccessStats
}

// Write implements objstoreapi.Writer.
func (u *BufferedWriter) Write(ctx context.Context, p []byte) (int, error) {
	n, err := u.write0(ctx, p)
	u.accessRec.RecWrite(n)
	return n, errors.Trace(err)
}

func (u *BufferedWriter) write0(ctx context.Context, p []byte) (int, error) {
	bytesWritten := 0
	for u.buf.Len()+len(p) > u.buf.Cap() {
		// We won't fit p in this chunk

		// Is this chunk full?
		chunkToFill := u.buf.Cap() - u.buf.Len()
		if chunkToFill > 0 {
			// It's not full so we write enough of p to fill it
			prewrite := p[0:chunkToFill]
			w, err := u.buf.Write(prewrite)
			bytesWritten += w
			if err != nil {
				return bytesWritten, errors.Trace(err)
			}
			p = p[w:]
			// continue buf because compressed data size may be less than Cap - Len
			if u.buf.Compressed() {
				continue
			}
		}
		_ = u.buf.Flush()
		err := u.uploadChunk(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	w, err := u.buf.Write(p)
	bytesWritten += w
	return bytesWritten, errors.Trace(err)
}

func (u *BufferedWriter) uploadChunk(ctx context.Context) error {
	if u.buf.Len() == 0 {
		return nil
	}
	b := u.buf.Bytes()
	u.buf.Reset()
	_, err := u.writer.Write(ctx, b)
	return errors.Trace(err)
}

// Close implements objstoreapi.Writer.
func (u *BufferedWriter) Close(ctx context.Context) error {
	u.buf.Close()
	err := u.uploadChunk(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return u.writer.Close(ctx)
}

// GetWriter get the underlying writer.
func (u *BufferedWriter) GetWriter() Writer {
	return u.writer
}

// NewUploaderWriter wraps the Writer interface over an uploader.
func NewUploaderWriter(writer Writer, chunkSize int, compressType compressedio.CompressType) Writer {
	return NewBufferedWriter(writer, chunkSize, compressType, nil)
}

// NewBufferedWriter is used to build a buffered writer.
func NewBufferedWriter(writer Writer, chunkSize int, compressType compressedio.CompressType, accessRec *recording.AccessStats) *BufferedWriter {
	return &BufferedWriter{
		writer:    writer,
		buf:       newInterceptBuffer(chunkSize, compressType),
		accessRec: accessRec,
	}
}
