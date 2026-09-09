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
	"context"
	"io"
)

// Reader represents the streaming external file reader.
type Reader interface {
	io.ReadSeekCloser
	// GetFileSize returns the file size.
	GetFileSize() (int64, error)
}

// Writer represents the streaming external file writer.
type Writer interface {
	// Write writes to buffer and if chunk is filled will upload it
	Write(ctx context.Context, p []byte) (int, error)
	// Close writes final chunk and completes the upload
	Close(ctx context.Context) error
}

// NewIOWriter adapts a context-carrying Writer to io.Writer, binding ctx once so
// it can be handed to writers that expect the standard io.Writer interface.
func NewIOWriter(ctx context.Context, w Writer) io.Writer {
	return &ioWriter{ctx: ctx, w: w}
}

type ioWriter struct {
	ctx context.Context
	w   Writer
}

func (a *ioWriter) Write(p []byte) (int, error) {
	return a.w.Write(a.ctx, p)
}
