// Copyright 2019 PingCAP, Inc.
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

package importer

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type trackingReader struct {
	*bytes.Reader
	size   int64
	closed bool
}

func newTrackingReader(content string) *trackingReader {
	return &trackingReader{
		Reader: bytes.NewReader([]byte(content)),
		size:   int64(len(content)),
	}
}

func (r *trackingReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset > 0 {
		return 0, errors.New("mock seek error")
	}
	return r.Reader.Seek(offset, whence)
}

func (r *trackingReader) Close() error {
	r.closed = true
	return nil
}

func (r *trackingReader) GetFileSize() (int64, error) {
	return r.size, nil
}

type trackingStorage struct {
	reader *trackingReader
}

func (*trackingStorage) WriteFile(context.Context, string, []byte) error  { panic("not implemented") }
func (*trackingStorage) ReadFile(context.Context, string) ([]byte, error) { panic("not implemented") }
func (*trackingStorage) FileExists(context.Context, string) (bool, error) { panic("not implemented") }
func (*trackingStorage) DeleteFile(context.Context, string) error         { panic("not implemented") }
func (s *trackingStorage) Open(context.Context, string, *storeapi.ReaderOption) (objectio.Reader, error) {
	return s.reader, nil
}
func (*trackingStorage) DeleteFiles(context.Context, []string) error { panic("not implemented") }
func (*trackingStorage) WalkDir(context.Context, *storeapi.WalkOption, func(string, int64) error) error {
	panic("not implemented")
}
func (*trackingStorage) URI() string { return "mock://tracking" }
func (*trackingStorage) Create(context.Context, string, *storeapi.WriterOption) (objectio.Writer, error) {
	panic("not implemented")
}
func (*trackingStorage) Rename(context.Context, string, string) error { panic("not implemented") }
func (*trackingStorage) PresignFile(context.Context, string, time.Duration) (string, error) {
	panic("not implemented")
}
func (*trackingStorage) Close() {}

func TestSamplerParserCloseOnError(t *testing.T) {
	newChunk := func() *Chunk {
		return &Chunk{
			Path:         "test.sql",
			FileSize:     16,
			Offset:       1,
			PrevRowIDMax: 1,
		}
	}

	reader := newTrackingReader("INSERT INTO t VALUES (1);\n")
	sampler := &kvSizeSampler{
		cfg: &KVSizeSampleConfig{
			Format: DataFormatSQL,
		},
		dataStore: &trackingStorage{reader: reader},
		logger:    zap.NewNop(),
	}
	_, err := sampler.getParser(context.Background(), newChunk())
	require.Error(t, err)
	require.True(t, reader.closed)

	reader = newTrackingReader("INSERT INTO t VALUES (1);\n")
	ctrl := &LoadDataController{
		Plan:      &Plan{Format: DataFormatSQL},
		dataStore: &trackingStorage{reader: reader},
		logger:    zap.NewNop(),
	}
	_, err = ctrl.getParser(context.Background(), newChunk())
	require.Error(t, err)
	require.True(t, reader.closed)
}
