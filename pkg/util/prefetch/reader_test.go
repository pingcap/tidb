// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prefetch

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	source := bytes.NewReader([]byte("01234567890"))
	r := NewReader(io.NopCloser(source), source.Size(), 3)
	buf := make([]byte, 1)
	n, err := r.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	require.EqualValues(t, "0", buf[:n])

	buf = make([]byte, 2)
	n, err = r.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
	require.EqualValues(t, "12", buf[:n])

	buf = make([]byte, 3)
	n, err = r.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 3, n)
	require.EqualValues(t, "345", buf[:n])

	buf = make([]byte, 4)
	n, err = r.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 4, n)
	require.EqualValues(t, "6789", buf[:n])
	n, err = r.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	require.EqualValues(t, "0", buf[:n])
	_, err = r.Read(buf)
	require.ErrorIs(t, err, io.EOF)

	source = bytes.NewReader([]byte("01234567890"))
	r = NewReader(io.NopCloser(source), source.Size(), 3)
	buf = make([]byte, 11)
	n, err = r.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 11, n)
	_, err = r.Read(buf)
	require.ErrorIs(t, err, io.EOF)

	source = bytes.NewReader([]byte("01234"))
	r = NewReader(io.NopCloser(source), source.Size(), 100)
	buf = make([]byte, 11)
	n, err = r.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 5, n)
	_, err = r.Read(buf)
	require.ErrorIs(t, err, io.EOF)
}

type unexpectedEOFReader struct {
}

func (u *unexpectedEOFReader) Read(_ []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

func TestConvertUnexpectedEOF(t *testing.T) {
	r := NewReader(io.NopCloser(&unexpectedEOFReader{}), 10, 10)
	buf := make([]byte, 10)
	_, err := r.Read(buf)
	// prefetch reader should not convert underlying io.ErrUnexpectedEOF to io.EOF
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestCloseBeforeDrainRead(t *testing.T) {
	data := make([]byte, 1024)
	source := bytes.NewReader(data)
	r := NewReader(io.NopCloser(source), source.Size(), 2)
	err := r.Close()
	require.NoError(t, err)
}

type fragmentReader struct {
	data     []byte
	i        atomic.Int64
	fragSize int
}

func (r *fragmentReader) Read(p []byte) (n int, err error) {
	i := int(r.i.Load())
	if i >= len(r.data) {
		return 0, io.EOF
	}
	copySize := min(len(p), r.fragSize)
	n = copy(p, r.data[i:i+copySize])
	r.i.Add(int64(n))
	return
}

func TestFillPrefetchBuffer(t *testing.T) {
	// test fragmentReader behaviour
	fragReader := &fragmentReader{
		data:     []byte("0123456789"),
		fragSize: 3,
	}
	buf := make([]byte, 5)
	n, err := fragReader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "012", string(buf[:n]))

	buf = make([]byte, 1)
	n, err = fragReader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, "3", string(buf[:n]))

	fragReader = &fragmentReader{
		data:     []byte("0123456789"),
		fragSize: 3,
	}
	// when prefetch is 10B, the two internal ping-pong buffers are 5B
	prefetchReader := NewReader(io.NopCloser(fragReader), int64(len(fragReader.data)), 10)
	// when no read happens, one of ping-pong buffer is fully filled
	require.Eventually(t, func() bool {
		return fragReader.i.Load() == 5
	}, time.Second, 10*time.Millisecond)
	// when any small read happens, one of ping-pong buffer is serving the read,
	// another buffer is fully filled
	buf = make([]byte, 2)
	n, err = prefetchReader.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
	require.EqualValues(t, "01", string(buf[:n]))
	require.Eventually(t, func() bool {
		return fragReader.i.Load() == 10
	}, time.Second, 10*time.Millisecond)
}
