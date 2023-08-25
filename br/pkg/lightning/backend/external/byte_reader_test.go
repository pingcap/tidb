// Copyright 2023 PingCAP, Inc.
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

package external

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

// mockExtStore is only used for test.
type mockExtStore struct {
	src []byte
	idx uint64
}

func (s *mockExtStore) Read(p []byte) (n int, err error) {
	// Read from src to p.
	if s.idx >= uint64(len(s.src)) {
		return 0, io.EOF
	}
	n = copy(p, s.src[s.idx:])
	s.idx += uint64(n)
	return n, nil
}

func (s *mockExtStore) Seek(_ int64, _ int) (int64, error) {
	return 0, errors.Errorf("unsupported operation")
}

func (s *mockExtStore) Close() error {
	return nil
}

func TestByteReader(t *testing.T) {
	// Test basic next() usage.
	br, err := newByteReader(context.Background(), &mockExtStore{src: []byte("abcde")}, 3)
	require.NoError(t, err)
	x := br.next(1)
	require.Equal(t, 1, len(x))
	require.Equal(t, byte('a'), x[0])
	x = br.next(2)
	require.Equal(t, 2, len(x))
	require.Equal(t, byte('b'), x[0])
	require.Equal(t, byte('c'), x[1])
	require.NoError(t, br.Close())

	// Test basic readNBytes() usage.
	br, err = newByteReader(context.Background(), &mockExtStore{src: []byte("abcde")}, 3)
	require.NoError(t, err)
	y, err := br.readNBytes(2)
	require.NoError(t, err)
	x = *y
	require.Equal(t, 2, len(x))
	require.Equal(t, byte('a'), x[0])
	require.Equal(t, byte('b'), x[1])
	require.NoError(t, br.Close())

	br, err = newByteReader(context.Background(), &mockExtStore{src: []byte("abcde")}, 3)
	require.NoError(t, err)
	y, err = br.readNBytes(5) // Read all the data.
	require.NoError(t, err)
	x = *y
	require.Equal(t, 5, len(x))
	require.Equal(t, byte('e'), x[4])
	require.NoError(t, br.Close())

	br, err = newByteReader(context.Background(), &mockExtStore{src: []byte("abcde")}, 3)
	require.NoError(t, err)
	_, err = br.readNBytes(7) // EOF
	require.Error(t, err)

	ms := &mockExtStore{src: []byte("abcdef")}
	br, err = newByteReader(context.Background(), ms, 2)
	require.NoError(t, err)
	y, err = br.readNBytes(3)
	require.NoError(t, err)
	// Pollute mockExtStore to verify if the slice is not affected.
	copy(ms.src, []byte("xyz"))
	x = *y
	require.Equal(t, 3, len(x))
	require.Equal(t, byte('c'), x[2])
	require.NoError(t, br.Close())

	ms = &mockExtStore{src: []byte("abcdef")}
	br, err = newByteReader(context.Background(), ms, 2)
	require.NoError(t, err)
	y, err = br.readNBytes(2)
	require.NoError(t, err)
	// Pollute mockExtStore to verify if the slice is not affected.
	copy(ms.src, []byte("xyz"))
	x = *y
	require.Equal(t, 2, len(x))
	require.Equal(t, byte('b'), x[1])
	br.reset()
	require.NoError(t, br.Close())
}

func TestByteReaderClone(t *testing.T) {
	ms := &mockExtStore{src: []byte("0123456789")}
	br, err := newByteReader(context.Background(), ms, 4)
	require.NoError(t, err)
	y1, err := br.readNBytes(2)
	require.NoError(t, err)
	y2, err := br.readNBytes(1)
	require.NoError(t, err)
	x1, x2 := *y1, *y2
	require.Len(t, x1, 2)
	require.Len(t, x2, 1)
	require.Equal(t, byte('0'), x1[0])
	require.Equal(t, byte('2'), x2[0])
	require.NoError(t, br.reload()) // Perform a reload to overwrite buffer.
	x1, x2 = *y1, *y2
	require.Len(t, x1, 2)
	require.Len(t, x2, 1)
	require.Equal(t, byte('4'), x1[0]) // Verify if the buffer is overwritten.
	require.Equal(t, byte('6'), x2[0])
	require.NoError(t, br.Close())

	ms = &mockExtStore{src: []byte("0123456789")}
	br, err = newByteReader(context.Background(), ms, 4)
	require.NoError(t, err)
	y1, err = br.readNBytes(2)
	require.NoError(t, err)
	y2, err = br.readNBytes(1)
	require.NoError(t, err)
	x1, x2 = *y1, *y2
	require.Len(t, x1, 2)
	require.Len(t, x2, 1)
	require.Equal(t, byte('0'), x1[0])
	require.Equal(t, byte('2'), x2[0])
	br.cloneSlices()
	require.NoError(t, br.reload()) // Perform a reload to overwrite buffer.
	x1, x2 = *y1, *y2
	require.Len(t, x1, 2)
	require.Len(t, x2, 1)
	require.Equal(t, byte('0'), x1[0]) // Verify if the buffer is NOT overwritten.
	require.Equal(t, byte('2'), x2[0])
	require.NoError(t, br.Close())
}

func TestByteReaderAuxBuf(t *testing.T) {
	ms := &mockExtStore{src: []byte("0123456789")}
	br, err := newByteReader(context.Background(), ms, 1)
	require.NoError(t, err)
	y1, err := br.readNBytes(1)
	require.NoError(t, err)
	y2, err := br.readNBytes(2)
	require.NoError(t, err)
	require.Equal(t, []byte("0"), *y1)
	require.Equal(t, []byte("12"), *y2)

	y3, err := br.readNBytes(1)
	require.NoError(t, err)
	y4, err := br.readNBytes(2)
	require.NoError(t, err)
	require.Equal(t, []byte("3"), *y3)
	require.Equal(t, []byte("45"), *y4)
	require.Equal(t, []byte("0"), *y1)
	require.Equal(t, []byte("12"), *y2)
}

func TestReset(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	src := make([]byte, 256)
	for i := range src {
		src[i] = byte(i)
	}
	ms := &mockExtStore{src: src}
	bufSize := rand.Intn(256)
	br, err := newByteReader(context.Background(), ms, bufSize)
	require.NoError(t, err)
	end := 0
	toCheck := make([]*[]byte, 0, 10)
	for end < len(src) {
		n := rand.Intn(len(src) - end)
		if n == 0 {
			n = 1
		}
		y, err := br.readNBytes(n)
		require.NoError(t, err)
		toCheck = append(toCheck, y)
		end += n

		l := end
		r := end
		for i := len(toCheck) - 1; i >= 0; i-- {
			l -= len(*toCheck[i])
			require.Equal(t, src[l:r], *toCheck[i])
			r = l
		}

		if rand.Intn(2) == 0 {
			br.reset()
			toCheck = toCheck[:0]
		}
	}
	_, err = br.readNBytes(1)
	require.Equal(t, io.EOF, err)
}

func TestUnexpectedEOF(t *testing.T) {
	ms := &mockExtStore{src: []byte("0123456789")}
	br, err := newByteReader(context.Background(), ms, 3)
	require.NoError(t, err)
	_, err = br.readNBytes(100)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestEmptyContent(t *testing.T) {
	ms := &mockExtStore{src: []byte{}}
	_, err := newByteReader(context.Background(), ms, 100)
	require.Equal(t, io.EOF, err)
}
