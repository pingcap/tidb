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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	source := bytes.NewReader([]byte("01234567890"))
	r := NewReader(io.NopCloser(source), 3)
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
	r = NewReader(io.NopCloser(source), 3)
	buf = make([]byte, 11)
	n, err = r.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 11, n)
	_, err = r.Read(buf)
	require.ErrorIs(t, err, io.EOF)

	source = bytes.NewReader([]byte("01234"))
	r = NewReader(io.NopCloser(source), 100)
	buf = make([]byte, 11)
	n, err = r.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 5, n)
	_, err = r.Read(buf)
	require.ErrorIs(t, err, io.EOF)
}

func TestCloseBeforeDrainRead(t *testing.T) {
	data := make([]byte, 1024)
	r := NewReader(io.NopCloser(bytes.NewReader(data)), 2)
	err := r.Close()
	require.NoError(t, err)
}
