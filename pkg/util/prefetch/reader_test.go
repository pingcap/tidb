// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

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
}
