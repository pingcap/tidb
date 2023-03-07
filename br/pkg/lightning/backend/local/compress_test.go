package local

import (
	"bytes"
	"compress/gzip" // use standard library to verify the result
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompressor(t *testing.T) {
	compressor := &gzipCompressor{}
	require.Equal(t, "gzip", compressor.Type())

	input := make([]byte, 1<<20)
	_, err := rand.Read(input)
	require.NoError(t, err)

	buf := &bytes.Buffer{}
	err = compressor.Do(buf, input)
	require.NoError(t, err)

	compressed := buf.Bytes()
	z, err := gzip.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	uncompressed, err := io.ReadAll(z)
	require.NoError(t, err)
	require.NoError(t, z.Close())

	require.Equal(t, input, uncompressed)
}

func TestDecompressor(t *testing.T) {
	decompressor := &gzipDecompressor{}
	require.Equal(t, "gzip", decompressor.Type())

	input := make([]byte, 1<<20)
	_, err := rand.Read(input)
	require.NoError(t, err)

	buf := &bytes.Buffer{}
	z := gzip.NewWriter(buf)
	_, err = z.Write(input)
	require.NoError(t, err)
	require.NoError(t, z.Close())

	uncompressed, err := decompressor.Do(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	require.Equal(t, input, uncompressed)
}
