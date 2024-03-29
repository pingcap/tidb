package local

import (
	"bytes"
	"compress/gzip" // use standard library to verify the result
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGzipCompressor(t *testing.T) {
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

func TestGzipDecompressor(t *testing.T) {
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

func BenchmarkGzipCompressor(b *testing.B) {
	benchCompressor(b, &gzipCompressor{})
}

func BenchmarkGrpcGzipCompressor(b *testing.B) {
	benchCompressor(b, grpc.NewGZIPCompressor())
}

func benchCompressor(b *testing.B, compressor grpc.Compressor) {
	input := make([]byte, 1<<20)
	_, err := rand.Read(input)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := &bytes.Buffer{}
		err = compressor.Do(buf, input)
		require.NoError(b, err)
	}
}

func BenchmarkGzipDecompressor(b *testing.B) {
	benchDecompressor(b, &gzipDecompressor{})
}

func BenchmarkGrpcGzipDecompressor(b *testing.B) {
	benchDecompressor(b, grpc.NewGZIPDecompressor())
}

func benchDecompressor(b *testing.B, decompressor grpc.Decompressor) {
	input := make([]byte, 1<<20)
	_, err := rand.Read(input)
	require.NoError(b, err)

	buf := &bytes.Buffer{}
	z := gzip.NewWriter(buf)
	_, err = z.Write(input)
	require.NoError(b, err)
	require.NoError(b, z.Close())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := decompressor.Do(bytes.NewReader(buf.Bytes()))
		require.NoError(b, err)
	}
}
