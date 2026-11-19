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

package packetio

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	server_err "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/util/israce"
	"github.com/stretchr/testify/require"
)

func BenchmarkPacketIOWrite(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var outBuffer bytes.Buffer
		pkt := &PacketIO{bufWriter: bufio.NewWriter(&outBuffer)}
		_ = pkt.WritePacket([]byte{0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x31, 0x30, 0x38, 0x0, 0xfe})
	}
}

func TestPacketIOWrite(t *testing.T) {
	// Test write one packet
	var outBuffer bytes.Buffer
	pkt := &PacketIO{bufWriter: bufio.NewWriter(&outBuffer)}
	err := pkt.WritePacket([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03})
	require.NoError(t, err)
	err = pkt.Flush()
	require.NoError(t, err)
	require.Equal(t, []byte{0x03, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03}, outBuffer.Bytes())

	// Test write more than one packet
	outBuffer.Reset()
	largeInput := make([]byte, mysql.MaxPayloadLen+4)
	pkt = &PacketIO{bufWriter: bufio.NewWriter(&outBuffer)}
	err = pkt.WritePacket(largeInput)
	require.NoError(t, err)
	err = pkt.Flush()
	require.NoError(t, err)
	res := outBuffer.Bytes()
	require.Equal(t, byte(0xff), res[0])
	require.Equal(t, byte(0xff), res[1])
	require.Equal(t, byte(0xff), res[2])
	require.Equal(t, byte(0), res[3])
}

func TestPacketIOWriteCompressed(t *testing.T) {
	var testdata, outBuffer bytes.Buffer

	seq := uint8(0)
	pkt := &PacketIO{
		bufWriter:            bufio.NewWriter(&outBuffer),
		compressionAlgorithm: mysql.CompressionZlib,
		compressedWriter:     newCompressedWriter(&testdata, mysql.CompressionZlib, &seq),
	}

	payload := bytes.Repeat([]byte{'A'}, 16*1024*1024)
	err := pkt.WritePacket(payload)
	require.NoError(t, err)

	err = pkt.Flush()
	require.NoError(t, err)

	packetNr := []byte{0x0}
	uncompressedLength := []byte{0x0, 0x0, 0x10} // 1048576 bytes

	require.Equal(t, packetNr, testdata.Bytes()[3:4])
	require.Equal(t, uncompressedLength, testdata.Bytes()[4:7])
	require.Equal(t, payload[:1048576], inflate(t, testdata.Bytes()))
}

func inflate(t *testing.T, packet []byte) []byte {
	compressedLength := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16
	compressed := bytes.NewReader(packet[7 : 7+compressedLength])
	r, err := zlib.NewReader(compressed)
	require.NoError(t, err)
	decoded, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Zero(t, compressed.Len())
	return decoded
}

func unzstd(t *testing.T, packet []byte) []byte {
	compressedLength := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(packet[7:7+compressedLength], nil)
	require.NoError(t, err)
	return decoded
}

func TestPacketIORead(t *testing.T) {
	t.Run("uncompressed", func(t *testing.T) {
		var inBuffer bytes.Buffer
		_, err := inBuffer.Write([]byte{0x01, 0x00, 0x00, 0x00, 0x01})
		require.NoError(t, err)
		// Test read one packet
		brc := util.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
		pkt := New(brc)
		readBytes, err := pkt.ReadPacket()
		require.NoError(t, err)
		require.Equal(t, uint8(1), pkt.sequence)
		require.Equal(t, []byte{0x01}, readBytes)

		inBuffer.Reset()
		buf := make([]byte, mysql.MaxPayloadLen+9)
		buf[0] = 0xff
		buf[1] = 0xff
		buf[2] = 0xff
		buf[3] = 0
		buf[2+mysql.MaxPayloadLen] = 0x00
		buf[3+mysql.MaxPayloadLen] = 0x00
		buf[4+mysql.MaxPayloadLen] = 0x01
		buf[7+mysql.MaxPayloadLen] = 0x01
		buf[8+mysql.MaxPayloadLen] = 0x0a

		_, err = inBuffer.Write(buf)
		require.NoError(t, err)
		// Test read multiple packets
		brc = util.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
		pkt = New(brc)
		readBytes, err = pkt.ReadPacket()
		require.NoError(t, err)
		require.Equal(t, uint8(2), pkt.sequence)
		require.Equal(t, mysql.MaxPayloadLen+1, len(readBytes))
		require.Equal(t, byte(0x0a), readBytes[mysql.MaxPayloadLen])
	})
	t.Run("compressed_short", func(t *testing.T) {
		var inBuffer bytes.Buffer
		_, err := inBuffer.Write([]byte{0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x23, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x73, 0x65, 0x6c, 0x65,
			0x63, 0x74, 0x20, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f,
			0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x6c,
			0x69, 0x6d, 0x69, 0x74, 0x20, 0x31})

		require.NoError(t, err)
		// Test read one packet
		brc := util.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
		pkt := New(brc)
		pkt.SetCompressionAlgorithm(mysql.CompressionZlib)
		readBytes, err := pkt.ReadPacket()
		require.NoError(t, err)
		require.Equal(t, uint8(1), pkt.sequence)

		// 03 00 01 73 65 6c 65 63  74 20 40 40 76 65 72 73  |...select @@vers|
		// 69 6f 6e 5f 63 6f 6d 6d  65 6e 74 20 6c 69 6d 69  |ion_comment limi|
		// 74 20 31                                          |t 1|
		expected := []byte{0x3, 0x0, 0x1, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20,
			0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63,
			0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69,
			0x74, 0x20, 0x31}

		require.Equal(t, expected, readBytes)
	})
	t.Run("zlib", func(t *testing.T) {
		var inBuffer bytes.Buffer

		// Header: 7 bytes (compressed_length<3>, packetnr<1>, compressed_length<3>)
		// Payload: Starting with the magic nr for zlib: 0x78
		_, err := inBuffer.Write([]byte{0x39, 0x00, 0x00, 0x00, 0x49, 0x00, 0x00,
			0x78, 0x5e, 0x73, 0x65, 0x60, 0x60, 0x60, 0x0e, 0x76, 0xf5, 0x71,
			0x75, 0x0e, 0x51, 0x30, 0x54, 0xd0, 0xd7, 0x52, 0x48, 0x4c, 0x4a,
			0x4e, 0x49, 0x4d, 0x4b, 0xcf, 0xc8, 0xcc, 0xca, 0xce, 0xc9, 0xcd,
			0xcb, 0x2f, 0x28, 0x2c, 0x2a, 0x2e, 0x29, 0x2d, 0x2b, 0xaf, 0xa8,
			0xac, 0x8a, 0xc7, 0x2d, 0xa5, 0xa0, 0xa5, 0x0f, 0x00, 0x59, 0xd8,
			0x1a, 0x09})

		require.NoError(t, err)
		// Test read one packet
		brc := util.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
		pkt := New(brc)
		pkt.SetCompressionAlgorithm(mysql.CompressionZlib)
		readBytes, err := pkt.ReadPacket()
		require.NoError(t, err)
		require.Equal(t, uint8(1), pkt.sequence)

		// 03 53 45 4c 45 43 54 20  31 20 2f 2a 20 61 62 63  |.SELECT 1 /* abc|
		// 64 65 66 67 68 69 6a 6b  6c 6d 6e 6f 70 71 72 73  |defghijklmnopqrs|
		// 74 75 76 77 78 79 7a 5f  61 62 63 64 65 66 67 68  |tuvwxyz_abcdefgh|
		// 69 6a 6b 6c 6d 6e 6f 70  71 72 73 74 75 76 77 78  |ijklmnopqrstuvwx|
		// 79 7a 20 2a 2f                                    |yz */|

		expected := []byte{0x3, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x31, 0x20,
			0x2f, 0x2a, 0x20, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
			0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73,
			0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x5f, 0x61, 0x62, 0x63,
			0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e,
			0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,
			0x7a, 0x20, 0x2a, 0x2f}

		require.Equal(t, expected, readBytes)
	})
	t.Run("zstd", func(t *testing.T) {
		var inBuffer bytes.Buffer
		// Header: 7 bytes (compressed_length<3>, packetnr<1>, compressed_length<3>)
		// Payload: Starting with the magic nr for zstd: 0x28, 0xb5, 0x2f, 0xfd
		_, err := inBuffer.Write([]byte{
			0x40, 0x00, 0x00, 0x00, 0x49, 0x00, 0x00, 0x28, 0xb5, 0x2f, 0xfd, 0x20,
			0x49, 0xbd, 0x01, 0x00, 0xf4, 0x02, 0x45, 0x00, 0x00, 0x00, 0x03, 0x53,
			0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x31, 0x20, 0x2f, 0x2a, 0x20, 0x61,
			0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d,
			0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,
			0x7a, 0x5f, 0x20, 0x2a, 0x2f, 0x01, 0x00, 0x74, 0x7b, 0x96, 0x01})

		require.NoError(t, err)
		// Test read one packet
		brc := util.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
		pkt := New(brc)
		pkt.SetCompressionAlgorithm(mysql.CompressionZstd)
		readBytes, err := pkt.ReadPacket()
		require.NoError(t, err)
		require.Equal(t, uint8(1), pkt.sequence)

		// 03 53 45 4c 45 43 54 20  31 20 2f 2a 20 61 62 63  |.SELECT 1 /* abc|
		// 64 65 66 67 68 69 6a 6b  6c 6d 6e 6f 70 71 72 73  |defghijklmnopqrs|
		// 74 75 76 77 78 79 7a 5f  61 62 63 64 65 66 67 68  |tuvwxyz_abcdefgh|
		// 69 6a 6b 6c 6d 6e 6f 70  71 72 73 74 75 76 77 78  |ijklmnopqrstuvwx|
		// 79 7a 20 2a 2f                                    |yz */|

		expected := []byte{0x3, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x31, 0x20,
			0x2f, 0x2a, 0x20, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
			0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73,
			0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x5f, 0x61, 0x62, 0x63,
			0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e,
			0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,
			0x7a, 0x20, 0x2a, 0x2f}

		require.Equal(t, expected, readBytes)
	})
}

// Small payloads of less than minCompressLength (50 bytes) don't get actually
// compressed, but have the same header as a compressed packet.
//
// Header:
// 0a 00 00   Compressed length: 10
// 00         Packetnr: 0
// 00 00 00   Uncompressed length: 0 (meaning not compressed)
//
// Payload:
// 74 65 73 74 5f 73 68 6f 72 74  test_short
func TestCompressedWriterShort(t *testing.T) {
	var testdata bytes.Buffer
	payload := []byte("test_short")
	seq := uint8(0)

	cw := newCompressedWriter(&testdata, mysql.CompressionZlib, &seq)
	cw.Write(payload)
	cw.Flush()

	// Test Header
	compressedLength := []byte{0xa, 0x0, 0x0}
	packetNr := []byte{0x0}
	uncompressedLength := []byte{0x0, 0x0, 0x0}
	require.Equal(t, compressedLength, testdata.Bytes()[:3])
	require.Equal(t, packetNr, testdata.Bytes()[3:4])
	require.Equal(t, uncompressedLength, testdata.Bytes()[4:7])

	// Payload, making sure it isn't compressed.
	require.Equal(t, payload, testdata.Bytes()[7:])
}

func TestCompressedWriterLong(t *testing.T) {
	t.Run("zlib", func(t *testing.T) {
		var testdata bytes.Buffer
		payload := bytes.Repeat([]byte("test_zlib "), 200)
		seq := uint8(0)

		cw := newCompressedWriter(&testdata, mysql.CompressionZlib, &seq)
		cw.Write(payload)
		cw.Flush()

		// Header:
		// xx xx xx   Compressed length: the rest of the packet
		// 00         Packetnr: 0
		// d0 07 00   Uncompressed length: 2000
		compressedLength := testdata.Len() - 7
		require.Less(t, compressedLength, len(payload))
		require.Equal(t, []byte{byte(compressedLength), byte(compressedLength >> 8), 0x0}, testdata.Bytes()[:3])
		require.Equal(t, []byte{0x0}, testdata.Bytes()[3:4])
		require.Equal(t, []byte{0xd0, 0x07, 0x0}, testdata.Bytes()[4:7])

		require.Equal(t, payload, inflate(t, testdata.Bytes()))
	})

	t.Run("zstd", func(t *testing.T) {
		var testdata bytes.Buffer
		payload := bytes.Repeat([]byte("test_zstd "), 200)
		seq := uint8(0)

		cw := newCompressedWriter(&testdata, mysql.CompressionZstd, &seq)
		cw.Write(payload)
		cw.Flush()

		// Header:
		// xx xx xx   Compressed length: the rest of the packet
		// 00         Packetnr: 0
		// d0 07 00   Uncompressed length: 2000
		compressedLength := testdata.Len() - 7
		require.Less(t, compressedLength, len(payload))
		require.Equal(t, []byte{byte(compressedLength), byte(compressedLength >> 8), 0x0}, testdata.Bytes()[:3])
		require.Equal(t, []byte{0x0}, testdata.Bytes()[3:4])
		require.Equal(t, []byte{0xd0, 0x07, 0x0}, testdata.Bytes()[4:7])

		// Payload:
		d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
		require.NoError(t, err)
		decoded, err := d.DecodeAll(testdata.Bytes()[7:], nil)
		require.NoError(t, err)
		require.Equal(t, payload, decoded)
	})
}

// TestCompressedReaderShort test a compressed protocol packet that has an uncompressed
// length of 0, which means the actual payload isn't compressed.
func TestCompressedReaderShort(t *testing.T) {
	// payload: 7 bytes compressed header, 37 bytes uncompressed payload
	payload := []byte{0x25, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x21, 0x0, 0x0, 0x0, 0x3,
		0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40, 0x76, 0x65, 0x72,
		0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74,
		0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31}
	r := bytes.NewReader(payload)
	seq := uint8(0)
	cr := newCompressedReader(r, mysql.CompressionZlib, &seq)

	// Read 4 byte header from the payload. This is the regular packet header,
	// not the compressed header.
	header := make([]byte, 4)
	_, err := cr.Read(header)
	require.NoError(t, err)
	require.Equal(t, []byte{0x21, 0x0, 0x0, 0x0}, header)

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	sequence := header[3]
	require.Equal(t, 33, length)
	require.Equal(t, uint8(0), sequence)

	data := make([]byte, length)
	_, err = cr.Read(data)
	require.NoError(t, err)
	expected := []byte{0x3, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40,
		0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d,
		0x65, 0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31}
	require.Equal(t, expected, data)
}

func TestCompressedReaderLong(t *testing.T) {
	t.Run("zlib", func(t *testing.T) {
		payload := []byte{0x19, 0x0, 0x0, 0x0, 0x9c, 0x0, 0x0, 0x78, 0x5e, 0x9b,
			0xc1, 0xc0, 0xc0, 0xc0, 0x1c, 0xec, 0xea, 0xe3, 0xea, 0x1c, 0xa2,
			0xa0, 0xe4, 0x38, 0xa8, 0x80, 0x12, 0x0, 0xbe, 0xe6, 0x26, 0xce}
		r := bytes.NewReader(payload)
		seq := uint8(0)
		cr := newCompressedReader(r, mysql.CompressionZlib, &seq)
		header := make([]byte, 4)
		_, err := cr.Read(header)
		require.NoError(t, err)
		require.Equal(t, []byte{0x98, 0x0, 0x0, 0x0}, header)

		length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
		sequence := header[3]
		require.Equal(t, 152, length)
		require.Equal(t, uint8(0), sequence)

		data := make([]byte, length)
		_, err = cr.Read(data)
		require.NoError(t, err)
		expected := []byte{0x3, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x22,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x22}
		require.Equal(t, expected, data)
	})
	t.Run("zstd", func(t *testing.T) {
		payload := []byte{0x1f, 0x0, 0x0, 0x0, 0x9c, 0x0, 0x0, 0x28, 0xb5, 0x2f, 0xfd,
			0x20, 0x9c, 0xb5, 0x0, 0x0, 0x78, 0x98, 0x0, 0x0, 0x0, 0x3, 0x53,
			0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x22, 0x41, 0x22, 0x1, 0x0, 0xa,
			0xa, 0x28, 0x1}
		r := bytes.NewReader(payload)
		seq := uint8(0)
		cr := newCompressedReader(r, mysql.CompressionZstd, &seq)
		header := make([]byte, 4)
		_, err := cr.Read(header)
		require.NoError(t, err)
		require.Equal(t, []byte{0x98, 0x0, 0x0, 0x0}, header)

		length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
		sequence := header[3]
		require.Equal(t, 152, length)
		require.Equal(t, uint8(0), sequence)

		data := make([]byte, length)
		_, err = cr.Read(data)
		require.NoError(t, err)
		expected := []byte{0x3, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x22,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
			0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x22}
		require.Equal(t, expected, data)
	})
}

// MariaDB Java Connecter 2.X would generate wrong sequence number for the sub header.
// TiDB should be compatible with it.
//
// MySQL Compressed Protocol Header:
// 0e 00 00   					Compressed length
// 00         					Compressed Packetnr
// 00 00 00   					Uncompressed length
//
// MySQL Protocol Header:
// 0a 00 00   					Payload length
// 01         					Packet Sequence Number (it should be 0x00, but MariaDB Connector/J 2.x sets it to 0x01)
// 03							COM_QUERY
// 73 65 6c 65 63 74 20 31 3b   "select 1;"
func TestSubHeaderWithWrongSequenceNumber(t *testing.T) {
	var inBuffer bytes.Buffer
	_, err := inBuffer.Write([]byte{0x0e, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x0a, 0x00, 0x00, 0x01, 0x03, 0x73, 0x65, 0x6c,
		0x65, 0x63, 0x74, 0x20, 0x31, 0x3b})
	require.NoError(t, err)
	// Test read one packet
	brc := util.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
	pkt := New(brc)
	pkt.SetCompressionAlgorithm(mysql.CompressionZlib)
	readBytes, err := pkt.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, uint8(1), pkt.sequence)
	require.Equal(t, uint8(1), pkt.compressedSequence)
	require.Equal(t, []byte{0x03, 0x73, 0x65, 0x6c, 0x65, 0x63,
		0x74, 0x20, 0x31, 0x3b}, readBytes)
}

func TestCompressedWriterThreshold(t *testing.T) {
	for _, algorithm := range []int{mysql.CompressionZlib, mysql.CompressionZstd} {
		var atLimit, aboveLimit bytes.Buffer
		seq := uint8(0)

		cw := newCompressedWriter(&atLimit, algorithm, &seq)
		cw.Write(bytes.Repeat([]byte{'a'}, minCompressLength))
		require.NoError(t, cw.Flush())
		require.Equal(t, []byte{0x0, 0x0, 0x0}, atLimit.Bytes()[4:7])
		require.Equal(t, bytes.Repeat([]byte{'a'}, minCompressLength), atLimit.Bytes()[7:])

		cw = newCompressedWriter(&aboveLimit, algorithm, &seq)
		cw.Write(bytes.Repeat([]byte{'a'}, minCompressLength+1))
		require.NoError(t, cw.Flush())
		uncompressedLength := minCompressLength + 1
		require.Equal(t, []byte{byte(uncompressedLength), byte(uncompressedLength >> 8), byte(uncompressedLength >> 16)},
			aboveLimit.Bytes()[4:7])
		require.Less(t, aboveLimit.Len(), minCompressLength)
	}
}

func TestCompressedZlibWritersShareThePool(t *testing.T) {
	var first, second bytes.Buffer
	firstSeq, secondSeq := uint8(0), uint8(0)
	firstWriter := newCompressedWriter(&first, mysql.CompressionZlib, &firstSeq)
	secondWriter := newCompressedWriter(&second, mysql.CompressionZlib, &secondSeq)
	firstPayload := bytes.Repeat([]byte("first "), 400)
	secondPayload := bytes.Repeat([]byte("second "), 400)

	for range 3 {
		first.Reset()
		second.Reset()
		firstWriter.Write(firstPayload)
		secondWriter.Write(secondPayload)
		require.NoError(t, firstWriter.Flush())
		require.NoError(t, secondWriter.Flush())

		require.Equal(t, firstPayload, inflate(t, first.Bytes()))
		require.Equal(t, secondPayload, inflate(t, second.Bytes()))
	}
}

func TestCompressedZstdWritersShareTheEncoderPools(t *testing.T) {
	var first, second bytes.Buffer
	firstSeq, secondSeq := uint8(0), uint8(0)
	firstWriter := newCompressedWriter(&first, mysql.CompressionZstd, &firstSeq)
	firstWriter.zstdLevel = zstd.SpeedFastest
	secondWriter := newCompressedWriter(&second, mysql.CompressionZstd, &secondSeq)
	secondWriter.zstdLevel = zstd.SpeedBetterCompression
	firstPayload := bytes.Repeat([]byte("first "), 400)
	secondPayload := bytes.Repeat([]byte("second "), 400)

	for range 3 {
		first.Reset()
		second.Reset()
		firstWriter.Write(firstPayload)
		secondWriter.Write(secondPayload)
		require.NoError(t, firstWriter.Flush())
		require.NoError(t, secondWriter.Flush())

		require.Equal(t, firstPayload, unzstd(t, first.Bytes()))
		require.Equal(t, secondPayload, unzstd(t, second.Bytes()))
	}

	cw := newCompressedWriter(io.Discard, mysql.CompressionZstd, &firstSeq)
	cw.zstdLevel = zstd.SpeedBestCompression + 1
	cw.Write(firstPayload)
	require.Error(t, cw.Flush())
}

func TestCompressedZstdWritersConcurrently(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out bytes.Buffer
			seq := uint8(0)
			cw := newCompressedWriter(&out, mysql.CompressionZstd, &seq)
			payload := bytes.Repeat([]byte(fmt.Sprintf("writer %d ", i)), 300)
			for range 50 {
				out.Reset()
				cw.Write(payload)
				if !assertNoError(t, cw.Flush()) {
					return
				}
				decoded := unzstd(t, out.Bytes())
				if !bytes.Equal(payload, decoded) {
					t.Errorf("writer %d decoded %d bytes", i, len(decoded))
					return
				}
			}
		}()
	}
	wg.Wait()
}

func assertNoError(t *testing.T, err error) bool {
	if err != nil {
		t.Error(err)
		return false
	}
	return true
}

func TestCompressedReaderZstdReadsOnePayload(t *testing.T) {
	first := append([]byte{0x05, 0x00, 0x00, 0x00, 0x03}, bytes.Repeat([]byte{'a'}, 2000)...)
	first[0], first[1], first[2] = byte(len(first)-4), byte((len(first)-4)>>8), 0
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(true))
	require.NoError(t, err)
	compressed := encoder.EncodeAll(first, nil)

	var in bytes.Buffer
	in.Write([]byte{byte(len(compressed)), byte(len(compressed) >> 8), byte(len(compressed) >> 16), 0x00,
		byte(len(first)), byte(len(first) >> 8), byte(len(first) >> 16)})
	in.Write(compressed)
	second := []byte{0x01, 0x00, 0x00, 0x01, 0x0e}
	in.Write([]byte{byte(len(second)), 0x00, 0x00, 0x01, 0x00, 0x00, 0x00})
	in.Write(second)

	pkt := New(util.NewBufferedReadConn(&testutil.BytesConn{Buffer: in}))
	pkt.SetCompressionAlgorithm(mysql.CompressionZstd)
	data, err := pkt.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, first[4:], data)
	data, err = pkt.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte{0x0e}, data)
}

func TestCompressedReaderZstdRejectsWrongLength(t *testing.T) {
	payload := bytes.Repeat([]byte{'a'}, 2000)
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := encoder.EncodeAll(payload, nil)
	var in bytes.Buffer
	in.Write([]byte{byte(len(compressed)), byte(len(compressed) >> 8), 0x00, 0x00, 0x10, 0x00, 0x00})
	in.Write(compressed)
	seq := uint8(0)
	cr := newCompressedReader(&in, mysql.CompressionZstd, &seq)
	_, err = cr.Read(make([]byte, 4))
	require.ErrorContains(t, err, "the header says 16")
}

func TestCompressedReaderZstdStopsAtTheHeaderLength(t *testing.T) {
	payload := bytes.Repeat([]byte{'a'}, 4<<20)
	withSize, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	var withoutSize bytes.Buffer
	streaming, err := zstd.NewWriter(&withoutSize)
	require.NoError(t, err)
	_, err = streaming.Write(payload)
	require.NoError(t, err)
	require.NoError(t, streaming.Close())
	for name, compressed := range map[string][]byte{
		"content size in the frame": withSize.EncodeAll(payload, nil),
		"no content size":           withoutSize.Bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			var in bytes.Buffer
			in.Write([]byte{byte(len(compressed)), byte(len(compressed) >> 8), byte(len(compressed) >> 16), 0x00, 0x10, 0x00, 0x00})
			in.Write(compressed)
			seq := uint8(0)
			cr := newCompressedReader(&in, mysql.CompressionZstd, &seq)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err := cr.Read(make([]byte, 4))
			runtime.ReadMemStats(&after)
			require.ErrorContains(t, err, "the header says 16")
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
		})
	}
}

func compressedPacket(seq uint8, uncompressedLength int, payload []byte) []byte {
	return append([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), seq,
		byte(uncompressedLength), byte(uncompressedLength >> 8), byte(uncompressedLength >> 16)}, payload...)
}

func deflate(t *testing.T, data []byte) []byte {
	var out bytes.Buffer
	w := zlib.NewWriter(&out)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return out.Bytes()
}

func TestCompressedWriterReaderRoundTrip(t *testing.T) {
	random := make([]byte, 200*1024)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range random {
		random[i] = byte(rng.Uint32())
	}
	type writer struct {
		name      string
		algorithm int
		level     zstd.EncoderLevel
	}
	writers := []writer{{name: "zlib", algorithm: mysql.CompressionZlib}}
	for level := zstd.SpeedFastest; level <= zstd.SpeedBestCompression; level++ {
		writers = append(writers, writer{name: "zstd " + level.String(), algorithm: mysql.CompressionZstd, level: level})
	}
	for _, w := range writers {
		t.Run(w.name, func(t *testing.T) {
			var wire bytes.Buffer
			writerSeq, readerSeq := uint8(252), uint8(252)
			cw := newCompressedWriter(&wire, w.algorithm, &writerSeq)
			cw.zstdLevel = w.level
			sizes := []int{1, minCompressLength, minCompressLength + 1, 70 * 1024, len(random)}
			for _, size := range sizes {
				_, err := cw.Write(random[:size])
				require.NoError(t, err)
				require.NoError(t, cw.Flush())
			}
			cr := newCompressedReader(&wire, w.algorithm, &readerSeq)
			for _, size := range sizes {
				got := make([]byte, size)
				_, err := io.ReadFull(cr, got)
				require.NoError(t, err)
				require.Equal(t, random[:size], got)
			}
			require.Zero(t, wire.Len())
			require.Equal(t, uint8(1), writerSeq)
			require.Equal(t, writerSeq, readerSeq)
		})
	}
}

func TestCompressedWriterFlushDoesNotAllocate(t *testing.T) {
	if israce.RaceEnabled {
		t.Skip("sync.Pool drops items at random under the race detector")
	}
	payload := bytes.Repeat([]byte("\x05\x00\x00\x01\x011\x0e2025-01-01 00:00:00"), 300)
	for _, algorithm := range []int{mysql.CompressionZlib, mysql.CompressionZstd} {
		seq := uint8(0)
		cw := newCompressedWriter(io.Discard, algorithm, &seq)
		allocs := testing.AllocsPerRun(20, func() {
			_, err := cw.Write(payload)
			require.NoError(t, err)
			require.NoError(t, cw.Flush())
		})
		require.Zero(t, allocs, "algorithm %d", algorithm)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestCompressedWriterReportsAFailedWrite(t *testing.T) {
	for _, size := range []int{10, 2000} {
		for _, algorithm := range []int{mysql.CompressionZlib, mysql.CompressionZstd} {
			seq := uint8(0)
			cw := newCompressedWriter(failingWriter{}, algorithm, &seq)
			_, err := cw.Write(bytes.Repeat([]byte{'a'}, size))
			require.NoError(t, err)
			require.ErrorIs(t, cw.Flush(), io.ErrClosedPipe)
		}
	}
}

func TestCompressedReaderReadsAOneBytePayload(t *testing.T) {
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	for algorithm, compressed := range map[int][]byte{
		mysql.CompressionZlib: deflate(t, []byte{'q'}),
		mysql.CompressionZstd: encoder.EncodeAll([]byte{'q'}, nil),
	} {
		seq := uint8(0)
		cr := newCompressedReader(bytes.NewReader(compressedPacket(0, 1, compressed)), algorithm, &seq)
		got := make([]byte, 1)
		_, err := io.ReadFull(cr, got)
		require.NoError(t, err)
		require.Equal(t, []byte{'q'}, got)
	}
}

func TestCompressedReaderRejectsABrokenPacket(t *testing.T) {
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	payload := bytes.Repeat([]byte("abc"), 1000)
	zlibPayload, zstdPayload := deflate(t, payload), encoder.EncodeAll(payload, nil)
	cases := []struct {
		name      string
		algorithm int
		input     []byte
		check     func(t *testing.T, err error)
	}{
		{"no header", mysql.CompressionZlib, nil, func(t *testing.T, err error) { require.ErrorIs(t, err, io.EOF) }},
		{"short header", mysql.CompressionZlib, []byte{0x01, 0x00, 0x00}, func(t *testing.T, err error) {
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		}},
		{"wrong sequence", mysql.CompressionZlib, compressedPacket(1, 0, []byte{'a'}), func(t *testing.T, err error) {
			require.True(t, server_err.ErrInvalidSequence.Equal(err), "%v", err)
			require.ErrorContains(t, err, "received 1 while expecting 0")
		}},
		{"short uncompressed payload", mysql.CompressionZlib, compressedPacket(0, 0, []byte("abcd"))[:9], requireError},
		{"short zlib payload", mysql.CompressionZlib, compressedPacket(0, len(payload), zlibPayload)[:20], requireError},
		{"short zstd payload", mysql.CompressionZstd, compressedPacket(0, len(payload), zstdPayload)[:20], requireError},
		{"corrupt zlib payload", mysql.CompressionZlib, compressedPacket(0, len(payload), bytes.Repeat([]byte{0xff}, 40)), requireError},
		{"corrupt zstd payload", mysql.CompressionZstd, compressedPacket(0, len(payload), bytes.Repeat([]byte{0xff}, 40)), requireError},
		{"zlib payload longer than its header", mysql.CompressionZlib, compressedPacket(0, 16, zlibPayload), func(t *testing.T, err error) {
			require.ErrorContains(t, err, "the header says 16")
		}},
		{"zlib payload with a bad checksum", mysql.CompressionZlib, compressedPacket(0, len(payload), corruptChecksum(zlibPayload)), func(t *testing.T, err error) {
			require.ErrorIs(t, err, zlib.ErrChecksum)
		}},
		{"zlib payload followed by other bytes", mysql.CompressionZlib, compressedPacket(0, len(payload), append(bytes.Clone(zlibPayload), 'x')), func(t *testing.T, err error) {
			require.ErrorContains(t, err, "1 bytes after the end")
		}},
		{"unknown algorithm", mysql.CompressionNone, compressedPacket(0, len(payload), zlibPayload), requireError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			seq := uint8(0)
			cr := newCompressedReader(bytes.NewReader(c.input), c.algorithm, &seq)
			n, err := cr.Read(make([]byte, 4))
			require.Zero(t, n)
			c.check(t, err)
		})
	}
}

func requireError(t *testing.T, err error) {
	require.Error(t, err)
}

func corruptChecksum(compressed []byte) []byte {
	corrupt := bytes.Clone(compressed)
	corrupt[len(corrupt)-1] ^= 0xff
	return corrupt
}

func TestCompressedReaderZlibReadsOnePayload(t *testing.T) {
	first := make([]byte, 256*1024)
	for i := range first {
		first[i] = byte(rand.IntN(256))
	}
	compressed := deflate(t, first)
	in := bytes.NewBuffer(compressedPacket(0, len(first), compressed))
	in.Write(compressedPacket(1, 0, []byte{'z'}))

	seq := uint8(0)
	cr := newCompressedReader(in, mysql.CompressionZlib, &seq)
	got := make([]byte, len(first)+1)
	_, err := io.ReadFull(cr, got)
	require.NoError(t, err)
	require.Equal(t, append(first, 'z'), got)
	require.Zero(t, in.Len())
}

func TestUint24(t *testing.T) {
	cases := []struct {
		value int
		bytes []byte
	}{
		{0, []byte{0x00, 0x00, 0x00}},
		{1, []byte{0x01, 0x00, 0x00}},
		{0xfffe, []byte{0xfe, 0xff, 0x00}},
		{0x10000, []byte{0x00, 0x00, 0x01}},
		{0xabcdef, []byte{0xef, 0xcd, 0xab}},
		{0xffffff, []byte{0xff, 0xff, 0xff}},
	}
	for _, c := range cases {
		got := make([]byte, 3)
		putUint24(got, c.value)
		require.Equal(t, c.bytes, got, "putUint24(%#x)", c.value)
		require.Equal(t, c.value, uint24(c.bytes), "uint24(% x)", c.bytes)
	}
}

func BenchmarkCompressedReaderZstd(b *testing.B) {
	row := []byte("\x05\x00\x00\x01\x011\x0e2025-01-01 00:00:00\x03123\x0bsome-string")
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{256, 4 * 1024, 64 * 1024} {
		payload := bytes.Repeat(row, size/len(row)+1)[:size]
		compressed := encoder.EncodeAll(payload, nil)
		packet := append([]byte{byte(len(compressed)), byte(len(compressed) >> 8), byte(len(compressed) >> 16), 0x00,
			byte(size), byte(size >> 8), byte(size >> 16)}, compressed...)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			r := bytes.NewReader(packet)
			buf := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				r.Reset(packet)
				seq := uint8(0)
				cr := newCompressedReader(r, mysql.CompressionZstd, &seq)
				if _, err := io.ReadFull(cr, buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCompressedWriterFlush(b *testing.B) {
	row := []byte("\x05\x00\x00\x01\x011\x0e2025-01-01 00:00:00\x03123\x0bsome-string")
	for _, algorithm := range []struct {
		name string
		id   int
	}{{"zlib", mysql.CompressionZlib}, {"zstd", mysql.CompressionZstd}} {
		for _, size := range []int{256, 4 * 1024, 64 * 1024} {
			payload := bytes.Repeat(row, size/len(row)+1)[:size]
			b.Run(algorithm.name+"/"+strconv.Itoa(size), func(b *testing.B) {
				benchmarkCompressedWriterFlush(b, algorithm.id, payload)
			})
		}
	}
}

func benchmarkCompressedWriterFlush(b *testing.B, algorithm int, payload []byte) {
	seq := uint8(0)
	cw := newCompressedWriter(io.Discard, algorithm, &seq)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := cw.Write(payload); err != nil {
			b.Fatal(err)
		}
		if err := cw.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPooledZstdEncoderHoldsTheTablesOfOneEncoder(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(32))
	pool, err := zstdEncoderPool(zstd.SpeedDefault)
	require.NoError(t, err)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	encoder := pool.New().(*zstd.Encoder)
	encoder.EncodeAll(bytes.Repeat([]byte("abc"), 1000), nil)
	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20))
}
