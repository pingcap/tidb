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

package server

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"io"
	"net"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func BenchmarkPacketIOWrite(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var outBuffer bytes.Buffer
		pkt := &packetIO{bufWriter: bufio.NewWriter(&outBuffer)}
		_ = pkt.writePacket([]byte{0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x31, 0x30, 0x38, 0x0, 0xfe})
	}
}

func TestPacketIOWrite(t *testing.T) {
	// Test write one packet
	var outBuffer bytes.Buffer
	pkt := &packetIO{bufWriter: bufio.NewWriter(&outBuffer)}
	err := pkt.writePacket([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03})
	require.NoError(t, err)
	err = pkt.flush()
	require.NoError(t, err)
	require.Equal(t, []byte{0x03, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03}, outBuffer.Bytes())

	// Test write more than one packet
	outBuffer.Reset()
	largeInput := make([]byte, mysql.MaxPayloadLen+4)
	pkt = &packetIO{bufWriter: bufio.NewWriter(&outBuffer)}
	err = pkt.writePacket(largeInput)
	require.NoError(t, err)
	err = pkt.flush()
	require.NoError(t, err)
	res := outBuffer.Bytes()
	require.Equal(t, byte(0xff), res[0])
	require.Equal(t, byte(0xff), res[1])
	require.Equal(t, byte(0xff), res[2])
	require.Equal(t, byte(0), res[3])
}

func TestPacketIORead(t *testing.T) {
	var inBuffer bytes.Buffer
	_, err := inBuffer.Write([]byte{0x01, 0x00, 0x00, 0x00, 0x01})
	require.NoError(t, err)
	// Test read one packet
	brc := newBufferedReadConn(&bytesConn{inBuffer})
	pkt := newPacketIO(brc)
	readBytes, err := pkt.readPacket()
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
	brc = newBufferedReadConn(&bytesConn{inBuffer})
	pkt = newPacketIO(brc)
	readBytes, err = pkt.readPacket()
	require.NoError(t, err)
	require.Equal(t, uint8(2), pkt.sequence)
	require.Equal(t, mysql.MaxPayloadLen+1, len(readBytes))
	require.Equal(t, byte(0x0a), readBytes[mysql.MaxPayloadLen])
}

type bytesConn struct {
	b bytes.Buffer
}

func (c *bytesConn) Read(b []byte) (n int, err error) {
	return c.b.Read(b)
}

func (c *bytesConn) Write(b []byte) (n int, err error) {
	return 0, nil
}

func (c *bytesConn) Close() error {
	return nil
}

func (c *bytesConn) LocalAddr() net.Addr {
	return nil
}

func (c *bytesConn) RemoteAddr() net.Addr {
	return nil
}

func (c *bytesConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *bytesConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *bytesConn) SetWriteDeadline(t time.Time) error {
	return nil
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

	cw := newCompressedWriter(&testdata, mysql.CompressionZlib)
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
		var testdata, decoded bytes.Buffer
		payload := []byte("test_zlib test_zlib test_zlib test_zlib test_zlib test_zlib test_zlib")

		cw := newCompressedWriter(&testdata, mysql.CompressionZlib)
		cw.Write(payload)
		cw.Flush()

		// Header:
		// 3c 00 00   Compressed length: 60
		// 00         Packetnr: 0
		// 45 00 00   Uncompressed length: 69
		compressedLength := []byte{0x3c, 0x0, 0x0}
		packetNr := []byte{0x0}
		uncompressedLength := []byte{0x45, 0x0, 0x0}
		require.Equal(t, compressedLength, testdata.Bytes()[:3])
		require.Equal(t, packetNr, testdata.Bytes()[3:4])
		require.Equal(t, uncompressedLength, testdata.Bytes()[4:7])

		// Payload:
		r, err := zlib.NewReader(bytes.NewReader(testdata.Bytes()[7:]))
		require.NoError(t, err)
		io.Copy(&decoded, r)
		require.Equal(t, payload, decoded.Bytes())
	})

	t.Run("zstd", func(t *testing.T) {
		var testdata bytes.Buffer
		payload := []byte("test_zstd test_zstd test_zstd test_zstd test_zstd test_zstd test_zstd")

		cw := newCompressedWriter(&testdata, mysql.CompressionZstd)
		cw.Write(payload)
		cw.Flush()

		// Header:
		// 1e 00 00   Compressed length: 30
		// 00         Packetnr: 0
		// 45 00 00   Uncompressed length: 69
		compressedLength := []byte{0x1e, 0x0, 0x0}
		packetNr := []byte{0x0}
		uncompressedLength := []byte{0x45, 0x0, 0x0}
		require.Equal(t, compressedLength, testdata.Bytes()[:3])
		require.Equal(t, packetNr, testdata.Bytes()[3:4])
		require.Equal(t, uncompressedLength, testdata.Bytes()[4:7])

		// Payload:
		d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
		require.NoError(t, err)
		decoded, err := d.DecodeAll(testdata.Bytes()[7:], nil)
		require.NoError(t, err)
		require.Equal(t, payload, decoded)
	})
}
