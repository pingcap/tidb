// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// Package packetio provides helpers to read and write MySQL protocol packets
// on a single connection.
package packetio

import (
	"bufio"
	"bytes"
	goerrors "errors"
	"io"
	"sync"
	"time"

	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	server_err "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	server_metrics "github.com/pingcap/tidb/pkg/server/metrics"
)

const defaultWriterSize = 16 * 1024

// PacketIO is a helper to read and write data in packet format.
// MySQL Packets: https://dev.mysql.com/doc/internals/en/mysql-packet.html
type PacketIO struct {
	bufReadConn      *util.BufferedReadConn
	bufWriter        *bufio.Writer
	compressedWriter *compressedWriter
	compressedReader *compressedReader
	readTimeout      time.Duration
	// maxAllowedPacket is the maximum size of one packet in ReadPacket.
	maxAllowedPacket uint64
	// accumulatedLength count the length of totally received 'payload' in ReadPacket.
	accumulatedLength    uint64
	compressionAlgorithm int
	zstdLevel            zstd.EncoderLevel
	sequence             uint8
	compressedSequence   uint8
}

// New creates a new PacketIO reading from the given *util.BufferedReadConn,
// with the max allowed packet taken from the global config.
func New(bufReadConn *util.BufferedReadConn) *PacketIO {
	p := &PacketIO{sequence: 0, compressionAlgorithm: mysql.CompressionNone, compressedSequence: 0, zstdLevel: 3}
	p.SetBufferedReadConn(bufReadConn)
	p.SetMaxAllowedPacket(config.GetMaxAllowedPacket())
	return p
}

// NewForTest creates a new PacketIO with given bufio.Writer.
func NewForTest(bufWriter *bufio.Writer) *PacketIO {
	p := &PacketIO{}
	p.SetBufWriter(bufWriter)
	return p
}

// SetZstdLevel sets the zstd compression level.
func (p *PacketIO) SetZstdLevel(level zstd.EncoderLevel) {
	p.zstdLevel = level
}

// Sequence returns the sequence of PacketIO.
func (p *PacketIO) Sequence() uint8 {
	return p.sequence
}

// SetSequence sets the sequence of PacketIO.
func (p *PacketIO) SetSequence(s uint8) {
	p.sequence = s
}

// SetCompressedSequence sets the compressed sequence of PacketIO.
func (p *PacketIO) SetCompressedSequence(s uint8) {
	p.compressedSequence = s
}

// SetBufWriter sets the bufio.Writer of PacketIO.
func (p *PacketIO) SetBufWriter(bufWriter *bufio.Writer) {
	p.bufWriter = bufWriter
}

// ResetBufWriter resets the bufio.Writer of PacketIO.
func (p *PacketIO) ResetBufWriter(w io.Writer) {
	p.bufWriter.Reset(w)
}

// SetCompressionAlgorithm sets the compression algorithm of PacketIO.
func (p *PacketIO) SetCompressionAlgorithm(ca int) {
	p.compressionAlgorithm = ca
	p.compressedWriter = newCompressedWriter(p.bufReadConn, ca, &p.compressedSequence)
	p.compressedWriter.zstdLevel = p.zstdLevel
	p.compressedReader = newCompressedReader(p.bufReadConn, ca, &p.compressedSequence)
	p.compressedReader.zstdLevel = p.zstdLevel
	p.bufWriter.Flush()
}

// SetBufferedReadConn sets the BufferedReadConn of PacketIO.
func (p *PacketIO) SetBufferedReadConn(bufReadConn *util.BufferedReadConn) {
	p.bufReadConn = bufReadConn
	p.bufWriter = bufio.NewWriterSize(bufReadConn, defaultWriterSize)
}

// SetReadTimeout sets the read timeout of PacketIO.
func (p *PacketIO) SetReadTimeout(timeout time.Duration) {
	p.readTimeout = timeout
}

func (p *PacketIO) readOnePacket() ([]byte, error) {
	var header [4]byte
	r := io.NopCloser(p.bufReadConn)
	if p.readTimeout > 0 {
		if err := p.bufReadConn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if p.compressionAlgorithm == mysql.CompressionNone {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		if _, err := io.ReadFull(p.compressedReader, header[:]); err != nil {
			return nil, errors.Trace(err)
		}
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	sequence := header[3]
	if sequence != p.sequence {
		err := server_err.ErrInvalidSequence.GenWithStack(
			"invalid sequence, received %d while expecting %d", sequence, p.sequence)
		if p.compressionAlgorithm == mysql.CompressionNone {
			return nil, err
		}
		// To be compatible with MariaDB Connector/J 2.x,
		// ignore sequence check and print a log when compression protocol is active.
		terror.Log(err)
	}
	p.sequence++

	// Accumulated payload length exceeds the limit.
	if p.accumulatedLength += uint64(length); p.accumulatedLength > p.maxAllowedPacket {
		terror.Log(server_err.ErrNetPacketTooLarge)
		return nil, server_err.ErrNetPacketTooLarge
	}

	data := make([]byte, length)
	if p.readTimeout > 0 {
		if err := p.bufReadConn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if p.compressionAlgorithm == mysql.CompressionNone {
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		if _, err := io.ReadFull(p.compressedReader, data); err != nil {
			return nil, errors.Trace(err)
		}
	}
	err := r.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

// SetMaxAllowedPacket sets the max allowed packet size of PacketIO.
func (p *PacketIO) SetMaxAllowedPacket(maxAllowedPacket uint64) {
	p.maxAllowedPacket = maxAllowedPacket
}

// ReadPacket reads a packet from the connection.
func (p *PacketIO) ReadPacket() ([]byte, error) {
	p.accumulatedLength = 0
	if p.readTimeout == 0 {
		if err := p.bufReadConn.SetReadDeadline(time.Time{}); err != nil {
			return nil, errors.Trace(err)
		}
	}
	data, err := p.readOnePacket()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) < mysql.MaxPayloadLen {
		server_metrics.InPacketBytes.Add(float64(len(data)))
		return data, nil
	}

	// handle multi-packet
	for {
		buf, err := p.readOnePacket()
		if err != nil {
			return nil, errors.Trace(err)
		}

		data = append(data, buf...)

		if len(buf) < mysql.MaxPayloadLen {
			break
		}
	}

	server_metrics.InPacketBytes.Add(float64(len(data)))
	return data, nil
}

// WritePacket writes data that already have header
func (p *PacketIO) WritePacket(data []byte) error {
	length := len(data) - 4
	server_metrics.OutPacketBytes.Add(float64(len(data)))

	maxPayloadLen := mysql.MaxPayloadLen

	for length >= maxPayloadLen {
		data[3] = p.sequence
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		if p.compressionAlgorithm != mysql.CompressionNone {
			if n, err := p.compressedWriter.Write(data[:4+maxPayloadLen]); err != nil {
				return errors.Trace(mysql.ErrBadConn)
			} else if n != (4 + maxPayloadLen) {
				return errors.Trace(mysql.ErrBadConn)
			}
		} else {
			if n, err := p.bufWriter.Write(data[:4+maxPayloadLen]); err != nil {
				return errors.Trace(mysql.ErrBadConn)
			} else if n != (4 + maxPayloadLen) {
				return errors.Trace(mysql.ErrBadConn)
			}
		}
		p.sequence++
		length -= maxPayloadLen
		data = data[maxPayloadLen:]
	}
	data[3] = p.sequence
	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)

	if p.compressionAlgorithm != mysql.CompressionNone {
		if n, err := p.compressedWriter.Write(data); err != nil {
			terror.Log(errors.Trace(err))
			return errors.Trace(mysql.ErrBadConn)
		} else if n != len(data) {
			return errors.Trace(mysql.ErrBadConn)
		}
		p.sequence++
		return nil
	}
	if n, err := p.bufWriter.Write(data); err != nil {
		terror.Log(errors.Trace(err))
		return errors.Trace(mysql.ErrBadConn)
	} else if n != len(data) {
		return errors.Trace(mysql.ErrBadConn)
	}
	p.sequence++
	return nil
}

// Flush flushes buffered data to network.
func (p *PacketIO) Flush() error {
	var err error
	if p.compressionAlgorithm != mysql.CompressionNone {
		err = p.compressedWriter.Flush()
	} else {
		err = p.bufWriter.Flush()
	}
	if err != nil {
		return errors.Trace(err)
	}

	if p.compressionAlgorithm != mysql.CompressionNone {
		p.sequence = p.compressedSequence
	}
	return err
}

func newCompressedWriter(w io.Writer, ca int, seq *uint8) *compressedWriter {
	return &compressedWriter{
		w,
		new(bytes.Buffer),
		seq,
		new(bytes.Buffer),
		ca,
		3,
	}
}

type compressedWriter struct {
	w                    io.Writer
	buf                  *bytes.Buffer
	compressedSequence   *uint8
	packet               *bytes.Buffer
	compressionAlgorithm int
	zstdLevel            zstd.EncoderLevel
}

func (cw *compressedWriter) Write(data []byte) (n int, err error) {
	// MySQL starts with `net_buffer_length` (default 16384) and larger packets after that.
	// The length itself must fit in the 3 byte field in the header.
	// Can't be bigger then the max value for `net_buffer_length` (1048576)
	maxCompressedSize := 1048576 // 1 MiB

	for {
		remainingLen := maxCompressedSize - cw.buf.Len()
		if len(data) <= remainingLen {
			written, err := cw.buf.Write(data)
			if err != nil {
				return 0, err
			}
			return n + written, nil
		}
		written, err := cw.buf.Write(data[:remainingLen])
		if err != nil {
			return 0, err
		}
		n += written
		data = data[remainingLen:]
		err = cw.Flush()
		if err != nil {
			return 0, err
		}
	}
}

func (cw *compressedWriter) Flush() error {
	data := cw.buf.Bytes()
	cw.buf.Reset()
	packet := cw.packet
	packet.Reset()
	packet.Write(compressedHeaderPlaceholder[:])
	uncompressedLength := 0
	if len(data) > minCompressLength {
		if err := cw.compress(packet, data); err != nil {
			return errors.Trace(err)
		}
		uncompressedLength = len(data)
	} else {
		packet.Write(data)
	}
	header := packet.Bytes()
	putUint24(header[0:3], packet.Len()-len(compressedHeaderPlaceholder))
	header[3] = *cw.compressedSequence
	putUint24(header[4:7], uncompressedLength)
	*cw.compressedSequence++
	_, err := cw.w.Write(packet.Bytes())
	return errors.Trace(err)
}

func (cw *compressedWriter) compress(dst *bytes.Buffer, data []byte) error {
	switch cw.compressionAlgorithm {
	case mysql.CompressionZlib:
		return deflateInto(dst, data)
	case mysql.CompressionZstd:
		return zstdInto(dst, data, cw.zstdLevel)
	}
	return errors.New("Unknown compression algorithm")
}

func deflateInto(dst *bytes.Buffer, data []byte) error {
	zw := zlibWriterPool.Get().(*zlib.Writer)
	defer zlibWriterPool.Put(zw)
	zw.Reset(dst)
	_, err := zw.Write(data)
	if err == nil {
		err = zw.Close()
	}
	zw.Reset(nil)
	return err
}

func zstdInto(dst *bytes.Buffer, data []byte, level zstd.EncoderLevel) error {
	pool, err := zstdEncoderPool(level)
	if err != nil {
		return err
	}
	encoder, _ := pool.Get().(*zstd.Encoder)
	if encoder == nil {
		return errors.Errorf("cannot create a zstd encoder at level %d", level)
	}
	dst.Write(encoder.EncodeAll(data, dst.AvailableBuffer()))
	pool.Put(encoder)
	return nil
}

var compressedHeaderPlaceholder [7]byte

func putUint24(b []byte, v int) {
	_ = b[2] // bounds check hint to compiler; see golang.org/issue/14808
	b[0], b[1], b[2] = byte(v), byte(v>>8), byte(v>>16)
}

func uint24(b []byte) int {
	_ = b[2] // bounds check hint to compiler; see golang.org/issue/14808
	return int(b[0]) | int(b[1])<<8 | int(b[2])<<16
}

// Packets up to this size are sent uncompressed; MySQL itself uses 50 bytes.
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_compression_packet.html
const minCompressLength = 512

// Shared by all connections: each writer holds a few hundred KiB of encoder state.
var zlibWriterPool = sync.Pool{
	New: func() any {
		w, _ := zlib.NewWriterLevel(nil, mysql.ZlibCompressDefaultLevel)
		return w
	},
}

// Shared by all connections, one pool per level: an encoder holds from a few to tens of MiB of
// tables, so they follow the number of concurrent flushes rather than the number of connections.
var zstdEncoderPools [zstd.SpeedBestCompression + 1]sync.Pool

func zstdEncoderPool(level zstd.EncoderLevel) (*sync.Pool, error) {
	if level < zstd.SpeedFastest || level > zstd.SpeedBestCompression {
		return nil, errors.Errorf("unknown zstd encoder level %d", level)
	}
	return &zstdEncoderPools[level], nil
}

func init() {
	for level := zstd.SpeedFastest; level <= zstd.SpeedBestCompression; level++ {
		zstdEncoderPools[level].New = func() any {
			encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithEncoderConcurrency(1))
			if err != nil {
				return nil
			}
			return encoder
		}
	}
}

// DecodeAll is safe for concurrent use and the decoder keeps little state, so one is shared.
var sharedZstdDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderMaxMemory(mysql.MaxPayloadLen), zstd.WithDecodeAllCapLimit(true))
})

func newCompressedReader(r io.Reader, ca int, seq *uint8) *compressedReader {
	return &compressedReader{
		r,
		seq,
		nil,
		ca,
		3,
		0,
	}
}

type compressedReader struct {
	r                    io.Reader
	compressedSequence   *uint8
	data                 []byte
	compressionAlgorithm int
	zstdLevel            zstd.EncoderLevel
	pos                  int
}

func (cr *compressedReader) Read(data []byte) (int, error) {
	if cr.data == nil {
		packet, err := cr.readPacket()
		if err != nil {
			return 0, err
		}
		cr.data = packet
	}
	n := copy(data, cr.data[cr.pos:])
	cr.pos += n
	if cr.pos == len(cr.data) {
		cr.pos = 0
		cr.data = nil
	}
	return n, nil
}

func (cr *compressedReader) readPacket() ([]byte, error) {
	var header [7]byte
	if _, err := io.ReadFull(cr.r, header[:]); err != nil {
		return nil, err
	}
	compressedLength, sequence, uncompressedLength := uint24(header[0:3]), header[3], uint24(header[4:7])
	if sequence != *cr.compressedSequence {
		return nil, server_err.ErrInvalidSequence.GenWithStack(
			"invalid compressed sequence, received %d while expecting %d", sequence, *cr.compressedSequence)
	}
	*cr.compressedSequence++
	if uncompressedLength == 0 {
		return readPayload(cr.r, compressedLength)
	}
	switch cr.compressionAlgorithm {
	case mysql.CompressionZlib:
		return readZlibPayload(cr.r, compressedLength, uncompressedLength)
	case mysql.CompressionZstd:
		return readZstdPayload(cr.r, compressedLength, uncompressedLength)
	}
	return nil, errors.New("Unknown compression algorithm")
}

func readPayload(r io.Reader, length int) ([]byte, error) {
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

// readZlibPayload reads exactly one compressed payload and checks that its stream, checksum
// included, ends where the header says.
func readZlibPayload(r io.Reader, compressedLength, uncompressedLength int) ([]byte, error) {
	compressed, err := readPayload(r, compressedLength)
	if err != nil {
		return nil, err
	}
	in := bytes.NewReader(compressed)
	zr, err := zlib.NewReader(in)
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err := readPayload(zr, uncompressedLength)
	if err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, err := zr.Read(extra[:]); n > 0 {
		return nil, errors.Errorf("zlib payload is larger than the header says %d", uncompressedLength)
	} else if err != io.EOF {
		return nil, errors.Trace(err)
	}
	if in.Len() > 0 {
		return nil, errors.Errorf("zlib payload has %d bytes after the end of its stream", in.Len())
	}
	return data, nil
}

func (*compressedReader) Close() error {
	return nil
}

// readZstdPayload reads exactly one compressed payload and checks its length against the header.
func readZstdPayload(r io.Reader, compressedLength, uncompressedLength int) ([]byte, error) {
	decoder, err := sharedZstdDecoder()
	if err != nil {
		return nil, errors.Trace(err)
	}
	compressed, err := readPayload(r, compressedLength)
	if err != nil {
		return nil, err
	}
	data, err := decoder.DecodeAll(compressed, make([]byte, 0, uncompressedLength))
	if goerrors.Is(err, zstd.ErrDecoderSizeExceeded) {
		return nil, errors.Errorf("zstd payload is larger than the header says %d", uncompressedLength)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(data) != uncompressedLength {
		return nil, errors.Errorf("zstd payload is %d bytes, the header says %d", len(data), uncompressedLength)
	}
	return data, nil
}
