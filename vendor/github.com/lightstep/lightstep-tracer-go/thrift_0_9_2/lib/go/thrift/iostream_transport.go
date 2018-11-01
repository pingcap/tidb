/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"bufio"
	"io"
)

// StreamTransport is a Transport made of an io.Reader and/or an io.Writer
type StreamTransport struct {
	io.Reader
	io.Writer
	isReadWriter bool
}

type StreamTransportFactory struct {
	Reader       io.Reader
	Writer       io.Writer
	isReadWriter bool
}

func (p *StreamTransportFactory) GetTransport(trans TTransport) TTransport {
	if trans != nil {
		t, ok := trans.(*StreamTransport)
		if ok {
			if t.isReadWriter {
				return NewStreamTransportRW(t.Reader.(io.ReadWriter))
			}
			if t.Reader != nil && t.Writer != nil {
				return NewStreamTransport(t.Reader, t.Writer)
			}
			if t.Reader != nil && t.Writer == nil {
				return NewStreamTransportR(t.Reader)
			}
			if t.Reader == nil && t.Writer != nil {
				return NewStreamTransportW(t.Writer)
			}
			return &StreamTransport{}
		}
	}
	if p.isReadWriter {
		return NewStreamTransportRW(p.Reader.(io.ReadWriter))
	}
	if p.Reader != nil && p.Writer != nil {
		return NewStreamTransport(p.Reader, p.Writer)
	}
	if p.Reader != nil && p.Writer == nil {
		return NewStreamTransportR(p.Reader)
	}
	if p.Reader == nil && p.Writer != nil {
		return NewStreamTransportW(p.Writer)
	}
	return &StreamTransport{}
}

func NewStreamTransportFactory(reader io.Reader, writer io.Writer, isReadWriter bool) *StreamTransportFactory {
	return &StreamTransportFactory{Reader: reader, Writer: writer, isReadWriter: isReadWriter}
}

func NewStreamTransport(r io.Reader, w io.Writer) *StreamTransport {
	return &StreamTransport{Reader: bufio.NewReader(r), Writer: bufio.NewWriter(w)}
}

func NewStreamTransportR(r io.Reader) *StreamTransport {
	return &StreamTransport{Reader: bufio.NewReader(r)}
}

func NewStreamTransportW(w io.Writer) *StreamTransport {
	return &StreamTransport{Writer: bufio.NewWriter(w)}
}

func NewStreamTransportRW(rw io.ReadWriter) *StreamTransport {
	bufrw := bufio.NewReadWriter(bufio.NewReader(rw), bufio.NewWriter(rw))
	return &StreamTransport{Reader: bufrw, Writer: bufrw, isReadWriter: true}
}

// (The streams must already be open at construction time, so this should
// always return true.)
func (p *StreamTransport) IsOpen() bool {
	return true
}

// (The streams must already be open. This method does nothing.)
func (p *StreamTransport) Open() error {
	return nil
}

// func (p *StreamTransport) Peek() bool {
// 	return p.IsOpen()
// }

// Closes both the input and output streams.
func (p *StreamTransport) Close() error {
	closedReader := false
	if p.Reader != nil {
		c, ok := p.Reader.(io.Closer)
		if ok {
			e := c.Close()
			closedReader = true
			if e != nil {
				return e
			}
		}
		p.Reader = nil
	}
	if p.Writer != nil && (!closedReader || !p.isReadWriter) {
		c, ok := p.Writer.(io.Closer)
		if ok {
			e := c.Close()
			if e != nil {
				return e
			}
		}
		p.Writer = nil
	}
	return nil
}

// Flushes the underlying output stream if not null.
func (p *StreamTransport) Flush() error {
	if p.Writer == nil {
		return NewTTransportException(NOT_OPEN, "Cannot flush null outputStream")
	}
	f, ok := p.Writer.(Flusher)
	if ok {
		err := f.Flush()
		if err != nil {
			return NewTTransportExceptionFromError(err)
		}
	}
	return nil
}

func (p *StreamTransport) Read(c []byte) (n int, err error) {
	n, err = p.Reader.Read(c)
	if err != nil {
		err = NewTTransportExceptionFromError(err)
	}
	return
}

func (p *StreamTransport) ReadByte() (c byte, err error) {
	f, ok := p.Reader.(io.ByteReader)
	if ok {
		c, err = f.ReadByte()
	} else {
		c, err = readByte(p.Reader)
	}
	if err != nil {
		err = NewTTransportExceptionFromError(err)
	}
	return
}

func (p *StreamTransport) Write(c []byte) (n int, err error) {
	n, err = p.Writer.Write(c)
	if err != nil {
		err = NewTTransportExceptionFromError(err)
	}
	return
}

func (p *StreamTransport) WriteByte(c byte) (err error) {
	f, ok := p.Writer.(io.ByteWriter)
	if ok {
		err = f.WriteByte(c)
	} else {
		err = writeByte(p.Writer, c)
	}
	if err != nil {
		err = NewTTransportExceptionFromError(err)
	}
	return
}

func (p *StreamTransport) WriteString(s string) (n int, err error) {
	f, ok := p.Writer.(stringWriter)
	if ok {
		n, err = f.WriteString(s)
	} else {
		n, err = p.Writer.Write([]byte(s))
	}
	if err != nil {
		err = NewTTransportExceptionFromError(err)
	}
	return
}
