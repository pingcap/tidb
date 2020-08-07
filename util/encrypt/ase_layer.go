// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"io"
	"sync"
)

// The AES block size in bytes.
const BlockSize = 16

var blockBufPool = sync.Pool{
	New: func() interface{} { return make([]byte, BlockSize) },
}

// Writer implements an io.WriteCloser, it encrypt data using AES before writing to the underlying object.
type Writer struct {
	err     error
	w       io.WriteCloser
	n       int
	buf     []byte
	counter uint64
	nonce   uint64
	key     []byte
}

// NewWriter returns a new Writer which encrypt data using AES before writing to the underlying object.
func NewWriter(w io.WriteCloser, key []byte, nonce uint64) (*Writer, error) {
	k := len(key)
	switch k {
	default:
		return nil, aes.KeySizeError(k)
	case 16, 24, 32:
	}
	writer := &Writer{w: w, key: key, nonce: nonce}
	writer.buf = make([]byte, BlockSize)
	return writer, nil
}

// AvailableSize returns how many bytes are unused in the buffer.
func (w *Writer) AvailableSize() int { return BlockSize - w.n }

// Write implements the io.Writer interface.
func (w *Writer) Write(p []byte) (n int, err error) {
	for len(p) > w.AvailableSize() && w.err == nil {
		copiedNum := copy(w.buf[w.n:], p)
		w.n += copiedNum
		err = w.Flush()
		if err != nil {
			return
		}
		n += copiedNum
		p = p[copiedNum:]
	}
	if w.err != nil {
		return n, w.err
	}
	copiedNum := copy(w.buf[w.n:], p)
	w.n += copiedNum
	n += copiedNum
	return
}

// Buffered returns the number of bytes that have been written into the current buffer.
func (w *Writer) Buffered() int { return w.n }

// Flush writes all the buffered data to the underlying object.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if w.n == 0 {
		return nil
	}
	counter := blockBufPool.Get().([]byte)
	defer blockBufPool.Put(counter)
	encryptTextText := blockBufPool.Get().([]byte)
	defer blockBufPool.Put(encryptTextText)

	binary.LittleEndian.PutUint64(counter, w.nonce)
	binary.LittleEndian.PutUint64(counter[8:], w.counter)
	block, err := aes.NewCipher(w.key)
	if err != nil {
		return err
	}
	blockMode := cipher.NewCTR(block, counter)
	blockMode.XORKeyStream(encryptTextText[:w.n], w.buf[:w.n])

	n, err := w.w.Write(encryptTextText[:w.n])
	if n < w.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
		return err
	}
	w.n = 0
	w.counter++
	return nil
}

// Close implements the io.Closer interface.
func (w *Writer) Close() (err error) {
	err = w.Flush()
	if err != nil {
		return
	}
	return w.w.Close()
}

// Reader implements an io.ReadAt, reading from the input source after decrypting.
type Reader struct {
	r     io.ReaderAt
	nonce uint64
	key   []byte
}

// NewReader returns a new Reader which can read from the input source after decrypting.
func NewReader(r io.ReaderAt, key []byte, nonce uint64) (*Reader, error) {
	k := len(key)
	switch k {
	default:
		return nil, aes.KeySizeError(k)
	case 16, 24, 32:
	}
	reader := &Reader{r: r, key: key, nonce: nonce}
	return reader, nil
}

// ReadAt implements the io.ReadAt interface.
func (r *Reader) ReadAt(p []byte, off int64) (nn int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	offset := off % BlockSize
	startBlock := off / BlockSize
	cursor := startBlock * BlockSize

	buf := blockBufPool.Get().([]byte)
	defer blockBufPool.Put(buf)
	counterBuf := blockBufPool.Get().([]byte)
	defer blockBufPool.Put(counterBuf)
	decryptText := blockBufPool.Get().([]byte)
	defer blockBufPool.Put(decryptText)

	var n int
	counter := uint64(startBlock)
	for len(p) > 0 && err == nil {
		n, err = r.r.ReadAt(buf, cursor)
		if err != nil {
			if n == 0 || err != io.EOF {
				return nn, err
			}
			err = nil
			// continue if n > 0 and r.err is io.EOF
		}
		cursor += int64(n)
		binary.LittleEndian.PutUint64(counterBuf, r.nonce)
		binary.LittleEndian.PutUint64(counterBuf[8:], counter)
		block, err := aes.NewCipher(r.key)
		if err != nil {
			return nn, err
		}
		blockMode := cipher.NewCTR(block, counterBuf)
		blockMode.XORKeyStream(decryptText, buf)

		n1 := copy(p, decryptText[offset:n])
		nn += n1
		p = p[n1:]
		offset = 0
		counter++
	}
	return nn, err
}
