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
	"errors"
	"io"
	"math/rand"
)

var errInvalidBlockSize = errors.New("invalid encrypt block size")

// defaultEncryptBlockSize indicates the default encrypt block size in bytes
const defaultEncryptBlockSize = 1024

// CtrCipher encrypting data using AES in counter mode
type CtrCipher struct {
	nonce uint64
	block cipher.Block
	// encryptBlockSize indicates the encrypt block size in bytes.
	encryptBlockSize int64
	// aesBlockCount indicates the total aes blocks in one encrypt block
	aesBlockCount int64
}

// NewCtrCipher return a CtrCipher using the default encrypt block size
func NewCtrCipher() (ctr *CtrCipher, err error) {
	return NewCtrCipherWithBlockSize(defaultEncryptBlockSize)
}

// NewCtrCipherWithBlockSize return a CtrCipher with the encrypt block size
func NewCtrCipherWithBlockSize(encryptBlockSize int64) (ctr *CtrCipher, err error) {
	key := make([]byte, aes.BlockSize)
	rand.Read(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if encryptBlockSize%aes.BlockSize != 0 {
		return nil, errInvalidBlockSize
	}
	ctr = new(CtrCipher)
	ctr.block = block
	ctr.nonce = rand.Uint64()
	ctr.encryptBlockSize = encryptBlockSize
	ctr.aesBlockCount = encryptBlockSize / aes.BlockSize
	return
}

// stream returns a cipher.Stream be use to encrypts/decrypts
func (ctr *CtrCipher) stream(counter uint64) cipher.Stream {
	counterBuf := make([]byte, aes.BlockSize)
	binary.BigEndian.PutUint64(counterBuf, ctr.nonce)
	binary.BigEndian.PutUint64(counterBuf[8:], counter)
	return cipher.NewCTR(ctr.block, counterBuf)
}

// Writer implements an io.WriteCloser, it encrypt data using AES before writing to the underlying object.
type Writer struct {
	err          error
	w            io.WriteCloser
	n            int
	buf          []byte
	cipherStream cipher.Stream
}

// NewWriter returns a new Writer which encrypt data using AES before writing to the underlying object.
func NewWriter(w io.WriteCloser, ctrCipher *CtrCipher) *Writer {
	writer := &Writer{w: w}
	writer.buf = make([]byte, ctrCipher.encryptBlockSize)
	writer.cipherStream = ctrCipher.stream(0)
	return writer
}

// AvailableSize returns how many bytes are unused in the buffer.
func (w *Writer) AvailableSize() int { return len(w.buf) - w.n }

// Write implements the io.Writer interface.
func (w *Writer) Write(p []byte) (n int, err error) {
	if w.err != nil {
		return n, w.err
	}
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
	w.cipherStream.XORKeyStream(w.buf[:w.n], w.buf[:w.n])
	n, err := w.w.Write(w.buf[:w.n])
	if n < w.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
		return err
	}
	w.n = 0
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
	r      io.ReaderAt
	cipher *CtrCipher
}

// NewReader returns a new Reader which can read from the input source after decrypting.
func NewReader(r io.ReaderAt, ctrCipher *CtrCipher) *Reader {
	reader := &Reader{r: r, cipher: ctrCipher}
	return reader
}

// ReadAt implements the io.ReadAt interface.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	offset := off % r.cipher.encryptBlockSize
	counter := (off / r.cipher.encryptBlockSize) * r.cipher.aesBlockCount
	cursor := off - offset

	buf := make([]byte, r.cipher.encryptBlockSize)
	var readNum int
	cipherStream := r.cipher.stream(uint64(counter))
	for len(p) > 0 && err == nil {
		readNum, err = r.r.ReadAt(buf, cursor)
		if err != nil {
			if readNum == 0 || err != io.EOF {
				return n, err
			}
			err = nil
			// continue if n > 0 and r.err is io.EOF
		}
		cursor += int64(readNum)
		cipherStream.XORKeyStream(buf[:readNum], buf[:readNum])
		copiedNum := copy(p, buf[offset:readNum])
		n += copiedNum
		p = p[copiedNum:]
		offset = 0
	}
	return n, err
}
