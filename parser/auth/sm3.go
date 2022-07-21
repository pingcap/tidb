// Copyright 2022 PingCAP, Inc.
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

package auth

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"strconv"
)

// The concrete SM3 Cryptographic Hash Algorithm can be accessed in http://www.sca.gov.cn/sca/xwdt/2010-12/17/content_1002389.shtml
// This implementation of 'type sm3 struct' is modified from https://github.com/tjfoc/gmsm/tree/601ddb090dcf53d7951cc4dcc66276e2b817837c/sm3
// Some other references:
// 	https://tools.ietf.org/id/draft-oscca-cfrg-sm3-01.html

/*
Copyright Suzhou Tongji Fintech Research Institute 2017 All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
                 http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

type sm3 struct {
	digest      [8]uint32 // digest represents the partial evaluation of V
	length      uint64    // length of the message
	unhandleMsg []byte
	blockSize   int
	size        int
}

func ff0(x, y, z uint32) uint32 { return x ^ y ^ z }

func ff1(x, y, z uint32) uint32 { return (x & y) | (x & z) | (y & z) }

func gg0(x, y, z uint32) uint32 { return x ^ y ^ z }

func gg1(x, y, z uint32) uint32 { return (x & y) | (^x & z) }

func p0(x uint32) uint32 { return x ^ leftRotate(x, 9) ^ leftRotate(x, 17) }

func p1(x uint32) uint32 { return x ^ leftRotate(x, 15) ^ leftRotate(x, 23) }

func leftRotate(x uint32, i uint32) uint32 { return x<<(i%32) | x>>(32-i%32) }

func (sm3 *sm3) pad() []byte {
	msg := sm3.unhandleMsg
	// Append '1'
	msg = append(msg, 0x80)
	// Append until the resulting message length (in bits) is congruent to 448 (mod 512)
	blockSize := 64
	for i := len(msg); i%blockSize != 56; i++ {
		msg = append(msg, 0x00)
	}
	// append message length
	msg = append(msg, uint8(sm3.length>>56&0xff))
	msg = append(msg, uint8(sm3.length>>48&0xff))
	msg = append(msg, uint8(sm3.length>>40&0xff))
	msg = append(msg, uint8(sm3.length>>32&0xff))
	msg = append(msg, uint8(sm3.length>>24&0xff))
	msg = append(msg, uint8(sm3.length>>16&0xff))
	msg = append(msg, uint8(sm3.length>>8&0xff))
	msg = append(msg, uint8(sm3.length>>0&0xff))

	return msg
}

func (sm3 *sm3) update(msg []byte) [8]uint32 {
	var w [68]uint32
	var w1 [64]uint32

	a, b, c, d, e, f, g, h := sm3.digest[0], sm3.digest[1], sm3.digest[2], sm3.digest[3], sm3.digest[4], sm3.digest[5], sm3.digest[6], sm3.digest[7]
	for len(msg) >= 64 {
		for i := 0; i < 16; i++ {
			w[i] = binary.BigEndian.Uint32(msg[4*i : 4*(i+1)])
		}
		for i := 16; i < 68; i++ {
			w[i] = p1(w[i-16]^w[i-9]^leftRotate(w[i-3], 15)) ^ leftRotate(w[i-13], 7) ^ w[i-6]
		}
		for i := 0; i < 64; i++ {
			w1[i] = w[i] ^ w[i+4]
		}
		a1, b1, c1, d1, e1, f1, g1, h1 := a, b, c, d, e, f, g, h
		for i := 0; i < 16; i++ {
			ss1 := leftRotate(leftRotate(a1, 12)+e1+leftRotate(0x79cc4519, uint32(i)), 7)
			ss2 := ss1 ^ leftRotate(a1, 12)
			tt1 := ff0(a1, b1, c1) + d1 + ss2 + w1[i]
			tt2 := gg0(e1, f1, g1) + h1 + ss1 + w[i]
			d1 = c1
			c1 = leftRotate(b1, 9)
			b1 = a1
			a1 = tt1
			h1 = g1
			g1 = leftRotate(f1, 19)
			f1 = e1
			e1 = p0(tt2)
		}
		for i := 16; i < 64; i++ {
			ss1 := leftRotate(leftRotate(a1, 12)+e1+leftRotate(0x7a879d8a, uint32(i)), 7)
			ss2 := ss1 ^ leftRotate(a1, 12)
			tt1 := ff1(a1, b1, c1) + d1 + ss2 + w1[i]
			tt2 := gg1(e1, f1, g1) + h1 + ss1 + w[i]
			d1 = c1
			c1 = leftRotate(b1, 9)
			b1 = a1
			a1 = tt1
			h1 = g1
			g1 = leftRotate(f1, 19)
			f1 = e1
			e1 = p0(tt2)
		}
		a ^= a1
		b ^= b1
		c ^= c1
		d ^= d1
		e ^= e1
		f ^= f1
		g ^= g1
		h ^= h1
		msg = msg[64:]
	}
	var digest [8]uint32
	digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7] = a, b, c, d, e, f, g, h
	return digest
}

// BlockSize returns the hash's underlying block size.
// The Write method must be able to accept any amount of data,
// but it may operate more efficiently if all writes are a multiple of the block size.
func (sm3 *sm3) BlockSize() int { return sm3.blockSize }

// Size returns the number of bytes Sum will return.
func (sm3 *sm3) Size() int { return sm3.size }

// Reset clears the internal state by zeroing bytes in the state buffer.
// This can be skipped for a newly-created hash state; the default zero-allocated state is correct.
func (sm3 *sm3) Reset() {
	// Reset digest
	sm3.digest[0] = 0x7380166f
	sm3.digest[1] = 0x4914b2b9
	sm3.digest[2] = 0x172442d7
	sm3.digest[3] = 0xda8a0600
	sm3.digest[4] = 0xa96f30bc
	sm3.digest[5] = 0x163138aa
	sm3.digest[6] = 0xe38dee4d
	sm3.digest[7] = 0xb0fb0e4e

	sm3.length = 0
	sm3.unhandleMsg = []byte{}
	sm3.blockSize = 64
	sm3.size = 32
}

// Write (via the embedded io.Writer interface) adds more data to the running hash.
// It never returns an error.
func (sm3 *sm3) Write(p []byte) (int, error) {
	toWrite := len(p)
	sm3.length += uint64(len(p) * 8)
	msg := append(sm3.unhandleMsg, p...)
	nblocks := len(msg) / sm3.BlockSize()
	sm3.digest = sm3.update(msg)
	sm3.unhandleMsg = msg[nblocks*sm3.BlockSize():]

	return toWrite, nil
}

// Sum appends the current hash to b and returns the resulting slice.
// It does not change the underlying hash state.
func (sm3 *sm3) Sum(in []byte) []byte {
	_, _ = sm3.Write(in)
	msg := sm3.pad()
	// Finalize
	digest := sm3.update(msg)

	// save hash to in
	needed := sm3.Size()
	if cap(in)-len(in) < needed {
		newIn := make([]byte, len(in), len(in)+needed)
		copy(newIn, in)
		in = newIn
	}
	out := in[len(in) : len(in)+needed]
	for i := 0; i < 8; i++ {
		binary.BigEndian.PutUint32(out[i*4:], digest[i])
	}
	return out
}

// NewSM3 returns a new hash.Hash computing the SM3 checksum.
func NewSM3() hash.Hash {
	var h sm3
	h.Reset()
	return &h
}

// SM3 returns the sm3 checksum of the data.
func SM3(data []byte) []byte {
	h := NewSM3()
	h.Write(data)
	return h.Sum(nil)
}

func sm3crypt(plaintext string, salt []byte, iterations int) string {
	// Numbers in the comments refer to the description of the algorithm on https://www.akkadia.org/drepper/SHA-crypt.txt

	// 1, 2, 3
	bufA := bytes.NewBuffer(make([]byte, 0, 4096))
	bufA.Write([]byte(plaintext))
	bufA.Write(salt)

	// 4, 5, 6, 7, 8
	bufB := bytes.NewBuffer(make([]byte, 0, 4096))
	bufB.Write([]byte(plaintext))
	bufB.Write(salt)
	bufB.Write([]byte(plaintext))
	sumB := SM3(bufB.Bytes())
	bufB.Reset()

	// 9, 10
	var i int
	for i = len(plaintext); i > MIXCHARS; i -= MIXCHARS {
		bufA.Write(sumB[:MIXCHARS])
	}
	bufA.Write(sumB[:i])

	// 11
	for i = len(plaintext); i > 0; i >>= 1 {
		if i%2 == 0 {
			bufA.Write([]byte(plaintext))
		} else {
			bufA.Write(sumB[:])
		}
	}

	// 12
	sumA := SM3(bufA.Bytes())
	bufA.Reset()

	// 13, 14, 15
	bufDP := bufA
	for range []byte(plaintext) {
		bufDP.Write([]byte(plaintext))
	}
	sumDP := SM3(bufDP.Bytes())
	bufDP.Reset()

	// 16
	p := make([]byte, 0, sha256.Size)
	for i = len(plaintext); i > 0; i -= MIXCHARS {
		if i > MIXCHARS {
			p = append(p, sumDP[:]...)
		} else {
			p = append(p, sumDP[0:i]...)
		}
	}

	// 17, 18, 19
	bufDS := bufA
	for i = 0; i < 16+int(sumA[0]); i++ {
		bufDS.Write(salt)
	}
	sumDS := SM3(bufDS.Bytes())
	bufDS.Reset()

	// 20
	s := make([]byte, 0, 32)
	for i = len(salt); i > 0; i -= MIXCHARS {
		if i > MIXCHARS {
			s = append(s, sumDS[:]...)
		} else {
			s = append(s, sumDS[0:i]...)
		}
	}

	// 21
	bufC := bufA
	var sumC []byte
	for i = 0; i < iterations; i++ {
		bufC.Reset()
		if i&1 != 0 {
			bufC.Write(p)
		} else {
			bufC.Write(sumA[:])
		}
		if i%3 != 0 {
			bufC.Write(s)
		}
		if i%7 != 0 {
			bufC.Write(p)
		}
		if i&1 != 0 {
			bufC.Write(sumA[:])
		} else {
			bufC.Write(p)
		}
		sumC = SM3(bufC.Bytes())
		sumA = sumC
	}

	// 22
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	buf.Write([]byte{'$', 'B', '$'})
	rounds := fmt.Sprintf("%03d", iterations/ITERATION_MULTIPLIER)
	buf.Write([]byte(rounds))
	buf.Write([]byte{'$'})
	buf.Write(salt)

	b64From24bit([]byte{sumC[0], sumC[10], sumC[20]}, 4, buf)
	b64From24bit([]byte{sumC[21], sumC[1], sumC[11]}, 4, buf)
	b64From24bit([]byte{sumC[12], sumC[22], sumC[2]}, 4, buf)
	b64From24bit([]byte{sumC[3], sumC[13], sumC[23]}, 4, buf)
	b64From24bit([]byte{sumC[24], sumC[4], sumC[14]}, 4, buf)
	b64From24bit([]byte{sumC[15], sumC[25], sumC[5]}, 4, buf)
	b64From24bit([]byte{sumC[6], sumC[16], sumC[26]}, 4, buf)
	b64From24bit([]byte{sumC[27], sumC[7], sumC[17]}, 4, buf)
	b64From24bit([]byte{sumC[18], sumC[28], sumC[8]}, 4, buf)
	b64From24bit([]byte{sumC[9], sumC[19], sumC[29]}, 4, buf)
	b64From24bit([]byte{0, sumC[31], sumC[30]}, 3, buf)

	return buf.String()
}

// CheckSM3Password checks if a sm3 authentication string matches a password
func CheckSM3Password(pwhash []byte, password string) (bool, error) {
	pwhashParts := bytes.Split(pwhash, []byte("$"))
	if len(pwhashParts) != 4 {
		return false, errors.New("failed to decode hash parts")
	}

	hashType := string(pwhashParts[1])
	if hashType != "B" {
		return false, errors.New("digest type is incompatible")
	}

	iterations, err := strconv.Atoi(string(pwhashParts[2]))
	if err != nil {
		return false, errors.New("failed to decode iterations")
	}
	iterations = iterations * ITERATION_MULTIPLIER
	salt := pwhashParts[3][:SALT_LENGTH]

	newHash := sm3crypt(password, salt, iterations)

	return bytes.Equal(pwhash, []byte(newHash)), nil
}

// NewSM3Password creates a new SM3 password hash
func NewSM3Password(pwd string) string {
	salt := make([]byte, SALT_LENGTH)
	rand.Read(salt)

	// Restrict to 7-bit to avoid multi-byte UTF-8
	for i := range salt {
		salt[i] = salt[i] &^ 128
		for salt[i] == 36 || salt[i] == 0 { // '$' or NUL
			newval := make([]byte, 1)
			rand.Read(newval)
			salt[i] = newval[0] &^ 128
		}
	}

	return sm3crypt(pwd, salt, 5*ITERATION_MULTIPLIER)
}
