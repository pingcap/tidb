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
	"encoding/binary"
	"hash"
)

// The concrete Sm3Hash Cryptographic Hash Algorithm can be accessed in http://www.sca.gov.cn/sca/xwdt/2010-12/17/content_1002389.shtml
// This implementation of 'type sm3 struct' is modified from https://github.com/tjfoc/gmsm/tree/601ddb090dcf53d7951cc4dcc66276e2b817837c/sm3
// Some other references:
// 	https://datatracker.ietf.org/doc/draft-sca-cfrg-sm3/

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

// NewSM3 returns a new hash.Hash computing the Sm3Hash checksum.
func NewSM3() hash.Hash {
	var h sm3
	h.Reset()
	return &h
}

// Sm3Hash returns the sm3 checksum of the data.
func Sm3Hash(data []byte) []byte {
	h := NewSM3()
	h.Write(data)
	return h.Sum(nil)
}
