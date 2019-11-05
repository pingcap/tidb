// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build go1.7,amd64,!gccgo,!appengine

package chacha20poly1305

import (
	"encoding/binary"

	"golang.org/x/crypto/internal/subtle"
	"golang.org/x/sys/cpu"
)

//go:noescape
func chacha20Poly1305Open(dst []byte, key []uint32, src, ad []byte) bool

//go:noescape
func chacha20Poly1305Seal(dst []byte, key []uint32, src, ad []byte)

var (
	useAVX2 = cpu.X86.HasAVX2 && cpu.X86.HasBMI2
)

// setupState writes a ChaCha20 input matrix to state. See
// https://tools.ietf.org/html/rfc7539#section-2.3.
func setupState(state *[16]uint32, key *[8]uint32, nonce []byte) {
	state[0] = 0x61707865
	state[1] = 0x3320646e
	state[2] = 0x79622d32
	state[3] = 0x6b206574

	state[4] = key[0]
	state[5] = key[1]
	state[6] = key[2]
	state[7] = key[3]
	state[8] = key[4]
	state[9] = key[5]
	state[10] = key[6]
	state[11] = key[7]

	state[12] = 0
	state[13] = binary.LittleEndian.Uint32(nonce[:4])
	state[14] = binary.LittleEndian.Uint32(nonce[4:8])
	state[15] = binary.LittleEndian.Uint32(nonce[8:12])
}

func (c *chacha20poly1305) seal(dst, nonce, plaintext, additionalData []byte) []byte {
	if !cpu.X86.HasSSSE3 {
		return c.sealGeneric(dst, nonce, plaintext, additionalData)
	}

	var state [16]uint32
	setupState(&state, &c.key, nonce)

	ret, out := sliceForAppend(dst, len(plaintext)+16)
	if subtle.InexactOverlap(out, plaintext) {
		panic("chacha20poly1305: invalid buffer overlap")
	}
	chacha20Poly1305Seal(out[:], state[:], plaintext, additionalData)
	return ret
}

func (c *chacha20poly1305) open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
	if !cpu.X86.HasSSSE3 {
		return c.openGeneric(dst, nonce, ciphertext, additionalData)
	}

	var state [16]uint32
	setupState(&state, &c.key, nonce)

	ciphertext = ciphertext[:len(ciphertext)-16]
	ret, out := sliceForAppend(dst, len(ciphertext))
	if subtle.InexactOverlap(out, ciphertext) {
		panic("chacha20poly1305: invalid buffer overlap")
	}
	if !chacha20Poly1305Open(out, state[:], ciphertext, additionalData) {
		for i := range out {
			out[i] = 0
		}
		return nil, errOpen
	}

	return ret, nil
}
