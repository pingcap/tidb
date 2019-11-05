// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package xtea implements XTEA encryption, as defined in Needham and Wheeler's
// 1997 technical report, "Tea extensions."
//
// XTEA is a legacy cipher and its short block size makes it vulnerable to
// birthday bound attacks (see https://sweet32.info). It should only be used
// where compatibility with legacy systems, not security, is the goal.
//
// Deprecated: any new system should use AES (from crypto/aes, if necessary in
// an AEAD mode like crypto/cipher.NewGCM) or XChaCha20-Poly1305 (from
// golang.org/x/crypto/chacha20poly1305).
package xtea // import "golang.org/x/crypto/xtea"

// For details, see http://www.cix.co.uk/~klockstone/xtea.pdf

import "strconv"

// The XTEA block size in bytes.
const BlockSize = 8

// A Cipher is an instance of an XTEA cipher using a particular key.
type Cipher struct {
	// table contains a series of precalculated values that are used each round.
	table [64]uint32
}

type KeySizeError int

func (k KeySizeError) Error() string {
	return "crypto/xtea: invalid key size " + strconv.Itoa(int(k))
}

// NewCipher creates and returns a new Cipher.
// The key argument should be the XTEA key.
// XTEA only supports 128 bit (16 byte) keys.
func NewCipher(key []byte) (*Cipher, error) {
	k := len(key)
	switch k {
	default:
		return nil, KeySizeError(k)
	case 16:
		break
	}

	c := new(Cipher)
	initCipher(c, key)

	return c, nil
}

// BlockSize returns the XTEA block size, 8 bytes.
// It is necessary to satisfy the Block interface in the
// package "crypto/cipher".
func (c *Cipher) BlockSize() int { return BlockSize }

// Encrypt encrypts the 8 byte buffer src using the key and stores the result in dst.
// Note that for amounts of data larger than a block,
// it is not safe to just call Encrypt on successive blocks;
// instead, use an encryption mode like CBC (see crypto/cipher/cbc.go).
func (c *Cipher) Encrypt(dst, src []byte) { encryptBlock(c, dst, src) }

// Decrypt decrypts the 8 byte buffer src using the key and stores the result in dst.
func (c *Cipher) Decrypt(dst, src []byte) { decryptBlock(c, dst, src) }

// initCipher initializes the cipher context by creating a look up table
// of precalculated values that are based on the key.
func initCipher(c *Cipher, key []byte) {
	// Load the key into four uint32s
	var k [4]uint32
	for i := 0; i < len(k); i++ {
		j := i << 2 // Multiply by 4
		k[i] = uint32(key[j+0])<<24 | uint32(key[j+1])<<16 | uint32(key[j+2])<<8 | uint32(key[j+3])
	}

	// Precalculate the table
	const delta = 0x9E3779B9
	var sum uint32

	// Two rounds of XTEA applied per loop
	for i := 0; i < numRounds; {
		c.table[i] = sum + k[sum&3]
		i++
		sum += delta
		c.table[i] = sum + k[(sum>>11)&3]
		i++
	}
}
