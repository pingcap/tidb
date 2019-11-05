// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hkdf_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

// Usage example that expands one master secret into three other
// cryptographically secure keys.
func Example_usage() {
	// Underlying hash function for HMAC.
	hash := sha256.New

	// Cryptographically secure master secret.
	secret := []byte{0x00, 0x01, 0x02, 0x03} // i.e. NOT this.

	// Non-secret salt, optional (can be nil).
	// Recommended: hash-length random value.
	salt := make([]byte, hash().Size())
	if _, err := rand.Read(salt); err != nil {
		panic(err)
	}

	// Non-secret context info, optional (can be nil).
	info := []byte("hkdf example")

	// Generate three 128-bit derived keys.
	hkdf := hkdf.New(hash, secret, salt, info)

	var keys [][]byte
	for i := 0; i < 3; i++ {
		key := make([]byte, 16)
		if _, err := io.ReadFull(hkdf, key); err != nil {
			panic(err)
		}
		keys = append(keys, key)
	}

	for i := range keys {
		fmt.Printf("Key #%d: %v\n", i+1, !bytes.Equal(keys[i], make([]byte, 16)))
	}

	// Output:
	// Key #1: true
	// Key #2: true
	// Key #3: true
}
