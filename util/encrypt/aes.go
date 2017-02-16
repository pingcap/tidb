// Copyright 2017 PingCAP, Inc.
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
	"bytes"
	"crypto/aes"
	"crypto/cipher"

	"github.com/juju/errors"
)

type ecb struct {
	b         cipher.Block
	blockSize int
}

func newECB(b cipher.Block) *ecb {
	return &ecb{
		b:         b,
		blockSize: b.BlockSize(),
	}
}

type ecbEncrypter ecb

// BlockSize implements BlockMode.BlockSize interface.
func (x *ecbEncrypter) BlockSize() int { return x.blockSize }

// CryptBlocks implements BlockMode.CryptBlocks interface.
func (x *ecbEncrypter) CryptBlocks(dst, src []byte) {
	if len(src)%x.blockSize != 0 {
		panic("ECBEncrypter: input not full blocks")
	}
	if len(dst) < len(src) {
		panic("ECBEncrypter: output smaller than input")
	}
	// See https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Electronic_Codebook_.28ECB.29
	for len(src) > 0 {
		x.b.Encrypt(dst, src[:x.blockSize])
		src = src[x.blockSize:]
		dst = dst[x.blockSize:]
	}
	return
}

// newECBEncrypter creates an AES encrypter with ecb mode.
func newECBEncrypter(b cipher.Block) cipher.BlockMode {
	return (*ecbEncrypter)(newECB(b))
}

type ecbDecrypter ecb

// BlockSize implements BlockMode.BlockSize interface.
func (x *ecbDecrypter) BlockSize() int { return x.blockSize }

// CryptBlocks implements BlockMode.CryptBlocks interface.
func (x *ecbDecrypter) CryptBlocks(dst, src []byte) {
	if len(src)%x.blockSize != 0 {
		panic("ECBDecrypter: input not full blocks")
	}
	if len(dst) < len(src) {
		panic("ECBDecrypter: output smaller than input")
	}
	// See https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Electronic_Codebook_.28ECB.29
	for len(src) > 0 {
		x.b.Decrypt(dst, src[:x.blockSize])
		src = src[x.blockSize:]
		dst = dst[x.blockSize:]
	}
}

func newECBDecrypter(b cipher.Block) cipher.BlockMode {
	return (*ecbDecrypter)(newECB(b))
}

// Padding using PKCS7
// See https://en.wikipedia.org/wiki/Padding_(cryptography)#PKCS7
func pkcs7Padding(ciphertext []byte, blockSize int) []byte {
	// The bytes need to padding.
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

// UnPadding using PKCS7
// See https://en.wikipedia.org/wiki/Padding_(cryptography)#PKCS7
func pkcs5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

// AESEncryptWithECB encrypts data using AES with ECB mode.
func AESEncryptWithECB(str, key []byte) ([]byte, error) {
	cb, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	blockSize := cb.BlockSize()
	// The str arguments can be any length, and padding is automatically added to
	// str so it is a multiple of a block as required by block-based algorithms such as AES.
	// This padding is automatically removed by the AES_DECRYPT() function.
	data := pkcs7Padding(str, blockSize)
	crypted := make([]byte, len(data))
	ecb := newECBEncrypter(cb)
	ecb.CryptBlocks(crypted, data)
	return crypted, nil
}

// AESDecryptWithECB decrypts data using AES with ECB mode.
func AESDecryptWithECB(cryptStr, key []byte) ([]byte, error) {
	cb, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	mode := newECBDecrypter(cb)
	data := make([]byte, len(cryptStr))
	mode.CryptBlocks(data, cryptStr)
	d := pkcs5UnPadding(data)
	return d, nil
}
