// Copyright 2026 PingCAP, Inc.
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

package pkdbcredential

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"

	"github.com/emmansun/gmsm/sm4"
)

const (
	envelopeMagic             = "TIDBCRED"
	envelopeVersion           = byte(0x01)
	envelopeAlgorithmSM4GCM   = byte(0x01)
	envelopeHeaderSize        = 16
	envelopeNonceSize         = 12
	envelopeTagSize           = 16
	authenticatedPrefixSize   = envelopeHeaderSize + envelopeNonceSize
	minimumEnvelopeSize       = authenticatedPrefixSize + envelopeTagSize
	maximumEnvelopeSize       = 65_536
	maximumCiphertextSize     = maximumEnvelopeSize - minimumEnvelopeSize
	ciphertextLengthOffset    = 12
	envelopeVersionOffset     = 8
	envelopeAlgorithmOffset   = 9
	envelopeNonceLengthOffset = 10
	envelopeTagLengthOffset   = 11
)

func seal(key, nonce, plaintext []byte) ([]byte, error) {
	aead, err := newSM4GCM(key)
	if err != nil {
		return nil, err
	}

	header := make([]byte, envelopeHeaderSize)
	copy(header, envelopeMagic)
	header[envelopeVersionOffset] = envelopeVersion
	header[envelopeAlgorithmOffset] = envelopeAlgorithmSM4GCM
	header[envelopeNonceLengthOffset] = envelopeNonceSize
	header[envelopeTagLengthOffset] = envelopeTagSize
	binary.BigEndian.PutUint32(header[ciphertextLengthOffset:], uint32(len(plaintext)))

	authenticatedPrefix := make([]byte, authenticatedPrefixSize)
	copy(authenticatedPrefix, header)
	copy(authenticatedPrefix[envelopeHeaderSize:], nonce)
	sealed := aead.Seal(nil, nonce, plaintext, authenticatedPrefix)
	return append(authenticatedPrefix, sealed...), nil
}

func openEnvelope(key, envelope []byte, path string) ([]byte, error) {
	if len(envelope) < minimumEnvelopeSize || len(envelope) > maximumEnvelopeSize {
		return nil, ErrInvalidEnvelope.GenWithStackByArgs(path)
	}
	if !bytes.Equal(envelope[:len(envelopeMagic)], []byte(envelopeMagic)) {
		return nil, ErrInvalidEnvelope.GenWithStackByArgs(path)
	}
	if envelope[envelopeVersionOffset] != envelopeVersion {
		return nil, ErrUnsupportedVersion.GenWithStackByArgs(envelope[envelopeVersionOffset], path)
	}
	if envelope[envelopeAlgorithmOffset] != envelopeAlgorithmSM4GCM {
		return nil, ErrUnsupportedAlgorithm.GenWithStackByArgs(envelope[envelopeAlgorithmOffset], path)
	}
	if envelope[envelopeNonceLengthOffset] != envelopeNonceSize || envelope[envelopeTagLengthOffset] != envelopeTagSize {
		return nil, ErrInvalidEnvelope.GenWithStackByArgs(path)
	}

	ciphertextSize := binary.BigEndian.Uint32(envelope[ciphertextLengthOffset:envelopeHeaderSize])
	if ciphertextSize > maximumCiphertextSize {
		return nil, ErrInvalidEnvelope.GenWithStackByArgs(path)
	}
	if len(envelope) != minimumEnvelopeSize+int(ciphertextSize) {
		return nil, ErrInvalidEnvelope.GenWithStackByArgs(path)
	}

	aead, err := newSM4GCM(key)
	if err != nil {
		return nil, ErrAuthenticationFailed.GenWithStackByArgs(path)
	}
	nonce := envelope[envelopeHeaderSize:authenticatedPrefixSize]
	sealed := envelope[authenticatedPrefixSize:]
	plaintext, err := aead.Open(nil, nonce, sealed, envelope[:authenticatedPrefixSize])
	if err != nil {
		return nil, ErrAuthenticationFailed.GenWithStackByArgs(path)
	}
	return plaintext, nil
}

func newSM4GCM(key []byte) (cipher.AEAD, error) {
	block, err := sm4.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
