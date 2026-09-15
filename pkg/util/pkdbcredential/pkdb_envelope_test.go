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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	fixedKey       = mustDecodeHex("0123456789abcdeffedcba9876543210")
	fixedNonce     = mustDecodeHex("00001234567800000000abcd")
	fixedPlaintext = []byte("TiDB-P@ssw0rd!")
	fixedEnvelope  = mustDecodeHex("544944424352454401010c100000000e00001234567800000000abcde93077180b9d3f37d11c5750b65ec7d99b6eebfac0516025405541bd1efc")
)

func TestFixedEnvelopeVector(t *testing.T) {
	envelope, err := seal(fixedKey, fixedNonce, fixedPlaintext)
	require.NoError(t, err)
	require.Equal(t, fixedEnvelope, envelope)

	plaintext, err := openEnvelope(fixedKey, fixedEnvelope, "fixed-vector")
	require.NoError(t, err)
	require.Equal(t, fixedPlaintext, plaintext)
}

func TestEnvelopeRoundTrip(t *testing.T) {
	testCases := []struct {
		name      string
		plaintext []byte
	}{
		{name: "empty", plaintext: []byte{}},
		{name: "ASCII", plaintext: []byte("plain password")},
		{name: "UTF-8", plaintext: []byte("密码-安全")},
		{name: "binary", plaintext: []byte{0x00, 0xff, '\n', '\r', 0x80}},
		{name: "maximum", plaintext: make([]byte, maximumCiphertextSize)},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			envelope, err := seal(fixedKey, fixedNonce, testCase.plaintext)
			require.NoError(t, err)
			require.Len(t, envelope, minimumEnvelopeSize+len(testCase.plaintext))

			plaintext, err := openEnvelope(fixedKey, envelope, "round-trip")
			require.NoError(t, err)
			require.Equal(t, string(testCase.plaintext), string(plaintext))
		})
	}
}

func TestEnvelopeStructureErrors(t *testing.T) {
	validEnvelope, err := seal(fixedKey, fixedNonce, []byte("password"))
	require.NoError(t, err)

	withMutation := func(offset int, value byte) []byte {
		mutated := append([]byte(nil), validEnvelope...)
		mutated[offset] = value
		return mutated
	}
	withCiphertextLength := func(size uint32) []byte {
		mutated := append([]byte(nil), validEnvelope...)
		binary.BigEndian.PutUint32(mutated[ciphertextLengthOffset:envelopeHeaderSize], size)
		return mutated
	}

	testCases := []struct {
		name     string
		envelope []byte
		expected error
	}{
		{name: "too small", envelope: make([]byte, minimumEnvelopeSize-1), expected: ErrInvalidEnvelope},
		{name: "too large", envelope: make([]byte, maximumEnvelopeSize+1), expected: ErrInvalidEnvelope},
		{name: "invalid magic", envelope: withMutation(0, validEnvelope[0]^1), expected: ErrInvalidEnvelope},
		{name: "unsupported version", envelope: withMutation(envelopeVersionOffset, 2), expected: ErrUnsupportedVersion},
		{name: "unsupported algorithm", envelope: withMutation(envelopeAlgorithmOffset, 2), expected: ErrUnsupportedAlgorithm},
		{name: "invalid nonce size", envelope: withMutation(envelopeNonceLengthOffset, envelopeNonceSize-1), expected: ErrInvalidEnvelope},
		{name: "invalid tag size", envelope: withMutation(envelopeTagLengthOffset, envelopeTagSize-1), expected: ErrInvalidEnvelope},
		{name: "ciphertext exceeds limit", envelope: withCiphertextLength(maximumCiphertextSize + 1), expected: ErrInvalidEnvelope},
		{name: "inconsistent ciphertext size", envelope: withCiphertextLength(0), expected: ErrInvalidEnvelope},
		{name: "truncated", envelope: validEnvelope[:len(validEnvelope)-1], expected: ErrInvalidEnvelope},
		{name: "trailing data", envelope: append(append([]byte(nil), validEnvelope...), 0), expected: ErrInvalidEnvelope},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := openEnvelope(fixedKey, testCase.envelope, "invalid-envelope")
			require.ErrorIs(t, err, testCase.expected)
		})
	}
}

func TestEnvelopeRejectsOneBitMutationInEveryHeaderField(t *testing.T) {
	validEnvelope, err := seal(fixedKey, fixedNonce, []byte("password"))
	require.NoError(t, err)

	fields := []struct {
		name     string
		offset   int
		expected error
	}{
		{name: "magic", offset: 0, expected: ErrInvalidEnvelope},
		{name: "version", offset: envelopeVersionOffset, expected: ErrUnsupportedVersion},
		{name: "algorithm", offset: envelopeAlgorithmOffset, expected: ErrUnsupportedAlgorithm},
		{name: "nonce length", offset: envelopeNonceLengthOffset, expected: ErrInvalidEnvelope},
		{name: "tag length", offset: envelopeTagLengthOffset, expected: ErrInvalidEnvelope},
		{name: "ciphertext length", offset: envelopeHeaderSize - 1, expected: ErrInvalidEnvelope},
	}
	for _, field := range fields {
		t.Run(field.name, func(t *testing.T) {
			mutated := append([]byte(nil), validEnvelope...)
			mutated[field.offset] ^= 1
			_, err := openEnvelope(fixedKey, mutated, "modified-header")
			require.ErrorIs(t, err, field.expected)
		})
	}
}

func TestEnvelopeRejectsEveryUnsupportedVersionAndAlgorithm(t *testing.T) {
	validEnvelope, err := seal(fixedKey, fixedNonce, []byte("password"))
	require.NoError(t, err)

	for value := 0; value <= 255; value++ {
		if byte(value) != envelopeVersion {
			mutated := append([]byte(nil), validEnvelope...)
			mutated[envelopeVersionOffset] = byte(value)
			_, err := openEnvelope(fixedKey, mutated, "unsupported-version")
			require.ErrorIs(t, err, ErrUnsupportedVersion, "version %d", value)
		}
		if byte(value) != envelopeAlgorithmSM4GCM {
			mutated := append([]byte(nil), validEnvelope...)
			mutated[envelopeAlgorithmOffset] = byte(value)
			_, err := openEnvelope(fixedKey, mutated, "unsupported-algorithm")
			require.ErrorIs(t, err, ErrUnsupportedAlgorithm, "algorithm %d", value)
		}
	}
}

func TestEnvelopeRejectsTruncationAtFieldBoundaries(t *testing.T) {
	validEnvelope, err := seal(fixedKey, fixedNonce, []byte("password"))
	require.NoError(t, err)

	boundaries := []int{
		0,
		len(envelopeMagic),
		envelopeVersionOffset + 1,
		envelopeAlgorithmOffset + 1,
		envelopeNonceLengthOffset + 1,
		envelopeTagLengthOffset + 1,
		envelopeHeaderSize,
		authenticatedPrefixSize,
		len(validEnvelope) - envelopeTagSize,
		len(validEnvelope) - 1,
	}
	for _, boundary := range boundaries {
		_, err := openEnvelope(fixedKey, validEnvelope[:boundary], "truncated-envelope")
		require.ErrorIs(t, err, ErrInvalidEnvelope, "boundary %d", boundary)
	}
}

func TestEnvelopeAuthenticationErrors(t *testing.T) {
	validEnvelope, err := seal(fixedKey, fixedNonce, []byte("password"))
	require.NoError(t, err)

	wrongKey := append([]byte(nil), fixedKey...)
	wrongKey[0] ^= 1
	_, err = openEnvelope(wrongKey, validEnvelope, "wrong-key")
	require.ErrorIs(t, err, ErrAuthenticationFailed)

	for _, offset := range []int{envelopeHeaderSize, authenticatedPrefixSize, len(validEnvelope) - 1} {
		mutated := append([]byte(nil), validEnvelope...)
		mutated[offset] ^= 1
		_, err = openEnvelope(fixedKey, mutated, "modified-envelope")
		require.ErrorIs(t, err, ErrAuthenticationFailed)
		require.False(t, strings.Contains(err.Error(), "message authentication failed"))
	}
}

func FuzzOpenEnvelope(f *testing.F) {
	f.Add([]byte{})
	f.Add(fixedEnvelope)
	f.Fuzz(func(t *testing.T, envelope []byte) {
		_, err := openEnvelope(fixedKey, envelope, "fuzz")
		if err != nil {
			require.True(t,
				errors.Is(err, ErrInvalidEnvelope) ||
					errors.Is(err, ErrUnsupportedVersion) ||
					errors.Is(err, ErrUnsupportedAlgorithm) ||
					errors.Is(err, ErrAuthenticationFailed),
			)
		}
	})
}

func mustDecodeHex(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return decoded
}
