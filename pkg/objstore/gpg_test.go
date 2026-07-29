// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/ProtonMail/go-crypto/openpgp/armor"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
	openpgp "github.com/ProtonMail/go-crypto/openpgp/v2"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/stretchr/testify/require"
)

func TestGPGEncryptionStorage(t *testing.T) {
	entity := newGPGTestEntity(t)
	armoredPublicKey := serializeGPGTestKey(t, entity, false, true)
	binaryPublicKey := serializeGPGTestKey(t, entity, false, false)
	armoredPrivateKey := serializeGPGTestKey(t, entity, true, true)

	_, err := WithGPGEncryption(nil, armoredPublicKey)
	require.ErrorContains(t, err, "requires a storage")
	_, err = WithGPGEncryption(NewMemStorage(), []byte("not an OpenPGP key"))
	require.ErrorContains(t, err, "parse GPG public key")
	_, err = WithGPGEncryption(NewMemStorage(), armoredPrivateKey)
	require.ErrorContains(t, err, "must not contain private key material")

	plaintext := []byte("schema and row data produced by dumpling\n")
	testCases := []struct {
		name         string
		publicKey    []byte
		streaming    bool
		compressType compressedio.CompressType
	}{
		{name: "armored WriteFile", publicKey: armoredPublicKey},
		{name: "binary Create", publicKey: binaryPublicKey, streaming: true},
		{name: "compressed Create", publicKey: armoredPublicKey, streaming: true, compressType: compressedio.Gzip},
	}
	ctx := context.Background()
	for index, testCase := range testCases {
		inner := NewMemStorage()
		encrypted, err := WithGPGEncryption(inner, testCase.publicKey)
		require.NoErrorf(t, err, "case %s", testCase.name)
		suffixer, ok := encrypted.(interface{ EncryptedFileSuffix() string })
		require.Truef(t, ok, "case %s", testCase.name)
		require.Equal(t, ".gpg", suffixer.EncryptedFileSuffix(), testCase.name)

		storage := WithCompression(encrypted, testCase.compressType, compressedio.DecompressConfig{})
		name := "dump-file-" + string(rune('a'+index))
		if testCase.streaming {
			writer, err := storage.Create(ctx, name, nil)
			require.NoErrorf(t, err, "case %s", testCase.name)
			middle := len(plaintext) / 2
			_, err = writer.Write(ctx, plaintext[:middle])
			require.NoErrorf(t, err, "case %s", testCase.name)
			_, err = writer.Write(ctx, plaintext[middle:])
			require.NoErrorf(t, err, "case %s", testCase.name)
			require.NoErrorf(t, writer.Close(ctx), "case %s", testCase.name)
		} else {
			require.NoErrorf(t, storage.WriteFile(ctx, name, plaintext), "case %s", testCase.name)
		}

		ciphertext, err := inner.ReadFile(ctx, name)
		require.NoErrorf(t, err, "case %s", testCase.name)
		require.False(t, bytes.Contains(ciphertext, plaintext), testCase.name)
		decrypted := decryptGPGTestFile(t, entity, ciphertext)
		if testCase.compressType != compressedio.NoCompression {
			reader, err := compressedio.NewReader(testCase.compressType, compressedio.DecompressConfig{}, bytes.NewReader(decrypted))
			require.NoErrorf(t, err, "case %s", testCase.name)
			decrypted, err = io.ReadAll(reader)
			require.NoErrorf(t, err, "case %s", testCase.name)
		}
		require.Equal(t, plaintext, decrypted, testCase.name)

		if testCase.compressType == compressedio.NoCompression {
			readThroughWrapper, err := encrypted.ReadFile(ctx, name)
			require.NoErrorf(t, err, "case %s", testCase.name)
			require.Equal(t, ciphertext, readThroughWrapper, testCase.name)
		}
	}
}

func newGPGTestEntity(t *testing.T) *openpgp.Entity {
	t.Helper()
	entity, err := openpgp.NewEntity("Dumpling Test", "", "dumpling@example.com", &packet.Config{
		Algorithm:     packet.PubKeyAlgoEdDSA,
		Curve:         packet.Curve25519,
		DefaultCipher: packet.CipherAES256,
	})
	require.NoError(t, err)
	return entity
}

func serializeGPGTestKey(t *testing.T, entity *openpgp.Entity, private, armored bool) []byte {
	t.Helper()
	var buffer bytes.Buffer
	var output io.Writer = &buffer
	var armorWriter io.WriteCloser
	if armored {
		blockType := openpgp.PublicKeyType
		if private {
			blockType = openpgp.PrivateKeyType
		}
		var err error
		armorWriter, err = armor.Encode(&buffer, blockType, nil)
		require.NoError(t, err)
		output = armorWriter
	}
	var err error
	if private {
		err = entity.SerializePrivate(output, nil)
	} else {
		err = entity.Serialize(output)
	}
	require.NoError(t, err)
	if armorWriter != nil {
		require.NoError(t, armorWriter.Close())
	}
	return buffer.Bytes()
}

func decryptGPGTestFile(t *testing.T, entity *openpgp.Entity, ciphertext []byte) []byte {
	t.Helper()
	message, err := openpgp.ReadMessage(bytes.NewReader(ciphertext), openpgp.EntityList{entity}, nil, nil)
	require.NoError(t, err)
	plaintext, err := io.ReadAll(message.UnverifiedBody)
	require.NoError(t, err)
	return plaintext
}
