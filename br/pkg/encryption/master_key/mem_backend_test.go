// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMemAesGcmBackend(t *testing.T) {
	key := make([]byte, 32) // 256-bit key
	_, err := NewMemAesGcmBackend(key)
	require.NoError(t, err, "Failed to create MemAesGcmBackend")

	shortKey := make([]byte, 16)
	_, err = NewMemAesGcmBackend(shortKey)
	require.Error(t, err, "Expected error for short key")
}

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	backend, err := NewMemAesGcmBackend(key)
	require.NoError(t, err, "Failed to create MemAesGcmBackend")

	plaintext := []byte("Hello, World!")

	iv, err := NewIVGcm()
	require.NoError(t, err, "failed to create gcm iv")

	ctx := context.Background()
	encrypted, err := backend.EncryptContent(ctx, plaintext, iv)
	require.NoError(t, err, "Encryption failed")

	decrypted, err := backend.DecryptContent(ctx, encrypted)
	require.NoError(t, err, "Decryption failed")

	require.Equal(t, plaintext, decrypted, "Decrypted text doesn't match original")
}

func TestDecryptWithWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	for i := range key2 {
		key2[i] = 1 // Different from key1
	}

	backend1, _ := NewMemAesGcmBackend(key1)
	backend2, _ := NewMemAesGcmBackend(key2)

	plaintext := []byte("Hello, World!")

	iv, err := NewIVGcm()
	require.NoError(t, err, "failed to create gcm iv")

	ctx := context.Background()
	encrypted, _ := backend1.EncryptContent(ctx, plaintext, iv)
	_, err = backend2.DecryptContent(ctx, encrypted)
	require.Error(t, err, "Expected decryption with wrong key to fail")
}

func TestDecryptWithTamperedCiphertext(t *testing.T) {
	key := make([]byte, 32)
	backend, _ := NewMemAesGcmBackend(key)

	plaintext := []byte("Hello, World!")

	iv, err := NewIVGcm()
	require.NoError(t, err, "failed to create gcm iv")

	ctx := context.Background()
	encrypted, _ := backend.EncryptContent(ctx, plaintext, iv)
	encrypted.Content[0] ^= 1 // Tamper with the ciphertext

	_, err = backend.DecryptContent(ctx, encrypted)
	require.Error(t, err, "Expected decryption of tampered ciphertext to fail")
}

func TestDecryptWithMissingMetadata(t *testing.T) {
	key := make([]byte, 32)
	backend, _ := NewMemAesGcmBackend(key)

	plaintext := []byte("Hello, World!")

	iv, err := NewIVGcm()
	require.NoError(t, err, "failed to create gcm iv")

	ctx := context.Background()
	encrypted, _ := backend.EncryptContent(ctx, plaintext, iv)
	delete(encrypted.Metadata, MetadataKeyMethod)

	_, err = backend.DecryptContent(ctx, encrypted)
	require.Error(t, err, "Expected decryption with missing metadata to fail")
}

func TestEncryptDecryptLargeData(t *testing.T) {
	key := make([]byte, 32)
	backend, _ := NewMemAesGcmBackend(key)

	plaintext := make([]byte, 1000000) // 1 MB of data

	iv, err := NewIVGcm()
	require.NoError(t, err, "failed to create gcm iv")

	ctx := context.Background()
	encrypted, err := backend.EncryptContent(ctx, plaintext, iv)
	require.NoError(t, err, "Encryption of large data failed")

	decrypted, err := backend.DecryptContent(ctx, encrypted)
	require.NoError(t, err, "Decryption of large data failed")

	require.True(t, bytes.Equal(plaintext, decrypted), "Decrypted large data doesn't match original")
}
