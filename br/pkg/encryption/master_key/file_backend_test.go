// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"context"
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TempKeyFile represents a temporary key file for testing
type TempKeyFile struct {
	Path string
	file *os.File
}

// Cleanup closes and removes the temporary file
func (tkf *TempKeyFile) Cleanup() {
	if tkf.file != nil {
		tkf.file.Close()
	}
	os.Remove(tkf.Path)
}

// createMasterKeyFile creates a temporary master key file for testing
func createMasterKeyFile() (*TempKeyFile, error) {
	tempFile, err := os.CreateTemp("", "test_key_*.txt")
	if err != nil {
		return nil, err
	}

	_, err = tempFile.WriteString("c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139\n")
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, err
	}

	return &TempKeyFile{
		Path: tempFile.Name(),
		file: tempFile,
	}, nil
}

func TestFileBackendAes256Gcm(t *testing.T) {
	pt, err := hex.DecodeString("25431587e9ecffc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749")
	require.NoError(t, err)
	ct, err := hex.DecodeString("84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980")
	require.NoError(t, err)
	ivBytes, err := hex.DecodeString("cafabd9672ca6c79a2fbdc22")
	require.NoError(t, err)

	tempKeyFile, err := createMasterKeyFile()
	require.NoError(t, err)
	defer tempKeyFile.Cleanup()

	backend, err := createFileBackend(tempKeyFile.Path)
	require.NoError(t, err)

	ctx := context.Background()
	iv, err := NewIVFromSlice(ivBytes)
	require.NoError(t, err)

	encryptedContent, err := backend.memCache.EncryptContent(ctx, pt, iv)
	require.NoError(t, err)
	require.Equal(t, ct, encryptedContent.Content)

	plaintext, err := backend.Decrypt(ctx, encryptedContent)
	require.NoError(t, err)
	require.Equal(t, pt, plaintext)
}

func TestFileBackendAuthenticate(t *testing.T) {
	pt := []byte{1, 2, 3}

	tempKeyFile, err := createMasterKeyFile()
	require.NoError(t, err)
	defer tempKeyFile.Cleanup()

	backend, err := createFileBackend(tempKeyFile.Path)
	require.NoError(t, err)

	ctx := context.Background()
	encryptedContent, err := backend.Encrypt(ctx, pt)
	require.NoError(t, err)

	plaintext, err := backend.Decrypt(ctx, encryptedContent)
	require.NoError(t, err)
	require.Equal(t, pt, plaintext)

	// Test checksum mismatch
	encryptedContent1 := *encryptedContent
	encryptedContent1.Metadata[MetadataKeyAesGcmTag][0] ^= 0xFF
	_, err = backend.Decrypt(ctx, &encryptedContent1)
	require.ErrorContains(t, err, wrongMasterKey)

	// Test checksum not found
	encryptedContent2 := *encryptedContent
	delete(encryptedContent2.Metadata, MetadataKeyAesGcmTag)
	_, err = backend.Decrypt(ctx, &encryptedContent2)
	require.ErrorContains(t, err, gcmTagNotFound)
}
