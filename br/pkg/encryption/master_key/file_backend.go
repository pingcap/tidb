// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"context"
	"encoding/hex"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const AesGcmKeyLen = 32 // AES-256 key length

// FileBackend is ported from TiKV FileBackend
type FileBackend struct {
	memCache *MemAesGcmBackend
}

func createFileBackend(keyPath string) (*FileBackend, error) {
	// FileBackend uses AES-256-GCM
	keyLen := AesGcmKeyLen

	content, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read master key file from disk")
	}

	fileLen := len(content)
	expectedLen := keyLen*2 + 1 // hex-encoded key + newline

	if fileLen != expectedLen {
		return nil, errors.Errorf("mismatch master key file size, expected %d, actual %d", expectedLen, fileLen)
	}

	if content[fileLen-1] != '\n' {
		return nil, errors.Errorf("master key file should end with newline")
	}

	key, err := hex.DecodeString(string(content[:fileLen-1]))
	if err != nil {
		return nil, errors.Annotate(err, "failed to decode hex format master key from file")
	}

	backend, err := NewMemAesGcmBackend(key)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create MemAesGcmBackend")
	}

	return &FileBackend{memCache: backend}, nil
}

func (f *FileBackend) Encrypt(ctx context.Context, plaintext []byte) (*encryptionpb.EncryptedContent, error) {
	iv, err := NewIVGcm()
	if err != nil {
		return nil, err
	}
	return f.memCache.EncryptContent(ctx, plaintext, iv)
}

func (f *FileBackend) Decrypt(ctx context.Context, content *encryptionpb.EncryptedContent) ([]byte, error) {
	return f.memCache.DecryptContent(ctx, content)
}

func (f *FileBackend) Close() {
	// nothing to close
}
