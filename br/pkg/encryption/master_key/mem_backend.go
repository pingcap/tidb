// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/kms"
)

const (
	gcmTagNotFound = "aes gcm tag not found"
	wrongMasterKey = "wrong master key"
)

type MemAesGcmBackend struct {
	key *kms.PlainKey
}

func NewMemAesGcmBackend(key []byte) (*MemAesGcmBackend, error) {
	plainKey, err := kms.NewPlainKey(key, kms.CryptographyTypeAesGcm256)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create new mem aes gcm backend")
	}
	return &MemAesGcmBackend{
		key: plainKey,
	}, nil
}

func (m *MemAesGcmBackend) EncryptContent(_ctx context.Context, plaintext []byte, iv IV) (
	*encryptionpb.EncryptedContent, error) {
	content := encryptionpb.EncryptedContent{
		Metadata: make(map[string][]byte),
	}
	content.Metadata[MetadataKeyMethod] = []byte(MetadataMethodAes256Gcm)
	content.Metadata[MetadataKeyIv] = iv.AsSlice()

	block, err := aes.NewCipher(m.key.Key())
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// The Seal function in AES-GCM mode appends the authentication tag to the ciphertext.
	// We need to separate the actual ciphertext from the tag for storage and later verification.
	// Reference: https://pkg.go.dev/crypto/cipher#AEAD
	ciphertext := aesgcm.Seal(nil, iv.AsSlice(), plaintext, nil)
	content.Content = ciphertext[:len(ciphertext)-aesgcm.Overhead()]
	content.Metadata[MetadataKeyAesGcmTag] = ciphertext[len(ciphertext)-aesgcm.Overhead():]

	return &content, nil
}

func (m *MemAesGcmBackend) DecryptContent(_ctx context.Context, content *encryptionpb.EncryptedContent) (
	[]byte, error) {
	method, ok := content.Metadata[MetadataKeyMethod]
	if !ok {
		return nil, errors.Errorf("metadata %s not found", MetadataKeyMethod)
	}
	if string(method) != MetadataMethodAes256Gcm {
		return nil, errors.Errorf("encryption method mismatch, expected %s vs actual %s",
			MetadataMethodAes256Gcm, method)
	}

	ivValue, ok := content.Metadata[MetadataKeyIv]
	if !ok {
		return nil, errors.Errorf("metadata %s not found", MetadataKeyIv)
	}

	iv, err := NewIVFromSlice(ivValue)
	if err != nil {
		return nil, err
	}

	tag, ok := content.Metadata[MetadataKeyAesGcmTag]
	if !ok {
		return nil, errors.New("aes gcm tag not found")
	}

	block, err := aes.NewCipher(m.key.Key())
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	ciphertext := append(content.Content, tag...)
	plaintext, err := aesgcm.Open(nil, iv.AsSlice(), ciphertext, nil)
	if err != nil {
		return nil, errors.Annotate(err, wrongMasterKey+" :decrypt in GCM mode failed")
	}

	return plaintext, nil
}
