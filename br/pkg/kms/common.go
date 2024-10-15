// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package kms

import (
	"bytes"

	"github.com/pingcap/errors"
)

// EncryptedKey is used to mark data as an encrypted key
type EncryptedKey []byte

func NewEncryptedKey(key []byte) (EncryptedKey, error) {
	if len(key) == 0 {
		return nil, errors.New("encrypted key cannot be empty")
	}
	return key, nil
}

// Equal method for EncryptedKey
func (e EncryptedKey) Equal(other *EncryptedKey) bool {
	return bytes.Equal(e, *other)
}

// CryptographyType represents different cryptography methods
type CryptographyType int

const (
	CryptographyTypePlain CryptographyType = iota
	CryptographyTypeAesGcm256
)

func (c CryptographyType) TargetKeySize() int {
	switch c {
	case CryptographyTypePlain:
		return 0 // Plain text has no limitation
	case CryptographyTypeAesGcm256:
		return 32
	default:
		return 0
	}
}

// PlainKey is used to mark a byte slice as a plaintext key
type PlainKey struct {
	tag CryptographyType
	key []byte
}

func NewPlainKey(key []byte, t CryptographyType) (*PlainKey, error) {
	limitation := t.TargetKeySize()
	if limitation > 0 && len(key) != limitation {
		return nil, errors.Errorf("encryption method and key length mismatch, expect %d got %d", limitation, len(key))
	}
	return &PlainKey{key: key, tag: t}, nil
}

func (p *PlainKey) KeyTag() CryptographyType {
	return p.tag
}

func (p *PlainKey) Key() []byte {
	return p.key
}
