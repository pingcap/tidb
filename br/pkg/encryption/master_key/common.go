// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// must keep it same with the constants in TiKV implementation
const (
	MetadataKeyMethod           string = "method"
	MetadataKeyIv               string = "iv"
	MetadataKeyAesGcmTag        string = "aes_gcm_tag"
	MetadataKeyKmsVendor        string = "kms_vendor"
	MetadataKeyKmsCiphertextKey string = "kms_ciphertext_key"
	MetadataMethodAes256Gcm     string = "aes256-gcm"
)

type IV [12]byte

func NewIV() IV {
	var iv IV
	binary.BigEndian.PutUint64(iv[:8], uint64(time.Now().UnixNano()))
	// Fill the remaining 4 bytes with random data
	if _, err := rand.Read(iv[8:]); err != nil {
		panic(err) // Handle this error appropriately in production code
	}
	return iv
}
