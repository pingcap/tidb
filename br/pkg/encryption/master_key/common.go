// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"crypto/rand"

	"github.com/pingcap/errors"
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

const (
	GcmIv12 = 12
	CtrIv16 = 16
)

type IvType int

const (
	IvTypeGcm IvType = iota
	IvTypeCtr
)

type IV struct {
	Type IvType
	Data []byte
}

func NewIVGcm() (IV, error) {
	iv := make([]byte, GcmIv12)
	_, err := rand.Read(iv)
	if err != nil {
		return IV{}, err
	}
	return IV{Type: IvTypeGcm, Data: iv}, nil
}

func NewIVFromSlice(src []byte) (IV, error) {
	switch len(src) {
	case CtrIv16:
		return IV{Type: IvTypeCtr, Data: append([]byte(nil), src...)}, nil
	case GcmIv12:
		return IV{Type: IvTypeGcm, Data: append([]byte(nil), src...)}, nil
	default:
		return IV{}, errors.Errorf("invalid IV length, must be 12 or 16 bytes, got %d", len(src))
	}
}

func (iv IV) AsSlice() []byte {
	return iv.Data
}
