// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/kms"
	"github.com/pingcap/tidb/br/pkg/utils"
)

type CachedKeys struct {
	encryptionBackend   *MemAesGcmBackend
	cachedCiphertextKey *kms.EncryptedKey
}

type KmsBackend struct {
	state struct {
		sync.Mutex
		cached *CachedKeys
	}
	kmsProvider kms.Provider
}

func NewKmsBackend(kmsProvider kms.Provider) (*KmsBackend, error) {
	return &KmsBackend{
		kmsProvider: kmsProvider,
	}, nil
}

func (k *KmsBackend) Decrypt(ctx context.Context, content *encryptionpb.EncryptedContent) ([]byte, error) {
	vendorName := k.kmsProvider.Name()
	if val, ok := content.Metadata[MetadataKeyKmsVendor]; !ok {
		return nil, errors.New("wrong master key: missing KMS vendor")
	} else if string(val) != vendorName {
		return nil, errors.Errorf("KMS vendor mismatch expect %s got %s", vendorName, string(val))
	}

	ciphertextKeyBytes, ok := content.Metadata[MetadataKeyKmsCiphertextKey]
	if !ok {
		return nil, errors.New("KMS ciphertext key not found")
	}
	ciphertextKey, err := kms.NewEncryptedKey(ciphertextKeyBytes)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create encrypted key")
	}

	k.state.Lock()
	defer k.state.Unlock()

	if k.state.cached != nil && k.state.cached.cachedCiphertextKey.Equal(&ciphertextKey) {
		return k.state.cached.encryptionBackend.DecryptContent(ctx, content)
	}

	decryptedKey, err :=
		utils.WithRetryV2(ctx, utils.NewBackoffRetryAllErrorStrategy(10, 500*time.Millisecond, 5*time.Second),
			func(ctx context.Context) ([]byte, error) {
				return k.kmsProvider.DecryptDataKey(ctx, ciphertextKey)
			})
	if err != nil {
		return nil, errors.Annotate(err, "decrypt encrypted key failed")
	}

	plaintextKey, err := kms.NewPlainKey(decryptedKey, kms.CryptographyTypeAesGcm256)
	if err != nil {
		return nil, errors.Annotate(err, "decrypt encrypted key failed")
	}

	backend, err := NewMemAesGcmBackend(plaintextKey.Key())
	if err != nil {
		return nil, errors.Annotate(err, "failed to create MemAesGcmBackend")
	}

	k.state.cached = &CachedKeys{
		encryptionBackend:   backend,
		cachedCiphertextKey: &ciphertextKey,
	}

	return k.state.cached.encryptionBackend.DecryptContent(ctx, content)
}

func (k *KmsBackend) Close() {
	k.kmsProvider.Close()
}
