// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"context"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	encryption "github.com/pingcap/tidb/br/pkg/encryption/master_key"
	"github.com/pingcap/tidb/br/pkg/utils"
)

type Manager struct {
	cipherInfo        *backuppb.CipherInfo
	masterKeyBackends *encryption.MultiMasterKeyBackend
	encryptionMethod  *encryptionpb.EncryptionMethod
}

func NewManager(cipherInfo *backuppb.CipherInfo, masterKeyConfigs *backuppb.MasterKeyConfig) (*Manager, error) {
	// should never happen since config has default
	if cipherInfo == nil || masterKeyConfigs == nil {
		return nil, errors.New("cipherInfo or masterKeyConfigs is nil")
	}

	if utils.IsEffectiveEncryptionMethod(cipherInfo.CipherType) {
		return &Manager{
			cipherInfo:        cipherInfo,
			masterKeyBackends: nil,
			encryptionMethod:  nil,
		}, nil
	}
	if utils.IsEffectiveEncryptionMethod(masterKeyConfigs.EncryptionType) {
		masterKeyBackends, err := encryption.NewMultiMasterKeyBackend(masterKeyConfigs.GetMasterKeys())
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &Manager{
			cipherInfo:        nil,
			masterKeyBackends: masterKeyBackends,
			encryptionMethod:  &masterKeyConfigs.EncryptionType,
		}, nil
	}
	return nil, nil
}

func (m *Manager) Decrypt(ctx context.Context, content []byte, fileEncryptionInfo *encryptionpb.FileEncryptionInfo) (
	[]byte, error) {
	switch mode := fileEncryptionInfo.Mode.(type) {
	case *encryptionpb.FileEncryptionInfo_PlainTextDataKey:
		if m.cipherInfo == nil {
			return nil, errors.New("plaintext data key info is required but not set")
		}
		decryptedContent, err := utils.Decrypt(content, m.cipherInfo, fileEncryptionInfo.FileIv)
		if err != nil {
			return nil, errors.Annotate(err, "failed to decrypt content using plaintext data key")
		}
		return decryptedContent, nil
	case *encryptionpb.FileEncryptionInfo_MasterKeyBased:
		encryptedContents := fileEncryptionInfo.GetMasterKeyBased().DataKeyEncryptedContent
		if len(encryptedContents) == 0 {
			return nil, errors.New("should contain at least one encrypted data key")
		}
		// pick first one, the list is for future expansion of multiple encrypted data keys by different master key backend
		encryptedContent := encryptedContents[0]
		decryptedDataKey, err := m.masterKeyBackends.Decrypt(ctx, encryptedContent)
		if err != nil {
			return nil, errors.Annotate(err, "failed to decrypt data key using master key")
		}

		cipherInfo := backuppb.CipherInfo{
			CipherType: fileEncryptionInfo.EncryptionMethod,
			CipherKey:  decryptedDataKey,
		}
		decryptedContent, err := utils.Decrypt(content, &cipherInfo, fileEncryptionInfo.FileIv)
		if err != nil {
			return nil, errors.Annotate(err, "failed to decrypt content using decrypted data key")
		}
		return decryptedContent, nil
	default:
		return nil, errors.Errorf("internal error: unsupported encryption mode type %T", mode)
	}
}

func (m *Manager) Close() {
	if m == nil {
		return
	}
	if m.masterKeyBackends != nil {
		m.masterKeyBackends.Close()
	}
}
