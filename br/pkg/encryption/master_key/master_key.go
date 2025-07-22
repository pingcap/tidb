// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/kms"
	"go.uber.org/zap"
)

const (
	StorageVendorNameAWS   = "aws"
	StorageVendorNameAzure = "azure"
	StorageVendorNameGCP   = "gcp"
)

// Backend is an interface that defines the methods required for an encryption backend.
type Backend interface {
	// Decrypt takes an EncryptedContent and returns the decrypted plaintext as a byte slice or an error.
	Decrypt(ctx context.Context, ciphertext *encryptionpb.EncryptedContent) ([]byte, error)
	Close()
}

func CreateBackend(config *encryptionpb.MasterKey) (Backend, error) {
	if config == nil {
		return nil, errors.Errorf("master key config is nil")
	}

	switch backend := config.Backend.(type) {
	case *encryptionpb.MasterKey_Plaintext:
		// should not plaintext type as guarded by caller
		return nil, errors.New("should not create plaintext master key")
	case *encryptionpb.MasterKey_File:
		fileBackend, err := createFileBackend(backend.File.Path)
		if err != nil {
			return nil, errors.Annotate(err, "master key config is nil")
		}
		return fileBackend, nil
	case *encryptionpb.MasterKey_Kms:
		return createCloudBackend(backend.Kms)
	default:
		return nil, errors.New("unknown master key backend type")
	}
}

func createCloudBackend(config *encryptionpb.MasterKeyKms) (Backend, error) {
	log.Info("creating cloud KMS backend",
		zap.String("region", config.GetRegion()),
		zap.String("endpoint", config.GetEndpoint()),
		zap.String("key_id", config.GetKeyId()),
		zap.String("Vendor", config.GetVendor()))

	switch config.Vendor {
	case StorageVendorNameAWS:
		kmsProvider, err := kms.NewAwsKms(config)
		if err != nil {
			return nil, errors.Annotate(err, "new AWS KMS")
		}
		return NewKmsBackend(kmsProvider)

	case StorageVendorNameAzure:
		return nil, errors.Errorf("not implemented Azure KMS")
	case StorageVendorNameGCP:
		kmsProvider, err := kms.NewGcpKms(config)
		if err != nil {
			return nil, errors.Annotate(err, "new GCP KMS")
		}
		return NewKmsBackend(kmsProvider)

	default:
		return nil, errors.Errorf("vendor not found: %s", config.Vendor)
	}
}
