// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"go.uber.org/multierr"
)

const (
	defaultBackendCapacity = 5
)

// MultiMasterKeyBackend can contain multiple master shard backends.
// If any one of those backends successfully decrypts the data, the data will be returned.
// The main purpose of this backend is to provide a high availability for master key in the future.
// Right now only one master key backend is used to encrypt/decrypt data.
type MultiMasterKeyBackend struct {
	backends []Backend
}

func NewMultiMasterKeyBackend(masterKeysProto []*encryptionpb.MasterKey) (*MultiMasterKeyBackend, error) {
	if masterKeysProto == nil && len(masterKeysProto) == 0 {
		return nil, errors.New("must provide at least one master key")
	}
	var backends = make([]Backend, 0, defaultBackendCapacity)
	for _, masterKeyProto := range masterKeysProto {
		backend, err := CreateBackend(masterKeyProto)
		if err != nil {
			return nil, errors.Trace(err)
		}
		backends = append(backends, backend)
	}
	return &MultiMasterKeyBackend{
		backends: backends,
	}, nil
}

func (m *MultiMasterKeyBackend) Decrypt(ctx context.Context, encryptedContent *encryptionpb.EncryptedContent) (
	[]byte, error) {
	if len(m.backends) == 0 {
		return nil, errors.New("internal error: should always contain at least one backend")
	}

	var err error
	for _, masterKeyBackend := range m.backends {
		res, decryptErr := masterKeyBackend.Decrypt(ctx, encryptedContent)
		if decryptErr == nil {
			return res, nil
		}
		err = multierr.Append(err, decryptErr)
	}

	return nil, errors.Wrap(err, "failed to decrypt in multi master key backend")
}

func (m *MultiMasterKeyBackend) Close() {
	for _, backend := range m.backends {
		backend.Close()
	}
}
