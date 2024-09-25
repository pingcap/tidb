// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const (
	defaultBackendCapacity = 5
)

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

func (m *MultiMasterKeyBackend) Decrypt(ctx context.Context, encryptedContent *encryptionpb.EncryptedContent) ([]byte, error) {
	if len(m.backends) == 0 {
		return nil, errors.New("internal error: should always contain at least one backend")
	}

	var errMsgs = make([]string, 0, defaultBackendCapacity)
	for _, masterKeyBackend := range m.backends {
		res, err := masterKeyBackend.Decrypt(ctx, encryptedContent)
		if err == nil {
			return res, nil
		}
		errMsgs = append(errMsgs, errors.ErrorStack(err))
	}

	return nil, errors.Errorf("failed to decrypt in multi master key backend: %s", strings.Join(errMsgs, ","))
}

func (m *MultiMasterKeyBackend) Close() {
	for _, backend := range m.backends {
		backend.Close()
	}
}
