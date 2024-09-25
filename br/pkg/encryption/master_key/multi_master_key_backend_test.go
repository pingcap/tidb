// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockBackend is a mock implementation of the Backend interface
type MockBackend struct {
	mock.Mock
}

func (m *MockBackend) Decrypt(ctx context.Context, encryptedContent *encryptionpb.EncryptedContent) ([]byte, error) {
	args := m.Called(ctx, encryptedContent)
	// The first return value should be []byte or nil
	if ret := args.Get(0); ret != nil {
		return ret.([]byte), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockBackend) Close() {
	// do nothing
}

func TestMultiMasterKeyBackendDecrypt(t *testing.T) {
	ctx := context.Background()
	encryptedContent := &encryptionpb.EncryptedContent{Content: []byte("encrypted")}

	t.Run("success first backend", func(t *testing.T) {
		mock1 := new(MockBackend)
		mock1.On("Decrypt", ctx, encryptedContent).Return([]byte("decrypted"), nil)

		mock2 := new(MockBackend)

		backend := &MultiMasterKeyBackend{
			backends: []Backend{mock1, mock2},
		}

		result, err := backend.Decrypt(ctx, encryptedContent)
		require.NoError(t, err)
		require.Equal(t, []byte("decrypted"), result)

		mock1.AssertExpectations(t)
		mock2.AssertNotCalled(t, "Decrypt")
	})

	t.Run("success second backend", func(t *testing.T) {
		mock1 := new(MockBackend)
		mock1.On("Decrypt", ctx, encryptedContent).Return(nil, errors.New("failed"))

		mock2 := new(MockBackend)
		mock2.On("Decrypt", ctx, encryptedContent).Return([]byte("decrypted"), nil)

		backend := &MultiMasterKeyBackend{
			backends: []Backend{mock1, mock2},
		}

		result, err := backend.Decrypt(ctx, encryptedContent)
		require.NoError(t, err)
		require.Equal(t, []byte("decrypted"), result)

		mock1.AssertExpectations(t)
		mock2.AssertExpectations(t)
	})

	t.Run("all backends fail", func(t *testing.T) {
		mock1 := new(MockBackend)
		mock1.On("Decrypt", ctx, encryptedContent).Return(nil, errors.New("failed1"))

		mock2 := new(MockBackend)
		mock2.On("Decrypt", ctx, encryptedContent).Return(nil, errors.New("failed2"))

		backend := &MultiMasterKeyBackend{
			backends: []Backend{mock1, mock2},
		}

		result, err := backend.Decrypt(ctx, encryptedContent)
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "failed1")
		require.Contains(t, err.Error(), "failed2")

		mock1.AssertExpectations(t)
		mock2.AssertExpectations(t)
	})

	t.Run("no backends", func(t *testing.T) {
		backend := &MultiMasterKeyBackend{
			backends: []Backend{},
		}

		result, err := backend.Decrypt(ctx, encryptedContent)
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "internal error")
	})
}
