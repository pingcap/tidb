package encryption

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		assert.NoError(t, err)
		assert.Equal(t, []byte("decrypted"), result)

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
		assert.NoError(t, err)
		assert.Equal(t, []byte("decrypted"), result)

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
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed1")
		assert.Contains(t, err.Error(), "failed2")

		mock1.AssertExpectations(t)
		mock2.AssertExpectations(t)
	})

	t.Run("no backends", func(t *testing.T) {
		backend := &MultiMasterKeyBackend{
			backends: []Backend{},
		}

		result, err := backend.Decrypt(ctx, encryptedContent)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "internal error")
	})
}
