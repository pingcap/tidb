package encryption

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/stretchr/testify/require"
)

type mockKmsProvider struct {
	name           string
	decryptCounter int
}

func (m *mockKmsProvider) Name() string {
	return m.name
}

func (m *mockKmsProvider) DecryptDataKey(_ctx context.Context, _encryptedKey []byte) ([]byte, error) {
	m.decryptCounter++
	return []byte("decrypted_key"), nil
}

func TestKmsBackendDecrypt(t *testing.T) {
	ctx := context.Background()
	mockProvider := &mockKmsProvider{name: "mock_kms"}
	backend, err := NewKmsBackend(mockProvider)
	require.NoError(t, err)

	ciphertextKey := []byte("ciphertext_key")
	content := &encryptionpb.EncryptedContent{
		Metadata: map[string][]byte{
			MetadataKeyKmsVendor:        []byte("mock_kms"),
			MetadataKeyKmsCiphertextKey: ciphertextKey,
		},
		Content: []byte("encrypted_content"),
	}

	// First decryption
	_, err = backend.Decrypt(ctx, content)
	require.NoError(t, err)
	require.Equal(t, 1, mockProvider.decryptCounter, "KMS provider should be called once")

	// Second decryption with the same ciphertext key (should use cache)
	_, err = backend.Decrypt(ctx, content)
	require.NoError(t, err)
	require.Equal(t, 1, mockProvider.decryptCounter, "KMS provider should not be called again")

	// Third decryption with a different ciphertext key
	content.Metadata[MetadataKeyKmsCiphertextKey] = []byte("new_ciphertext_key")
	_, err = backend.Decrypt(ctx, content)
	require.NoError(t, err)
	require.Equal(t, 2, mockProvider.decryptCounter, "KMS provider should be called again for a new key")
}

func TestKmsBackendDecryptErrors(t *testing.T) {
	ctx := context.Background()
	mockProvider := &mockKmsProvider{name: "mock_kms"}
	backend, err := NewKmsBackend(mockProvider)
	require.NoError(t, err)

	testCases := []struct {
		name    string
		content *encryptionpb.EncryptedContent
		errMsg  string
	}{
		{
			name: "missing KMS vendor",
			content: &encryptionpb.EncryptedContent{
				Metadata: map[string][]byte{
					MetadataKeyKmsCiphertextKey: []byte("ciphertext_key"),
				},
			},
			errMsg: "wrong master key: missing KMS vendor",
		},
		{
			name: "KMS vendor mismatch",
			content: &encryptionpb.EncryptedContent{
				Metadata: map[string][]byte{
					MetadataKeyKmsVendor:        []byte("wrong_kms"),
					MetadataKeyKmsCiphertextKey: []byte("ciphertext_key"),
				},
			},
			errMsg: "KMS vendor mismatch expect mock_kms got wrong_kms",
		},
		{
			name: "missing ciphertext key",
			content: &encryptionpb.EncryptedContent{
				Metadata: map[string][]byte{
					MetadataKeyKmsVendor: []byte("mock_kms"),
				},
			},
			errMsg: "KMS ciphertext key not found",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := backend.Decrypt(ctx, tc.content)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}
