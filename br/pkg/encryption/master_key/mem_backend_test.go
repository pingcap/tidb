package encryption

import (
	"bytes"
	"context"
	"testing"
)

func TestNewMemAesGcmBackend(t *testing.T) {
	key := make([]byte, 32) // 256-bit key
	_, err := NewMemAesGcmBackend(key)
	if err != nil {
		t.Fatalf("Failed to create MemAesGcmBackend: %v", err)
	}

	shortKey := make([]byte, 16)
	_, err = NewMemAesGcmBackend(shortKey)
	if err == nil {
		t.Fatal("Expected error for short key, got nil")
	}
}

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	backend, err := NewMemAesGcmBackend(key)
	if err != nil {
		t.Fatalf("Failed to create MemAesGcmBackend: %v", err)
	}

	plaintext := []byte("Hello, World!")
	iv := NewIV()

	ctx := context.Background()
	encrypted, err := backend.EncryptContent(ctx, plaintext, iv)
	if err != nil {
		t.Fatalf("Encryption failed: %v", err)
	}

	decrypted, err := backend.DecryptContent(ctx, encrypted)
	if err != nil {
		t.Fatalf("Decryption failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Fatalf("Decrypted text doesn't match original. Got %v, want %v", decrypted, plaintext)
	}
}

func TestDecryptWithWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	for i := range key2 {
		key2[i] = 1 // Different from key1
	}

	backend1, _ := NewMemAesGcmBackend(key1)
	backend2, _ := NewMemAesGcmBackend(key2)

	plaintext := []byte("Hello, World!")
	iv := NewIV()

	ctx := context.Background()
	encrypted, _ := backend1.EncryptContent(ctx, plaintext, iv)
	_, err := backend2.DecryptContent(ctx, encrypted)
	if err == nil {
		t.Fatal("Expected decryption with wrong key to fail, but it succeeded")
	}
}

func TestDecryptWithTamperedCiphertext(t *testing.T) {
	key := make([]byte, 32)
	backend, _ := NewMemAesGcmBackend(key)

	plaintext := []byte("Hello, World!")
	iv := NewIV()

	ctx := context.Background()
	encrypted, _ := backend.EncryptContent(ctx, plaintext, iv)
	encrypted.Content[0] ^= 1 // Tamper with the ciphertext

	_, err := backend.DecryptContent(ctx, encrypted)
	if err == nil {
		t.Fatal("Expected decryption of tampered ciphertext to fail, but it succeeded")
	}
}

func TestDecryptWithMissingMetadata(t *testing.T) {
	key := make([]byte, 32)
	backend, _ := NewMemAesGcmBackend(key)

	plaintext := []byte("Hello, World!")
	iv := NewIV()

	ctx := context.Background()
	encrypted, _ := backend.EncryptContent(ctx, plaintext, iv)
	delete(encrypted.Metadata, MetadataKeyMethod)

	_, err := backend.DecryptContent(ctx, encrypted)
	if err == nil {
		t.Fatal("Expected decryption with missing metadata to fail, but it succeeded")
	}
}

func TestEncryptDecryptLargeData(t *testing.T) {
	key := make([]byte, 32)
	backend, _ := NewMemAesGcmBackend(key)

	plaintext := make([]byte, 1000000) // 1 MB of data
	iv := NewIV()

	ctx := context.Background()
	encrypted, err := backend.EncryptContent(ctx, plaintext, iv)
	if err != nil {
		t.Fatalf("Encryption of large data failed: %v", err)
	}

	decrypted, err := backend.DecryptContent(ctx, encrypted)
	if err != nil {
		t.Fatalf("Decryption of large data failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Fatal("Decrypted large data doesn't match original")
	}
}
