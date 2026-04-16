package encrypt

import (
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	original := []byte("this is secret")

	encrypted := AESEncryptWithGCM(original)
	if string(encrypted) == string(original) {
		t.Errorf("Encryption failed: ciphertext is same as original")
	}

	decrypted, err := AESDecryptWithGCM(encrypted)
	if err != nil {
		t.Fatalf("Decryption failed with error: %v", err)
	}

	if string(decrypted) != string(original) {
		t.Errorf("Decryption failed: got %s, want %s", decrypted, original)
	}
}
