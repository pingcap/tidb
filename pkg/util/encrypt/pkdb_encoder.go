package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
)

var encryptionKey = []byte("columnencryptionencryptioncolumn") // 32

// AESEncryptWithGCM encrypts the input data using AES-256-GCM encryption.
func AESEncryptWithGCM(raw []byte) []byte {
	base64Encoded := base64.StdEncoding.EncodeToString(raw)
	ciphertext, err := encryptAES256GCM([]byte(base64Encoded), encryptionKey)
	if err != nil {
		return raw
	}
	finalEncoded := base64.StdEncoding.EncodeToString(ciphertext)
	return []byte(finalEncoded)
}

// AES-256-GCM
func encryptAES256GCM(plaintext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := []byte("abcdefghijkl") //12
	if len(nonce) != aesgcm.NonceSize() {
		return nil, err
	}
	ciphertext := aesgcm.Seal(nil, nonce, plaintext, nil)
	return ciphertext, nil
}

// AESDecryptWithGCM decrypts the input data using AES-256-GCM decryption.
func AESDecryptWithGCM(ciphertext []byte) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(string(ciphertext))
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := []byte("abcdefghijkl") // 12
	base64Data, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	plaintext, err := base64.StdEncoding.DecodeString(string(base64Data))
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
