// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pkdbcredential

import (
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
)

// ResolvePassword validates the configured password sources and returns the effective database password.
func ResolvePassword(plaintextPassword, encryptedPasswordFile, passwordKeyFile string) (string, error) {
	hasEncryptedFile := encryptedPasswordFile != ""
	hasKeyFile := passwordKeyFile != ""

	if plaintextPassword != "" && (hasEncryptedFile || hasKeyFile) {
		return "", ErrPasswordSourceConflict.GenWithStackByArgs()
	}
	if hasEncryptedFile != hasKeyFile {
		return "", ErrIncompleteEncryptedSource.GenWithStackByArgs()
	}
	if !hasEncryptedFile {
		return plaintextPassword, nil
	}
	return decryptPasswordFile(encryptedPasswordFile, passwordKeyFile)
}

func decryptPasswordFile(encryptedPasswordFile, passwordKeyFile string) (string, error) {
	key, err := readPasswordKey(passwordKeyFile)
	if err != nil {
		return "", err
	}
	envelope, err := readEncryptedPasswordFile(encryptedPasswordFile)
	if err != nil {
		return "", err
	}
	plaintext, err := openEnvelope(key, envelope, encryptedPasswordFile)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// CreateEncryptedPasswordFile reads one database password and creates a new Encrypted Password File.
// It never overwrites or removes an existing output entry and does not close plaintextPassword.
func CreateEncryptedPasswordFile(plaintextPassword io.Reader, passwordKeyFile, outputFile string) error {
	if err := checkOutputAbsent(outputFile); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outputFile), 0o755); err != nil {
		return ErrOutputWrite.Wrap(err).GenWithStackByArgs(outputFile)
	}

	key, err := readPasswordKey(passwordKeyFile)
	if err != nil {
		return err
	}
	plaintext, err := readPlaintextPassword(plaintextPassword)
	if err != nil {
		return err
	}

	nonce := make([]byte, envelopeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return ErrOutputWrite.GenWithStackByArgs(outputFile)
	}
	envelope, err := seal(key, nonce, plaintext)
	if err != nil {
		return ErrOutputWrite.GenWithStackByArgs(outputFile)
	}

	file, err := os.OpenFile(outputFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return ErrOutputExists.GenWithStackByArgs(outputFile)
		}
		return ErrOutputWrite.Wrap(err).GenWithStackByArgs(outputFile)
	}
	return writeAndCloseOutput(file, envelope, outputFile)
}

func checkOutputAbsent(path string) error {
	_, err := os.Lstat(path)
	if err == nil {
		return ErrOutputExists.GenWithStackByArgs(path)
	}
	if !os.IsNotExist(err) {
		return ErrOutputWrite.Wrap(err).GenWithStackByArgs(path)
	}
	return nil
}

func readPlaintextPassword(reader io.Reader) ([]byte, error) {
	plaintext, err := readAtMost(reader, maximumCiphertextSize+1)
	if len(plaintext) > maximumCiphertextSize {
		return nil, ErrPasswordInput.GenWithStackByArgs()
	}
	if err != nil {
		if ErrPasswordMismatch.Equal(err) {
			return nil, err
		}
		return nil, ErrPasswordInput.GenWithStackByArgs()
	}
	return plaintext, nil
}

func writeAndCloseOutput(file *os.File, envelope []byte, path string) error {
	written, writeErr := file.Write(envelope)
	if writeErr == nil && written != len(envelope) {
		writeErr = io.ErrShortWrite
	}
	closeErr := file.Close()

	if writeErr != nil {
		return ErrOutputWrite.Wrap(writeErr).GenWithStackByArgs(path)
	}
	if closeErr != nil {
		return ErrOutputWrite.Wrap(closeErr).GenWithStackByArgs(path)
	}
	return nil
}
