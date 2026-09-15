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
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	perrors "github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestResolvePasswordSourceValidation(t *testing.T) {
	testCases := []struct {
		name              string
		plaintextPassword string
		encryptedFile     string
		keyFile           string
		expectedPassword  string
		expectedError     error
	}{
		{name: "no source"},
		{name: "plaintext configured", plaintextPassword: "password", expectedPassword: "password"},
		{
			name: "encrypted file only", encryptedFile: "encrypted",
			expectedError: ErrIncompleteEncryptedSource,
		},
		{name: "key file only", keyFile: "key", expectedError: ErrIncompleteEncryptedSource},
		{
			name: "plaintext and encrypted file", plaintextPassword: "password", encryptedFile: "encrypted",
			expectedError: ErrPasswordSourceConflict,
		},
		{
			name: "plaintext and key file", plaintextPassword: "password", keyFile: "key",
			expectedError: ErrPasswordSourceConflict,
		},
		{
			name: "plaintext and complete encrypted source", plaintextPassword: "password",
			encryptedFile: "encrypted", keyFile: "key", expectedError: ErrPasswordSourceConflict,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			password, err := ResolvePassword(testCase.plaintextPassword, testCase.encryptedFile, testCase.keyFile)
			if testCase.expectedError != nil {
				require.ErrorIs(t, err, testCase.expectedError)
				require.Empty(t, password)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.expectedPassword, password)
		})
	}
}

func TestCreateAndResolveEncryptedPasswordFile(t *testing.T) {
	passwords := [][]byte{
		{},
		[]byte("plain password"),
		[]byte("密码-安全"),
		{0x00, 0xff, '\n', '\r', 0x80},
		bytes.Repeat([]byte{'x'}, maximumCiphertextSize),
	}

	for _, password := range passwords {
		keyFile := writeCredentialTestFile(t, encodedFixedKey+"\n", 0o600)
		outputFile := filepath.Join(t.TempDir(), "nested", "password.enc")
		require.NoError(t, CreateEncryptedPasswordFile(bytes.NewReader(password), keyFile, outputFile))

		info, err := os.Stat(outputFile)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

		resolved, err := ResolvePassword("", outputFile, keyFile)
		require.NoError(t, err)
		require.Equal(t, string(password), resolved)
	}
}

func TestResolveFixedEnvelopeFile(t *testing.T) {
	keyFile := writeCredentialTestFile(t, encodedFixedKey, 0o600)
	envelopeFile := writeCredentialTestFile(t, string(fixedEnvelope), 0o600)

	password, err := ResolvePassword("", envelopeFile, keyFile)
	require.NoError(t, err)
	require.Equal(t, string(fixedPlaintext), password)
}

func TestEncryptedPasswordFileUsesFreshNonce(t *testing.T) {
	keyFile := writeCredentialTestFile(t, encodedFixedKey, 0o600)
	directory := t.TempDir()
	firstOutput := filepath.Join(directory, "first.enc")
	secondOutput := filepath.Join(directory, "second.enc")

	require.NoError(t, CreateEncryptedPasswordFile(strings.NewReader("same password"), keyFile, firstOutput))
	require.NoError(t, CreateEncryptedPasswordFile(strings.NewReader("same password"), keyFile, secondOutput))
	firstEnvelope, err := os.ReadFile(firstOutput)
	require.NoError(t, err)
	secondEnvelope, err := os.ReadFile(secondOutput)
	require.NoError(t, err)
	require.NotEqual(t, firstEnvelope[envelopeHeaderSize:authenticatedPrefixSize], secondEnvelope[envelopeHeaderSize:authenticatedPrefixSize])
	require.NotEqual(t, firstEnvelope, secondEnvelope)
}

func TestEncryptedPasswordFileDoesNotContainPlaintext(t *testing.T) {
	const password = "unique-canary-password-that-must-not-be-persisted"
	keyFile := writeCredentialTestFile(t, encodedFixedKey, 0o600)
	outputFile := filepath.Join(t.TempDir(), "password.enc")

	require.NoError(t, CreateEncryptedPasswordFile(strings.NewReader(password), keyFile, outputFile))
	envelope, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	require.NotContains(t, string(envelope), password)
}

func TestCreateEncryptedPasswordFileDoesNotOverwrite(t *testing.T) {
	t.Run("regular file", func(t *testing.T) {
		outputFile := filepath.Join(t.TempDir(), "password.enc")
		require.NoError(t, os.WriteFile(outputFile, []byte("existing"), 0o600))
		reader := &observedReader{err: errors.New("password reader should not be called")}

		err := CreateEncryptedPasswordFile(reader, filepath.Join(t.TempDir(), "missing-key"), outputFile)
		require.ErrorIs(t, err, ErrOutputExists)
		require.Zero(t, reader.reads)
		content, readErr := os.ReadFile(outputFile)
		require.NoError(t, readErr)
		require.Equal(t, []byte("existing"), content)
	})

	t.Run("dangling symlink", func(t *testing.T) {
		directory := t.TempDir()
		target := filepath.Join(directory, "missing-target")
		outputFile := filepath.Join(directory, "password.enc")
		require.NoError(t, os.Symlink(target, outputFile))

		err := CreateEncryptedPasswordFile(strings.NewReader("password"), "missing-key", outputFile)
		require.ErrorIs(t, err, ErrOutputExists)
		_, targetErr := os.Stat(target)
		require.ErrorIs(t, targetErr, os.ErrNotExist)
	})

	t.Run("directory", func(t *testing.T) {
		outputFile := t.TempDir()
		err := CreateEncryptedPasswordFile(strings.NewReader("password"), "missing-key", outputFile)
		require.ErrorIs(t, err, ErrOutputExists)
	})
}

func TestCreateEncryptedPasswordFileConcurrentNoClobber(t *testing.T) {
	keyFile := writeCredentialTestFile(t, encodedFixedKey, 0o600)
	outputFile := filepath.Join(t.TempDir(), "password.enc")
	entered := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	results := make(chan error, 2)

	for range 2 {
		reader := &barrierReader{
			content: []byte("password"),
			entered: entered,
			release: release,
		}
		go func() {
			results <- CreateEncryptedPasswordFile(reader, keyFile, outputFile)
		}()
	}

	for range 2 {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent creator did not reach password input")
		}
	}
	release <- struct{}{}
	release <- struct{}{}

	var succeeded, alreadyExists int
	for range 2 {
		select {
		case err := <-results:
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrOutputExists):
				alreadyExists++
			default:
				require.NoError(t, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent creator did not finish")
		}
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, alreadyExists)

	password, err := ResolvePassword("", outputFile, keyFile)
	require.NoError(t, err)
	require.Equal(t, "password", password)
}

func TestCreateEncryptedPasswordFileValidationOrder(t *testing.T) {
	t.Run("parent directories precede key validation", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "new", "parent")
		outputFile := filepath.Join(parent, "password.enc")
		err := CreateEncryptedPasswordFile(strings.NewReader("password"), filepath.Join(t.TempDir(), "missing-key"), outputFile)
		require.ErrorIs(t, err, ErrKeyFileAccess)
		info, statErr := os.Stat(parent)
		require.NoError(t, statErr)
		require.True(t, info.IsDir())
	})

	t.Run("key validation precedes password input", func(t *testing.T) {
		keyFile := writeCredentialTestFile(t, "invalid-key", 0o600)
		outputFile := filepath.Join(t.TempDir(), "password.enc")
		reader := &observedReader{err: errors.New("password reader should not be called")}

		err := CreateEncryptedPasswordFile(reader, keyFile, outputFile)
		require.ErrorIs(t, err, ErrInvalidKeyFormat)
		require.Zero(t, reader.reads)
	})
}

func TestCreateEncryptedPasswordFileInputErrorsAreSafe(t *testing.T) {
	keyFile := writeCredentialTestFile(t, encodedFixedKey, 0o600)

	t.Run("reader failure", func(t *testing.T) {
		const secretCause = "secret-reader-error"
		outputFile := filepath.Join(t.TempDir(), "password.enc")
		err := CreateEncryptedPasswordFile(&observedReader{err: errors.New(secretCause)}, keyFile, outputFile)
		require.ErrorIs(t, err, ErrPasswordInput)
		require.NotContains(t, err.Error(), secretCause)
		_, statErr := os.Stat(outputFile)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("password mismatch", func(t *testing.T) {
		outputFile := filepath.Join(t.TempDir(), "password.enc")
		err := CreateEncryptedPasswordFile(
			&observedReader{err: ErrPasswordMismatch.GenWithStackByArgs()},
			keyFile,
			outputFile,
		)
		require.ErrorIs(t, err, ErrPasswordMismatch)
		_, statErr := os.Stat(outputFile)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("oversized password", func(t *testing.T) {
		outputFile := filepath.Join(t.TempDir(), "password.enc")
		password := bytes.Repeat([]byte{'x'}, maximumCiphertextSize+1)
		err := CreateEncryptedPasswordFile(bytes.NewReader(password), keyFile, outputFile)
		require.ErrorIs(t, err, ErrPasswordInput)
		require.NotContains(t, err.Error(), strings.Repeat("x", 32))
		_, statErr := os.Stat(outputFile)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})
}

func TestCreateEncryptedPasswordFileDoesNotCloseReader(t *testing.T) {
	keyFile := writeCredentialTestFile(t, encodedFixedKey, 0o600)
	outputFile := filepath.Join(t.TempDir(), "password.enc")
	reader := &observedReader{reader: strings.NewReader("password")}

	require.NoError(t, CreateEncryptedPasswordFile(reader, keyFile, outputFile))
	require.False(t, reader.closed)
}

func TestResolvePasswordAuthenticationFailureIsSafe(t *testing.T) {
	keyFile := writeCredentialTestFile(t, encodedFixedKey, 0o600)
	outputFile := filepath.Join(t.TempDir(), "password.enc")
	require.NoError(t, CreateEncryptedPasswordFile(strings.NewReader("password"), keyFile, outputFile))

	const wrongEncodedKey = "1123456789abcdeffedcba9876543210"
	wrongKeyFile := writeCredentialTestFile(t, wrongEncodedKey, 0o600)
	_, err := ResolvePassword("", outputFile, wrongKeyFile)
	require.ErrorIs(t, err, ErrAuthenticationFailed)
	require.NotContains(t, err.Error(), wrongEncodedKey)
	require.NotContains(t, err.Error(), "message authentication failed")
}

func TestCredentialErrorContracts(t *testing.T) {
	testCases := []struct {
		identity *perrors.Error
		code     string
		message  string
	}{
		{ErrPasswordSourceConflict, "Credential:Config:ErrPasswordSourceConflict", "plaintext password and encrypted password file settings cannot be used together; remove either the plaintext password setting or both password-encrypted-file and password-key-file"},
		{ErrIncompleteEncryptedSource, "Credential:Config:ErrIncompleteEncryptedSource", "password-encrypted-file and password-key-file must be configured together"},
		{ErrKeyFileAccess, "Credential:KeyFile:ErrKeyFileAccess", "cannot access password key file '%s'"},
		{ErrInsecureKeyFilePermissions, "Credential:KeyFile:ErrInsecureKeyFilePermissions", "insecure password key file permissions for '%s': expected mode 0400 or 0600, got %04o"},
		{ErrInvalidKeyFormat, "Credential:KeyFile:ErrInvalidKeyFormat", "invalid password key file '%s': expected 32 hexadecimal characters with an optional trailing LF"},
		{ErrEncryptedFileAccess, "Credential:Envelope:ErrEncryptedFileAccess", "cannot access encrypted password file '%s'"},
		{ErrInvalidEnvelope, "Credential:Envelope:ErrInvalidEnvelope", "invalid encrypted password file '%s'"},
		{ErrUnsupportedVersion, "Credential:Envelope:ErrUnsupportedVersion", "unsupported encrypted password file version %d in '%s'"},
		{ErrUnsupportedAlgorithm, "Credential:Envelope:ErrUnsupportedAlgorithm", "unsupported encrypted password file algorithm %d in '%s'"},
		{ErrAuthenticationFailed, "Credential:Envelope:ErrAuthenticationFailed", "encrypted password authentication failed for '%s'"},
		{ErrPasswordInput, "Credential:Input:ErrPasswordInput", "invalid database password input"},
		{ErrPasswordMismatch, "Credential:Input:ErrPasswordMismatch", "database passwords do not match"},
		{ErrOutputExists, "Credential:Output:ErrOutputExists", "encrypted password file '%s' already exists; no file was changed"},
		{ErrOutputWrite, "Credential:Output:ErrOutputWrite", "cannot create encrypted password file '%s'"},
	}

	for _, testCase := range testCases {
		require.Equal(t, testCase.code, string(testCase.identity.RFCCode()))
		require.Equal(t, testCase.message, testCase.identity.MessageTemplate())
	}
}

type barrierReader struct {
	content []byte
	entered chan<- struct{}
	release <-chan struct{}
	read    bool
}

func (reader *barrierReader) Read(buffer []byte) (int, error) {
	if reader.read {
		return 0, io.EOF
	}
	reader.entered <- struct{}{}
	<-reader.release
	reader.read = true
	return copy(buffer, reader.content), nil
}

type observedReader struct {
	reader io.Reader
	err    error
	reads  int
	closed bool
}

func (reader *observedReader) Read(buffer []byte) (int, error) {
	reader.reads++
	if reader.err != nil {
		return 0, reader.err
	}
	return reader.reader.Read(buffer)
}

func (reader *observedReader) Close() error {
	reader.closed = true
	return nil
}
