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

package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/util/pkdbcredential"
	"github.com/stretchr/testify/require"
)

const testEncodedKey = "0123456789abcdeffedcba9876543210"

func TestInformationalCommandsDoNotReadPassword(t *testing.T) {
	testCases := []struct {
		name string
		args []string
	}{
		{name: "root help"},
		{name: "explicit root help", args: []string{"--help"}},
		{name: "encrypt help", args: []string{"encrypt", "--help"}},
		{name: "version", args: []string{"--version"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			input := &observedInput{err: errors.New("password input must not be read")}
			var stdout, stderr bytes.Buffer
			exitCode := run(testCase.args, input, &stdout, &stderr)
			require.Zero(t, exitCode)
			require.Zero(t, input.reads)
			require.NotEmpty(t, stdout.String())
			require.Empty(t, stderr.String())
		})
	}
}

func TestEncryptNonTerminalPasswordRoundTrip(t *testing.T) {
	testCases := []struct {
		name     string
		password []byte
	}{
		{name: "empty password"},
		{name: "trailing newline", password: []byte("plain password\n")},
		{name: "arbitrary bytes", password: []byte{0x00, 0xff, '\r', '\n', 0x80}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			password := testCase.password
			keyFile := writeCommandTestFile(t, "key", []byte(testEncodedKey+"\n"), 0o600)
			outputFile := filepath.Join(t.TempDir(), "nested", "password.enc")
			var stdout, stderr bytes.Buffer

			exitCode := run(
				[]string{"encrypt", "--key-file", keyFile, "--output", outputFile},
				bytes.NewReader(password),
				&stdout,
				&stderr,
			)
			require.Zero(t, exitCode)
			require.Empty(t, stderr.String())
			absoluteOutput, err := filepath.Abs(outputFile)
			require.NoError(t, err)
			require.Equal(t, "encrypted password file created: "+absoluteOutput+"\n", stdout.String())

			decrypted, err := pkdbcredential.ResolvePassword("", outputFile, keyFile)
			require.NoError(t, err)
			require.Equal(t, string(password), decrypted)
			info, err := os.Stat(outputFile)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

			envelope, err := os.ReadFile(outputFile)
			require.NoError(t, err)
			if len(password) != 0 {
				require.NotContains(t, stdout.String(), string(password))
			}
			require.NotContains(t, stdout.String(), string(envelope))
			digest := sha256.Sum256(password)
			require.NotContains(t, stdout.String(), string(digest[:]))
		})
	}
}

func TestArgumentErrorsDoNotReadOrEchoPassword(t *testing.T) {
	const passwordCanary = "argv-password-canary"
	baseDirectory := t.TempDir()
	keyFile := writeCommandTestFile(t, "key", []byte(testEncodedKey), 0o600)

	testCases := []struct {
		name          string
		args          []string
		expectedError string
		outputFile    string
	}{
		{name: "unknown operation", args: []string{passwordCanary}, expectedError: invalidArgumentsMessage},
		{name: "unknown password flag", args: []string{"encrypt", "--password=" + passwordCanary}, expectedError: invalidArgumentsMessage},
		{name: "positional password", args: []string{"encrypt", "--key-file", keyFile, "--output", filepath.Join(baseDirectory, "positional.enc"), passwordCanary}, expectedError: errPositionalArguments.Error()},
		{name: "missing key", args: []string{"encrypt", "--output", filepath.Join(baseDirectory, "missing-key.enc")}, expectedError: errKeyFileRequired.Error()},
		{name: "empty key", args: []string{"encrypt", "--key-file=", "--output", filepath.Join(baseDirectory, "empty-key.enc")}, expectedError: errKeyFileRequired.Error()},
		{name: "missing output", args: []string{"encrypt", "--key-file", keyFile}, expectedError: errOutputRequired.Error()},
		{name: "empty output", args: []string{"encrypt", "--key-file", keyFile, "--output="}, expectedError: errOutputRequired.Error()},
		{
			name:          "output ending in slash",
			args:          []string{"encrypt", "--key-file", keyFile, "--output", filepath.Join(baseDirectory, "new-parent") + "/"},
			expectedError: errOutputPathMustNameFile.Error(),
			outputFile:    filepath.Join(baseDirectory, "new-parent"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			input := &observedInput{err: errors.New(passwordCanary)}
			var stdout, stderr bytes.Buffer
			exitCode := run(testCase.args, input, &stdout, &stderr)
			require.Equal(t, 1, exitCode)
			require.Zero(t, input.reads)
			require.Empty(t, stdout.String())
			require.Contains(t, stderr.String(), testCase.expectedError)
			require.NotContains(t, stderr.String(), passwordCanary)
			if testCase.outputFile != "" {
				_, err := os.Lstat(testCase.outputFile)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

func TestOutputAndKeyValidationPrecedePasswordInput(t *testing.T) {
	t.Run("existing output", func(t *testing.T) {
		keyFile := writeCommandTestFile(t, "key", []byte(testEncodedKey), 0o600)
		outputFile := writeCommandTestFile(t, "existing.enc", []byte("existing"), 0o600)
		input := &observedInput{err: errors.New("password input must not be read")}
		var stdout, stderr bytes.Buffer

		exitCode := run([]string{"encrypt", "--key-file", keyFile, "--output", outputFile}, input, &stdout, &stderr)
		require.Equal(t, 1, exitCode)
		require.Zero(t, input.reads)
		require.Contains(t, stderr.String(), "already exists")
		content, err := os.ReadFile(outputFile)
		require.NoError(t, err)
		require.Equal(t, []byte("existing"), content)
	})

	t.Run("root output uses existing-entry rule", func(t *testing.T) {
		input := &observedInput{err: errors.New("password input must not be read")}
		var stdout, stderr bytes.Buffer

		exitCode := run([]string{"encrypt", "--key-file", "missing-key", "--output", "/"}, input, &stdout, &stderr)
		require.Equal(t, 1, exitCode)
		require.Zero(t, input.reads)
		require.Contains(t, stderr.String(), "already exists")
	})

	t.Run("invalid key after parent creation", func(t *testing.T) {
		keyFile := writeCommandTestFile(t, "invalid-key", []byte("invalid-key"), 0o600)
		parent := filepath.Join(t.TempDir(), "new", "parent")
		outputFile := filepath.Join(parent, "password.enc")
		input := &observedInput{err: errors.New("password input must not be read")}
		var stdout, stderr bytes.Buffer

		exitCode := run([]string{"encrypt", "--key-file", keyFile, "--output", outputFile}, input, &stdout, &stderr)
		require.Equal(t, 1, exitCode)
		require.Zero(t, input.reads)
		require.Contains(t, stderr.String(), "invalid password key file")
		info, err := os.Stat(parent)
		require.NoError(t, err)
		require.True(t, info.IsDir())
		_, err = os.Stat(outputFile)
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}

func TestConfirmingPasswordReader(t *testing.T) {
	t.Run("matching entries", func(t *testing.T) {
		entries := [][]byte{[]byte("password\x00"), []byte("password\x00")}
		var prompts bytes.Buffer
		readCalls := 0
		reader := &confirmingPasswordReader{
			fileDescriptor: 42,
			promptOutput:   &prompts,
			readPassword: func(fileDescriptor int) ([]byte, error) {
				require.Equal(t, 42, fileDescriptor)
				entry := entries[readCalls]
				readCalls++
				return entry, nil
			},
		}

		password, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, entries[0], password)
		require.Equal(t, 2, readCalls)
		require.Equal(t, passwordPrompt+"\n"+passwordConfirmationPrompt+"\n", prompts.String())
		_, err = io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, 2, readCalls)
	})

	t.Run("matching empty entries", func(t *testing.T) {
		readCalls := 0
		reader := &confirmingPasswordReader{
			fileDescriptor: 1,
			promptOutput:   io.Discard,
			readPassword: func(int) ([]byte, error) {
				readCalls++
				return nil, nil
			},
		}

		password, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Empty(t, password)
		require.Equal(t, 2, readCalls)
	})

	t.Run("mismatch", func(t *testing.T) {
		entries := [][]byte{[]byte("first-secret"), []byte("second-secret")}
		readCalls := 0
		reader := &confirmingPasswordReader{
			fileDescriptor: 1,
			promptOutput:   io.Discard,
			readPassword: func(int) ([]byte, error) {
				entry := entries[readCalls]
				readCalls++
				return entry, nil
			},
		}

		_, err := io.ReadAll(reader)
		require.ErrorIs(t, err, pkdbcredential.ErrPasswordMismatch)
		require.NotContains(t, err.Error(), string(entries[0]))
		require.NotContains(t, err.Error(), string(entries[1]))
		_, secondErr := reader.Read(make([]byte, 1))
		require.ErrorIs(t, secondErr, pkdbcredential.ErrPasswordMismatch)
		require.Equal(t, 2, readCalls)
	})
}

func TestConfirmingPasswordReaderInputFailuresAreSafe(t *testing.T) {
	const secretCause = "secret-terminal-error"

	testCases := []struct {
		name              string
		promptOutput      io.Writer
		terminalReadError error
		expectedReads     int
	}{
		{name: "first prompt failure", promptOutput: &failOnWrite{failAt: 1, err: errors.New(secretCause)}, expectedReads: 0},
		{name: "confirmation prompt failure", promptOutput: &failOnWrite{failAt: 3, err: errors.New(secretCause)}, expectedReads: 1},
		{name: "terminal read failure", promptOutput: io.Discard, terminalReadError: errors.New(secretCause), expectedReads: 1},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			readCalls := 0
			reader := &confirmingPasswordReader{
				fileDescriptor: 1,
				promptOutput:   testCase.promptOutput,
				readPassword: func(int) ([]byte, error) {
					readCalls++
					if testCase.terminalReadError != nil {
						return nil, testCase.terminalReadError
					}
					return []byte("password"), nil
				},
			}

			_, err := io.ReadAll(reader)
			require.ErrorIs(t, err, pkdbcredential.ErrPasswordInput)
			require.NotContains(t, err.Error(), secretCause)
			require.Equal(t, testCase.expectedReads, readCalls)
		})
	}
}

func TestMatchingPasswordCreatesOutput(t *testing.T) {
	keyFile := writeCommandTestFile(t, "key", []byte(testEncodedKey), 0o600)
	outputFile := filepath.Join(t.TempDir(), "password.enc")
	readCalls := 0
	reader := &confirmingPasswordReader{
		fileDescriptor: 1,
		promptOutput:   io.Discard,
		readPassword: func(int) ([]byte, error) {
			readCalls++
			return []byte("password"), nil
		},
	}

	require.NoError(t, pkdbcredential.CreateEncryptedPasswordFile(reader, keyFile, outputFile))
	require.Equal(t, 2, readCalls)
	password, err := pkdbcredential.ResolvePassword("", outputFile, keyFile)
	require.NoError(t, err)
	require.Equal(t, "password", password)
}

func TestPasswordMismatchDoesNotCreateOutput(t *testing.T) {
	keyFile := writeCommandTestFile(t, "key", []byte(testEncodedKey), 0o600)
	outputFile := filepath.Join(t.TempDir(), "password.enc")
	entries := [][]byte{[]byte("first"), []byte("second")}
	readCalls := 0
	reader := &confirmingPasswordReader{
		fileDescriptor: 1,
		promptOutput:   io.Discard,
		readPassword: func(int) ([]byte, error) {
			entry := entries[readCalls]
			readCalls++
			return entry, nil
		},
	}

	err := pkdbcredential.CreateEncryptedPasswordFile(reader, keyFile, outputFile)
	require.ErrorIs(t, err, pkdbcredential.ErrPasswordMismatch)
	require.Equal(t, 2, readCalls)
	_, statErr := os.Stat(outputFile)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestOversizedPasswordFailsWithoutOutput(t *testing.T) {
	keyFile := writeCommandTestFile(t, "key", []byte(testEncodedKey), 0o600)
	outputFile := filepath.Join(t.TempDir(), "password.enc")
	password := bytes.Repeat([]byte{'p'}, 65_493)
	var stdout, stderr bytes.Buffer

	exitCode := run(
		[]string{"encrypt", "--key-file", keyFile, "--output", outputFile},
		bytes.NewReader(password),
		&stdout,
		&stderr,
	)
	require.Equal(t, 1, exitCode)
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), pkdbcredential.ErrPasswordInput.MessageTemplate())
	require.NotContains(t, stderr.String(), strings.Repeat("p", 32))
	_, err := os.Stat(outputFile)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestSuccessOutputFailureDoesNotChangeResult(t *testing.T) {
	keyFile := writeCommandTestFile(t, "key", []byte(testEncodedKey), 0o600)
	outputFile := filepath.Join(t.TempDir(), "password.enc")
	var stderr bytes.Buffer

	exitCode := run(
		[]string{"encrypt", "--key-file", keyFile, "--output", outputFile},
		strings.NewReader("password"),
		&failOnWrite{failAt: 1, err: errors.New("stdout failure")},
		&stderr,
	)
	require.Zero(t, exitCode)
	require.Empty(t, stderr.String())
	password, err := pkdbcredential.ResolvePassword("", outputFile, keyFile)
	require.NoError(t, err)
	require.Equal(t, "password", password)
}

func writeCommandTestFile(t *testing.T, name string, content []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, content, mode))
	require.NoError(t, os.Chmod(path, mode))
	return path
}

type observedInput struct {
	reads int
	err   error
}

func (input *observedInput) Read([]byte) (int, error) {
	input.reads++
	return 0, input.err
}

type failOnWrite struct {
	writes int
	failAt int
	err    error
}

func (writer *failOnWrite) Write(content []byte) (int, error) {
	writer.writes++
	if writer.writes == writer.failAt {
		return 0, writer.err
	}
	return len(content), nil
}
