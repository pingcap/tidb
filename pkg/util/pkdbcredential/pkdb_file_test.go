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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const encodedFixedKey = "0123456789abcdeffedcba9876543210"

func TestPasswordKeyFormats(t *testing.T) {
	validKeys := []string{
		encodedFixedKey,
		encodedFixedKey + "\n",
		strings.ToUpper(encodedFixedKey),
	}
	for i, content := range validKeys {
		t.Run(fmt.Sprintf("valid-%d", i), func(t *testing.T) {
			path := writeCredentialTestFile(t, content, 0o600)
			key, err := readPasswordKey(path)
			require.NoError(t, err)
			require.Equal(t, fixedKey, key)
		})
	}

	invalidKeys := []string{
		encodedFixedKey[:31],
		encodedFixedKey + "x",
		encodedFixedKey + "\r\n",
		"\ufeff" + encodedFixedKey,
		"0x" + encodedFixedKey,
		" " + encodedFixedKey,
		"g" + encodedFixedKey[1:],
		strings.Repeat("a", maximumKeyFileSize+1),
	}
	for i, content := range invalidKeys {
		t.Run(fmt.Sprintf("invalid-%d", i), func(t *testing.T) {
			path := writeCredentialTestFile(t, content, 0o600)
			_, err := readPasswordKey(path)
			require.ErrorIs(t, err, ErrInvalidKeyFormat)
			require.NotContains(t, err.Error(), content)
		})
	}
}

func TestPasswordKeyPermissions(t *testing.T) {
	acceptedModes := []os.FileMode{0o400, 0o600}
	for _, mode := range acceptedModes {
		t.Run(fmt.Sprintf("accepted-%04o", mode), func(t *testing.T) {
			path := writeCredentialTestFile(t, encodedFixedKey, mode)
			_, err := readPasswordKey(path)
			require.NoError(t, err)
		})
	}

	rejectedModes := []os.FileMode{0o401, 0o500, 0o601, 0o602, 0o610, 0o611, 0o620, 0o640, 0o644, 0o700}
	for _, mode := range rejectedModes {
		t.Run(fmt.Sprintf("rejected-%04o", mode), func(t *testing.T) {
			path := writeCredentialTestFile(t, encodedFixedKey, mode)
			_, err := readPasswordKey(path)
			require.ErrorIs(t, err, ErrInsecureKeyFilePermissions)
			require.Contains(t, err.Error(), fmt.Sprintf("%04o", mode))
			require.NotContains(t, err.Error(), encodedFixedKey)
		})
	}
}

func TestPasswordKeyPermissionCheckPrecedesContentRead(t *testing.T) {
	path := writeCredentialTestFile(t, "not-a-key", 0o644)
	_, err := readPasswordKey(path)
	require.ErrorIs(t, err, ErrInsecureKeyFilePermissions)
	require.NotErrorIs(t, err, ErrInvalidKeyFormat)
}

func TestCredentialInputFileTypes(t *testing.T) {
	t.Run("regular-file symlink", func(t *testing.T) {
		target := writeCredentialTestFile(t, encodedFixedKey, 0o600)
		link := filepath.Join(t.TempDir(), "key-link")
		require.NoError(t, os.Symlink(target, link))

		key, err := readPasswordKey(link)
		require.NoError(t, err)
		require.Equal(t, fixedKey, key)
	})

	t.Run("directories", func(t *testing.T) {
		directory := t.TempDir()
		_, err := readPasswordKey(directory)
		require.ErrorIs(t, err, ErrKeyFileAccess)
		_, err = readEncryptedPasswordFile(directory)
		require.ErrorIs(t, err, ErrEncryptedFileAccess)
	})

	t.Run("FIFO", func(t *testing.T) {
		directory := t.TempDir()
		fifo := filepath.Join(directory, "credential-fifo")
		require.NoError(t, unix.Mkfifo(fifo, 0o600))

		err := runCredentialReadWithTimeout(t, func() error {
			_, err := readPasswordKey(fifo)
			return err
		})
		require.ErrorIs(t, err, ErrKeyFileAccess)

		err = runCredentialReadWithTimeout(t, func() error {
			_, err := readEncryptedPasswordFile(fifo)
			return err
		})
		require.ErrorIs(t, err, ErrEncryptedFileAccess)
	})

	t.Run("symlink to FIFO", func(t *testing.T) {
		directory := t.TempDir()
		fifo := filepath.Join(directory, "credential-fifo")
		link := filepath.Join(directory, "credential-link")
		require.NoError(t, unix.Mkfifo(fifo, 0o600))
		require.NoError(t, os.Symlink(fifo, link))

		err := runCredentialReadWithTimeout(t, func() error {
			_, err := readPasswordKey(link)
			return err
		})
		require.ErrorIs(t, err, ErrKeyFileAccess)

		err = runCredentialReadWithTimeout(t, func() error {
			_, err := readEncryptedPasswordFile(link)
			return err
		})
		require.ErrorIs(t, err, ErrEncryptedFileAccess)
	})

	t.Run("device", func(t *testing.T) {
		const device = "/dev/null"
		if _, err := os.Stat(device); err != nil {
			t.Skipf("device unavailable: %v", err)
		}

		err := runCredentialReadWithTimeout(t, func() error {
			_, err := readPasswordKey(device)
			return err
		})
		require.ErrorIs(t, err, ErrKeyFileAccess)

		err = runCredentialReadWithTimeout(t, func() error {
			_, err := readEncryptedPasswordFile(device)
			return err
		})
		require.ErrorIs(t, err, ErrEncryptedFileAccess)
	})

	t.Run("Unix socket", func(t *testing.T) {
		directory, err := os.MkdirTemp("", "pkdbcred-")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, os.RemoveAll(directory)) })
		socket := filepath.Join(directory, "credential.sock")
		listener, err := net.Listen("unix", socket)
		require.NoError(t, err)
		defer listener.Close()

		err = runCredentialReadWithTimeout(t, func() error {
			_, err := readPasswordKey(socket)
			return err
		})
		require.ErrorIs(t, err, ErrKeyFileAccess)

		err = runCredentialReadWithTimeout(t, func() error {
			_, err := readEncryptedPasswordFile(socket)
			return err
		})
		require.ErrorIs(t, err, ErrEncryptedFileAccess)
	})
}

func TestReadAtMostBoundsInfiniteInput(t *testing.T) {
	testCases := []struct {
		name  string
		limit int
	}{
		{name: "password key", limit: maximumKeyFileSize + 1},
		{name: "encrypted password", limit: maximumEnvelopeSize + 1},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reader := &infiniteReader{}
			content, err := readAtMost(reader, testCase.limit)
			require.NoError(t, err)
			require.Len(t, content, testCase.limit)
			require.Equal(t, testCase.limit, reader.bytesRead)
		})
	}
}

type infiniteReader struct {
	bytesRead int
}

func (reader *infiniteReader) Read(buffer []byte) (int, error) {
	for index := range buffer {
		buffer[index] = 'a'
	}
	reader.bytesRead += len(buffer)
	return len(buffer), nil
}

func TestCredentialInputSizeBounds(t *testing.T) {
	keyPath := writeCredentialTestFile(t, strings.Repeat("a", maximumKeyFileSize+1), 0o600)
	_, err := readPasswordKey(keyPath)
	require.ErrorIs(t, err, ErrInvalidKeyFormat)

	envelopePath := writeCredentialTestFile(t, strings.Repeat("a", maximumEnvelopeSize+1), 0o600)
	_, err = readEncryptedPasswordFile(envelopePath)
	require.ErrorIs(t, err, ErrInvalidEnvelope)
}

func TestCredentialInputMissingFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	_, err := readPasswordKey(path)
	require.ErrorIs(t, err, ErrKeyFileAccess)
	require.Contains(t, err.Error(), path)

	_, err = readEncryptedPasswordFile(path)
	require.ErrorIs(t, err, ErrEncryptedFileAccess)
	require.Contains(t, err.Error(), path)
}

func writeCredentialTestFile(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credential")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	require.NoError(t, os.Chmod(path, mode))
	return path
}

func runCredentialReadWithTimeout(t *testing.T, read func() error) error {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		result <- read()
	}()

	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("credential input file read blocked")
		return nil
	}
}
