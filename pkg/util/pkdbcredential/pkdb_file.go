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
	"encoding/hex"
	"io"
	"os"

	"github.com/pingcap/errors"
	"golang.org/x/sys/unix"
)

const (
	encodedPasswordKeySize = 32
	maximumKeyFileSize     = encodedPasswordKeySize + 1
	passwordKeySize        = 16
)

var errCredentialFileNotRegular = errors.New("opened target is not a regular file")

func readPasswordKey(path string) ([]byte, error) {
	file, info, err := openRegularCredentialFile(path, ErrKeyFileAccess)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	permissions := info.Mode().Perm()
	if permissions != 0o400 && permissions != 0o600 {
		return nil, ErrInsecureKeyFilePermissions.GenWithStackByArgs(path, permissions)
	}

	encodedKey, err := readAtMost(file, maximumKeyFileSize+1)
	if err != nil {
		return nil, ErrKeyFileAccess.Wrap(err).GenWithStackByArgs(path)
	}
	if len(encodedKey) == maximumKeyFileSize && encodedKey[maximumKeyFileSize-1] == '\n' {
		encodedKey = encodedKey[:encodedPasswordKeySize]
	}
	if len(encodedKey) != encodedPasswordKeySize {
		return nil, ErrInvalidKeyFormat.GenWithStackByArgs(path)
	}

	key := make([]byte, passwordKeySize)
	if _, err := hex.Decode(key, encodedKey); err != nil {
		return nil, ErrInvalidKeyFormat.GenWithStackByArgs(path)
	}
	return key, nil
}

func readEncryptedPasswordFile(path string) ([]byte, error) {
	file, _, err := openRegularCredentialFile(path, ErrEncryptedFileAccess)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	envelope, err := readAtMost(file, maximumEnvelopeSize+1)
	if err != nil {
		return nil, ErrEncryptedFileAccess.Wrap(err).GenWithStackByArgs(path)
	}
	if len(envelope) > maximumEnvelopeSize {
		return nil, ErrInvalidEnvelope.GenWithStackByArgs(path)
	}
	return envelope, nil
}

func openRegularCredentialFile(path string, accessError *errors.Error) (*os.File, os.FileInfo, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, accessError.Wrap(err).GenWithStackByArgs(path)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, accessError.Wrap(err).GenWithStackByArgs(path)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, accessError.Wrap(errCredentialFileNotRegular).GenWithStackByArgs(path)
	}
	return file, info, nil
}

func readAtMost(reader io.Reader, limit int) ([]byte, error) {
	return io.ReadAll(io.LimitReader(reader, int64(limit)))
}
