// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore

import (
	"bytes"
	"context"
	"io"

	"github.com/ProtonMail/go-crypto/openpgp/packet"
	openpgp "github.com/ProtonMail/go-crypto/openpgp/v2"
	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/multierr"
)

const gpgEncryptedFileSuffix = ".gpg"

type gpgStorage struct {
	storeapi.Storage
	recipients openpgp.EntityList
	config     *packet.Config
}

// WithGPGEncryption wraps a storage so WriteFile and Create encrypt each file
// for all recipients in publicKey. Read operations return the encrypted bytes.
func WithGPGEncryption(inner storeapi.Storage, publicKey []byte) (storeapi.Storage, error) {
	if inner == nil {
		return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "GPG encryption requires a storage")
	}
	config := &packet.Config{DefaultCipher: packet.CipherAES256}
	recipients, err := readGPGRecipients(publicKey, config)
	if err != nil {
		return nil, err
	}
	return &gpgStorage{
		Storage:    inner,
		recipients: recipients,
		config:     config,
	}, nil
}

func readGPGRecipients(publicKey []byte, config *packet.Config) (openpgp.EntityList, error) {
	trimmed := bytes.TrimSpace(publicKey)
	var (
		recipients openpgp.EntityList
		err        error
	)
	if bytes.HasPrefix(trimmed, []byte("-----BEGIN PGP ")) {
		recipients, err = openpgp.ReadArmoredKeyRing(bytes.NewReader(trimmed))
	} else {
		recipients, err = openpgp.ReadKeyRing(bytes.NewReader(publicKey))
	}
	if err != nil {
		return nil, errors.Annotate(err, "parse GPG public key")
	}
	if len(recipients) == 0 {
		return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "GPG public key contains no recipients")
	}
	for _, recipient := range recipients {
		if recipient == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "GPG public key contains a nil recipient")
		}
		if hasGPGPrivateKey(recipient) {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "GPG key file must not contain private key material")
		}
		if _, err := recipient.EncryptionKeyWithError(config.Now(), config); err != nil {
			return nil, errors.Annotate(err, "GPG public key has no valid encryption key")
		}
	}
	return recipients, nil
}

func hasGPGPrivateKey(entity *openpgp.Entity) bool {
	if entity.PrivateKey != nil {
		return true
	}
	for _, subkey := range entity.Subkeys {
		if subkey.PrivateKey != nil {
			return true
		}
	}
	return false
}

// EncryptedFileSuffix returns the conventional suffix for OpenPGP messages.
func (*gpgStorage) EncryptedFileSuffix() string {
	return gpgEncryptedFileSuffix
}

func (s *gpgStorage) encrypt(ciphertext io.Writer) (io.WriteCloser, error) {
	plaintext, err := openpgp.Encrypt(ciphertext, s.recipients, nil, nil, nil, s.config)
	return plaintext, errors.Trace(err)
}

func (s *gpgStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	var ciphertext bytes.Buffer
	plaintext, err := s.encrypt(&ciphertext)
	if err != nil {
		return err
	}
	if _, err = plaintext.Write(data); err != nil {
		closeErr := plaintext.Close()
		return multierr.Combine(errors.Trace(err), errors.Annotate(closeErr, "close GPG writer after write failure"))
	}
	if err = plaintext.Close(); err != nil {
		return errors.Annotate(err, "close GPG writer")
	}
	return s.Storage.WriteFile(ctx, name, ciphertext.Bytes())
}

func (s *gpgStorage) Create(ctx context.Context, name string, option *storeapi.WriterOption) (objectio.Writer, error) {
	inner, err := s.Storage.Create(ctx, name, option)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ciphertext := &gpgCiphertextWriter{ctx: ctx, writer: inner}
	plaintext, err := s.encrypt(ciphertext)
	if err != nil {
		closeErr := inner.Close(ctx)
		deleteErr := s.Storage.DeleteFile(ctx, name)
		return nil, multierr.Combine(
			err,
			errors.Annotate(closeErr, "close incomplete GPG file"),
			errors.Annotate(deleteErr, "delete incomplete GPG file"),
		)
	}
	return &gpgWriter{
		plaintext:  plaintext,
		ciphertext: ciphertext,
		storage:    s.Storage,
		name:       name,
	}, nil
}

type gpgCiphertextWriter struct {
	ctx    context.Context
	writer objectio.Writer
}

func (w *gpgCiphertextWriter) Write(data []byte) (int, error) {
	return w.writer.Write(w.ctx, data)
}

type gpgWriter struct {
	plaintext  io.WriteCloser
	ciphertext *gpgCiphertextWriter
	storage    storeapi.Storage
	name       string
	writeErr   error
}

func (w *gpgWriter) Write(ctx context.Context, data []byte) (int, error) {
	w.ciphertext.ctx = ctx
	n, err := w.plaintext.Write(data)
	if err != nil && w.writeErr == nil {
		w.writeErr = errors.Trace(err)
	}
	return n, errors.Trace(err)
}

func (w *gpgWriter) Close(ctx context.Context) error {
	w.ciphertext.ctx = ctx
	encryptionErr := w.plaintext.Close()
	storageErr := w.ciphertext.writer.Close(ctx)
	err := multierr.Combine(
		w.writeErr,
		errors.Annotate(encryptionErr, "close GPG writer"),
		errors.Annotate(storageErr, "close encrypted file"),
	)
	if err == nil {
		return nil
	}
	deleteErr := w.storage.DeleteFile(ctx, w.name)
	return multierr.Combine(err, errors.Annotate(deleteErr, "delete incomplete GPG file"))
}
