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

import "github.com/pingcap/errors"

// Credential error identities are shared by credential producers and consumers.
var (
	// ErrPasswordSourceConflict indicates that plaintext and encrypted password sources are both configured.
	ErrPasswordSourceConflict = errors.Normalize(
		"plaintext password and encrypted password file settings cannot be used together; remove either the plaintext password setting or both password-encrypted-file and password-key-file",
		errors.RFCCodeText("Credential:Config:ErrPasswordSourceConflict"),
	)
	// ErrIncompleteEncryptedSource indicates that only one encrypted-source path is configured.
	ErrIncompleteEncryptedSource = errors.Normalize(
		"password-encrypted-file and password-key-file must be configured together",
		errors.RFCCodeText("Credential:Config:ErrIncompleteEncryptedSource"),
	)

	// ErrKeyFileAccess indicates that the Password Key File cannot be safely accessed.
	ErrKeyFileAccess = errors.Normalize(
		"cannot access password key file '%s'",
		errors.RFCCodeText("Credential:KeyFile:ErrKeyFileAccess"),
	)
	// ErrInsecureKeyFilePermissions indicates that the Password Key File does not use mode 0400 or 0600.
	ErrInsecureKeyFilePermissions = errors.Normalize(
		"insecure password key file permissions for '%s': expected mode 0400 or 0600, got %04o",
		errors.RFCCodeText("Credential:KeyFile:ErrInsecureKeyFilePermissions"),
	)
	// ErrInvalidKeyFormat indicates that the Password Key File does not contain one hex-encoded SM4 key.
	ErrInvalidKeyFormat = errors.Normalize(
		"invalid password key file '%s': expected 32 hexadecimal characters with an optional trailing LF",
		errors.RFCCodeText("Credential:KeyFile:ErrInvalidKeyFormat"),
	)

	// ErrEncryptedFileAccess indicates that the Encrypted Password File cannot be safely accessed.
	ErrEncryptedFileAccess = errors.Normalize(
		"cannot access encrypted password file '%s'",
		errors.RFCCodeText("Credential:Envelope:ErrEncryptedFileAccess"),
	)
	// ErrInvalidEnvelope indicates that an Encrypted Password File is structurally invalid.
	ErrInvalidEnvelope = errors.Normalize(
		"invalid encrypted password file '%s'",
		errors.RFCCodeText("Credential:Envelope:ErrInvalidEnvelope"),
	)
	// ErrUnsupportedVersion indicates that the envelope version is not supported.
	ErrUnsupportedVersion = errors.Normalize(
		"unsupported encrypted password file version %d in '%s'",
		errors.RFCCodeText("Credential:Envelope:ErrUnsupportedVersion"),
	)
	// ErrUnsupportedAlgorithm indicates that the envelope algorithm is not supported.
	ErrUnsupportedAlgorithm = errors.Normalize(
		"unsupported encrypted password file algorithm %d in '%s'",
		errors.RFCCodeText("Credential:Envelope:ErrUnsupportedAlgorithm"),
	)
	// ErrAuthenticationFailed indicates that envelope authentication failed.
	ErrAuthenticationFailed = errors.Normalize(
		"encrypted password authentication failed for '%s'",
		errors.RFCCodeText("Credential:Envelope:ErrAuthenticationFailed"),
	)

	// ErrPasswordInput indicates that plaintext password input could not be read safely.
	ErrPasswordInput = errors.Normalize(
		"invalid database password input",
		errors.RFCCodeText("Credential:Input:ErrPasswordInput"),
	)
	// ErrPasswordMismatch indicates that two interactive password entries differ.
	ErrPasswordMismatch = errors.Normalize(
		"database passwords do not match",
		errors.RFCCodeText("Credential:Input:ErrPasswordMismatch"),
	)

	// ErrOutputExists indicates that no-clobber creation found an existing output entry.
	ErrOutputExists = errors.Normalize(
		"encrypted password file '%s' already exists; no file was changed",
		errors.RFCCodeText("Credential:Output:ErrOutputExists"),
	)
	// ErrOutputWrite indicates that an Encrypted Password File could not be created completely.
	ErrOutputWrite = errors.Normalize(
		"cannot create encrypted password file '%s'",
		errors.RFCCodeText("Credential:Output:ErrOutputWrite"),
	)
)
