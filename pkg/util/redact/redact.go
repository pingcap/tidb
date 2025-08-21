// Copyright 2024 PingCAP, Inc.
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

package redact

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var (
	_ fmt.Stringer = redactStringer{}
)

// String will redact the input string according to 'mode'. Check 'tidb_redact_log': https://github.com/pingcap/tidb/blob/acf9e3128693a5a13f31027f05f4de41edf8d7b2/pkg/sessionctx/variable/sysvar.go#L2154.
func String(mode string, input string) string {
	switch mode {
	case "MARKER":
		b := &strings.Builder{}
		b.Grow(len(input))
		_, _ = b.WriteRune('‹')
		for _, c := range input {
			if c == '‹' || c == '›' {
				_, _ = b.WriteRune(c)
				_, _ = b.WriteRune(c)
			} else {
				_, _ = b.WriteRune(c)
			}
		}
		_, _ = b.WriteRune('›')
		return b.String()
	case "OFF":
		return input
	case "ON":
		return ""
	default:
		// should never happen
		intest.Assert(false, "invalid redact mode")
		return ""
	}
}

type redactStringer struct {
	mode     string
	stringer fmt.Stringer
}

func (s redactStringer) String() string {
	return String(s.mode, s.stringer.String())
}

// Stringer will redact the input stringer according to 'mode', similar to String().
func Stringer(mode string, input fmt.Stringer) redactStringer {
	return redactStringer{mode, input}
}

// DeRedactFile will deredact the input file, either removing marked contents, or remove the marker. It works line by line.
func DeRedactFile(remove bool, input string, output string) error {
	ifile, err := os.Open(filepath.Clean(input))
	if err != nil {
		return errors.WithStack(err)
	}
	defer ifile.Close()

	var ofile io.Writer
	if output == "-" {
		ofile = os.Stdout
	} else {
		//nolint: gosec
		file, err := os.OpenFile(filepath.Clean(output), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return errors.WithStack(err)
		}
		defer file.Close()
		ofile = file
	}

	return DeRedact(remove, ifile, ofile, "\n")
}

// DeRedact is similar to DeRedactFile, but act on reader/writer, it works line by line.
func DeRedact(remove bool, input io.Reader, output io.Writer, sep string) error {
	sc := bufio.NewScanner(input)
	out := bufio.NewWriter(output)
	defer out.Flush()
	buf := bytes.NewBuffer(nil)
	s := bufio.NewReader(nil)

	for sc.Scan() {
		s.Reset(strings.NewReader(sc.Text()))
		start := false
		for {
			ch, _, err := s.ReadRune()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.WithStack(err)
			}
			if ch == '‹' {
				if start {
					// must be '<'
					pch, _, err := s.ReadRune()
					if err != nil {
						return errors.WithStack(err)
					}
					if pch == ch {
						_, _ = buf.WriteRune(ch)
					} else {
						_, _ = buf.WriteRune(ch)
						_, _ = buf.WriteRune(pch)
					}
				} else {
					start = true
					buf.Reset()
				}
			} else if ch == '›' {
				if start {
					// peek the next
					pch, _, err := s.ReadRune()
					if err != nil && err != io.EOF {
						return errors.WithStack(err)
					}
					if pch == ch {
						_, _ = buf.WriteRune(ch)
					} else {
						start = false
						if err != io.EOF {
							// unpeek it
							if err := s.UnreadRune(); err != nil {
								return errors.WithStack(err)
							}
						}
						if remove {
							_ = out.WriteByte('?')
						} else {
							_, err = io.Copy(out, buf)
							if err != nil {
								return errors.WithStack(err)
							}
						}
					}
				} else {
					_, _ = out.WriteRune(ch)
				}
			} else if start {
				_, _ = buf.WriteRune(ch)
			} else {
				_, _ = out.WriteRune(ch)
			}
		}
		if start {
			_, _ = out.WriteRune('‹')
			_, _ = out.WriteString(buf.String())
		}
		_, _ = out.WriteString(sep)
	}

	return nil
}

// InitRedact inits the enableRedactLog
func InitRedact(redactLog bool) {
	mode := errors.RedactLogDisable
	if redactLog {
		mode = errors.RedactLogEnable
	}
	errors.RedactLogEnabled.Store(mode)
}

// NeedRedact returns whether to redact log
func NeedRedact() bool {
	mode := errors.RedactLogEnabled.Load()
	return mode != errors.RedactLogDisable && mode != ""
}

// Value receives string argument and return omitted information if redact log enabled
func Value(arg string) string {
	if NeedRedact() {
		return "?"
	}
	return arg
}

// Key receives a key return omitted information if redact log enabled
func Key(key []byte) string {
	if NeedRedact() {
		return "?"
	}
	return strings.ToUpper(hex.EncodeToString(key))
}

// WriteRedact is to write string with redact into `strings.Builder`
func WriteRedact(build *strings.Builder, v string, redact string) {
	if redact == errors.RedactLogMarker {
		build.WriteString("‹")
		build.WriteString(v)
		build.WriteString("›")
		return
	} else if redact == errors.RedactLogEnable {
		build.WriteString("?")
		return
	}
	build.WriteString(v)
}

// TaskInfoRedacted is a wrapper of backup.StreamBackupTaskInfo to redact sensitive information
type TaskInfoRedacted struct {
	Info *backup.StreamBackupTaskInfo
}

// String returns the redacted string of the task info
func (t TaskInfoRedacted) String() string {
	if t.Info == nil {
		return "nil"
	}

	infoCopy := *t.Info

	if t.Info.Storage != nil {
		// Create a copy of StorageBackend to modify
		storageCopy := *t.Info.Storage

		// Handle different backend types
		switch backend := storageCopy.Backend.(type) {
		case *backup.StorageBackend_S3:
			if backend.S3 != nil {
				// Copy S3 config and redact sensitive fields
				s3Copy := *backend.S3
				s3Copy.AccessKey = "[REDACTED]"
				s3Copy.SecretAccessKey = "[REDACTED]"
				s3Copy.SseKmsKeyId = "[REDACTED]"
				storageCopy.Backend = &backup.StorageBackend_S3{S3: &s3Copy}
			}

		case *backup.StorageBackend_Gcs:
			if backend.Gcs != nil {
				gcsCopy := *backend.Gcs
				gcsCopy.CredentialsBlob = "[REDACTED]"
				storageCopy.Backend = &backup.StorageBackend_Gcs{Gcs: &gcsCopy}
			}

		case *backup.StorageBackend_AzureBlobStorage:
			if backend.AzureBlobStorage != nil {
				azCopy := *backend.AzureBlobStorage
				azCopy.SharedKey = "[REDACTED]"
				azCopy.AccessSig = "[REDACTED]"
				azCopy.EncryptionKey = &backup.AzureCustomerKey{EncryptionKey: "[REDACTED]"}
				storageCopy.Backend = &backup.StorageBackend_AzureBlobStorage{
					AzureBlobStorage: &azCopy,
				}
			}
		default:
		}

		infoCopy.Storage = &storageCopy
	}

	return proto.CompactTextString(&infoCopy)
}
