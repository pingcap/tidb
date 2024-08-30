// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package redact

import (
	"encoding/hex"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	backup "github.com/pingcap/kvproto/pkg/brpb"
)

var (
	reAccessKey       = regexp.MustCompile(`access_key:\"[^\"]*\"`)
	reSecretAccessKey = regexp.MustCompile(`secret_access_key:\"[^\"]*\"`)
	reSharedKey       = regexp.MustCompile(`shared_key:\"[^\"]*\"`)
	reCredentialsBlob = regexp.MustCompile(`credentials_blob:\"[^\"]*\"`)
	reAccessSig       = regexp.MustCompile(`access_sig:\"[^\"]*\"`)
	reEncryptKey      = regexp.MustCompile(`encryption_key:<.*?>`)
)

// InitRedact inits the enableRedactLog
func InitRedact(redactLog bool) {
	errors.RedactLogEnabled.Store(redactLog)
}

// NeedRedact returns whether to redact log
func NeedRedact() bool {
	return errors.RedactLogEnabled.Load()
}

// String receives string argument and return omitted information if redact log enabled
func String(arg string) string {
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

// TaskInfoRedacted is a wrapper of backup.StreamBackupTaskInfo to redact sensitive information
type TaskInfoRedacted struct {
	Info *backup.StreamBackupTaskInfo
}

func (TaskInfoRedacted) redact(input string) string {
	// Replace the matched fields with redacted versions
	output := reAccessKey.ReplaceAllString(input, `access_key:"[REDACTED]"`)
	output = reSecretAccessKey.ReplaceAllString(output, `secret_access_key:"[REDACTED]"`)
	output = reSharedKey.ReplaceAllString(output, `shared_key:"[REDACTED]"`)
	output = reCredentialsBlob.ReplaceAllString(output, `CredentialsBlob:"[REDACTED]"`)
	output = reAccessSig.ReplaceAllString(output, `access_sig:"[REDACTED]"`)
	output = reEncryptKey.ReplaceAllString(output, `encryption_key:<[REDACTED]>`)

	return output
}

// String returns the redacted string of the task info
func (t TaskInfoRedacted) String() string {
	return t.redact(t.Info.String())
}
