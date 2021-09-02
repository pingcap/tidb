// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package redact

import (
	"encoding/hex"
	"strings"

	"github.com/pingcap/errors"
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
