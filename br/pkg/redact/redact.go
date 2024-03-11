// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package redact

import (
	"encoding/hex"
	"strings"

	"github.com/pingcap/errors"
)

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
