// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package redact_test

import (
	"encoding/hex"
	"testing"

	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/stretchr/testify/require"
)

func TestRedact(t *testing.T) {
	redacted, secret := "?", "secret"

	redact.InitRedact(false)
	require.Equal(t, redact.Value(secret), secret)
	require.Equal(t, redact.Key([]byte(secret)), hex.EncodeToString([]byte(secret)))

	redact.InitRedact(true)
	require.Equal(t, redact.Value(secret), redacted)
	require.Equal(t, redact.Key([]byte(secret)), redacted)
}
