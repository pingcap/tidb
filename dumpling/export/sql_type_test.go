// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEscapeSQL(t *testing.T) {
	var bf bytes.Buffer
	str := []byte(`MWQeWw""'\rNmtGxzGp`)

	escapeSQL(str, &bf, true)
	require.Equal(t, `MWQeWw\"\"\'\\rNmtGxzGp`, bf.String())

	bf.Reset()
	escapeSQL(str, &bf, false)
	require.Equal(t, `MWQeWw""''\rNmtGxzGp`, bf.String())
}
