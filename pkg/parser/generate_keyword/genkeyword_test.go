package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseLine(t *testing.T) {
	add := parseLine("	add               \"ADD\"")
	require.Equal(t, add, "ADD")

	tso := parseLine("	tidbCurrentTSO    \"TIDB_CURRENT_TSO\"")
	require.Equal(t, tso, "TIDB_CURRENT_TSO")
}
