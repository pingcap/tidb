package task

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertBoolForTiKV(t *testing.T) {
	require.Equal(t, "true", convertBoolForTiKV(true))
	require.Equal(t, "false", convertBoolForTiKV(false))
	require.Equal(t, 42, convertBoolForTiKV(42))
	require.Equal(t, "hello", convertBoolForTiKV("hello"))
}

func TestConvertToPlainKeys(t *testing.T) {
	converted := convertToPlainKeys(map[string]any{
		"import": map[string]any{"num-threads": 10},
		"log":    map[string]any{"file": map[string]any{"max-num": 2, "max-age": "1d"}},
		"other":  false,
	})
	require.Equal(t, map[string]any{
		"import.num-threads": 10,
		"log.file.max-num":   2,
		"log.file.max-age":   "1d",
		"other":              false,
	}, converted)
}
