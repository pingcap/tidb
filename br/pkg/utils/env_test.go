// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProxyFields(t *testing.T) {
	revIndex := map[string]int{
		"http_proxy":  0,
		"https_proxy": 1,
		"no_proxy":    2,
	}
	envs := [...]string{"http_proxy", "https_proxy", "no_proxy"}
	envPreset := [...]string{"http://127.0.0.1:8080", "https://127.0.0.1:8443", "localhost,127.0.0.1"}

	// Exhaust all combinations of those environment variables' selection.
	// Each bit of the mask decided whether this index of `envs` would be set.
	for mask := 0; mask <= 0b111; mask++ {
		for _, env := range envs {
			require.NoError(t, os.Unsetenv(env))
		}

		for i := 0; i < 3; i++ {
			if (1<<i)&mask != 0 {
				require.NoError(t, os.Setenv(envs[i], envPreset[i]))
			}
		}

		for _, field := range proxyFields() {
			idx, ok := revIndex[field.Key]
			require.True(t, ok)
			require.NotZero(t, (1<<idx)&mask)
			require.Equal(t, envPreset[idx], field.String)
		}
	}
}
