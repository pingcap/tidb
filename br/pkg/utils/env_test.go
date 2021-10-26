// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"os"

	"github.com/pingcap/check"
)

type envSuit struct{}

var _ = check.Suite(&envSuit{})

func (s *envSuit) TestProxyFields(c *check.C) {
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
			c.Assert(os.Unsetenv(env), check.IsNil)
		}

		for i := 0; i < 3; i++ {
			if (1<<i)&mask != 0 {
				c.Assert(os.Setenv(envs[i], envPreset[i]), check.IsNil)
			}
		}

		for _, field := range proxyFields() {
			idx, ok := revIndex[field.Key]
			c.Assert(ok, check.IsTrue)
			c.Assert((1<<idx)&mask, check.Not(check.Equals), 0)
			c.Assert(field.String, check.Equals, envPreset[idx])
		}
	}
}
