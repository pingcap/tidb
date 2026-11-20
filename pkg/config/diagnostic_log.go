// Copyright 2026 PingCAP, Inc.
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

package config

import "fmt"

// DiagnosticLog selects the diagnostic files initialized at server startup.
type DiagnosticLog struct {
	Types []string `toml:"types" json:"types"`
}

func (c *DiagnosticLog) validate() error {
	seen := make(map[string]struct{}, len(c.Types))
	unique := make([]string, 0, len(c.Types))
	for _, kind := range c.Types {
		if kind != "deadlock" && kind != "index_usage" {
			return fmt.Errorf("unsupported diagnostic-log.types value %q", kind)
		}
		if _, ok := seen[kind]; !ok {
			seen[kind] = struct{}{}
			unique = append(unique, kind)
		}
	}
	c.Types = unique
	return nil
}
