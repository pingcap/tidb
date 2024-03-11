// Copyright 2024 PingCAP, Inc.
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

package redact

import (
	"fmt"
	"strings"
)

// Redact will redact the input string according to 'mode'. Check 'tidb_redact_log': https://github.com/pingcap/tidb/blob/acf9e3128693a5a13f31027f05f4de41edf8d7b2/pkg/sessionctx/variable/sysvar.go#L2154.
func Redact(mode string, input string) string {
	switch mode {
	case "MARKER":
		b := &strings.Builder{}
		b.Grow(len(input))
		_, _ = b.WriteRune('‹')
		for _, c := range input {
			if c == '‹' || c == '›' {
				_, _ = b.WriteRune(c)
				_, _ = b.WriteRune(c)
			} else {
				_, _ = b.WriteRune(c)
			}
		}
		_, _ = b.WriteRune('›')
		return b.String()
	case "OFF":
		return input
	default:
		return ""
	}
}

type redactStringer struct {
	mode     string
	stringer fmt.Stringer
}

func (s *redactStringer) String() string {
	return Redact(s.mode, s.stringer.String())
}

func RedactStringer(mode string, input fmt.Stringer) fmt.Stringer {
	return &redactStringer{mode, input}
}
