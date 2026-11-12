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

package sessionstates

import (
	"strings"

	"github.com/pingcap/errors"
)

// SessionTokenMode controls issuance and acceptance independently during upgrade.
type SessionTokenMode string

const (
	// SessionTokenCompatible preserves migration to and from unmodified TiDB.
	SessionTokenCompatible SessionTokenMode = "COMPATIBLE"
	// SessionTokenTransition issues V2 while outstanding legacy tokens drain.
	SessionTokenTransition SessionTokenMode = "TRANSITION"
	// SessionTokenStrict rejects legacy authentication after operator finalization.
	SessionTokenStrict SessionTokenMode = "STRICT"
)

// ParseSessionTokenMode rejects unknown policies rather than enabling legacy auth.
func ParseSessionTokenMode(value string) (SessionTokenMode, error) {
	switch mode := SessionTokenMode(strings.ToUpper(value)); mode {
	case SessionTokenCompatible, SessionTokenTransition, SessionTokenStrict:
		return mode, nil
	default:
		return "", errors.New("invalid session token mode")
	}
}
