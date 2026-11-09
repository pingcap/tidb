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

package executor

import (
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

var slowLogEscapedNewlineReplacer = strings.NewReplacer(
	"\r\n", `\n`,
	"\r", " ",
	"\n", `\n`,
	"\t", " ",
)

func formatSlowLogSQL(sql string) string {
	if !variable.SlowLogEscapeNewline.Load() || !strings.Contains(sql, "\n") {
		return formatSQL(sql)
	}
	return formatSQLWithReplacer(sql, slowLogEscapedNewlineReplacer)
}
