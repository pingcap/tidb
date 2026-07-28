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
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestRedactGlobalSysVarValueForLog(t *testing.T) {
	for _, name := range []string{
		vardef.TiDBExpEmbedJinaAIAPIKey,
		vardef.TiDBExpEmbedOpenAIAPIKey,
		vardef.TiDBExpEmbedCohereAPIKey,
		vardef.TiDBExpEmbedHuggingFaceAPIKey,
		vardef.TiDBExpEmbedNvidiaNIMAPIKey,
		vardef.TiDBExpEmbedGeminiAPIKey,
	} {
		require.Equal(t, "******", redactGlobalSysVarValueForLog(name, "secret-api-key"))
		require.Empty(t, redactGlobalSysVarValueForLog(name, ""))
		require.Equal(t, "******", redactGlobalSysVarValueForAudit(name, "secret-api-key"))
		require.Empty(t, redactGlobalSysVarValueForAudit(name, ""))
	}
	require.Equal(t, "ordinary-value", redactGlobalSysVarValueForLog("ordinary-variable", "ordinary-value"))
	require.Equal(t, "ordinary-value", redactGlobalSysVarValueForAudit("ordinary-variable", "ordinary-value"))
	require.Equal(t, "s3://bucket?secret-access-key=secret", redactGlobalSysVarValueForAudit(
		vardef.TiDBCloudStorageURI,
		"s3://bucket?secret-access-key=secret",
	))
}
