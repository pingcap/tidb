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

package stmtctx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Gate: bindingDigestMemo is pure storage with first-writer-wins semantics —
// an empty digest never initializes it (absence stays absence),
// later producers cannot overwrite the first writer, and Reset (the
// per-statement StatementContext reuse path) clears it so no digest can
// leak across statements.
func TestBindingDigestMemo(t *testing.T) {
	sc := NewStmtCtx()

	_, ok := sc.BindingDigest()
	require.False(t, ok, "fresh statement context has no binding digest")

	sc.InitBindingDigest("norm-a", "digest-a")
	digest, ok := sc.BindingDigest()
	require.True(t, ok)
	require.Equal(t, "digest-a", digest)

	// First writer wins: later producers must not overwrite.
	sc.InitBindingDigest("norm-b", "digest-b")
	digest, ok = sc.BindingDigest()
	require.True(t, ok)
	require.Equal(t, "digest-a", digest)

	// An empty digest must never initialize the memo.
	sc2 := NewStmtCtx()
	sc2.InitBindingDigest("norm", "")
	_, ok = sc2.BindingDigest()
	require.False(t, ok, "empty digest must leave the memo absent")

	// Reset clears the memo together with the whole context, and a fresh
	// producer can initialize it again.
	require.True(t, sc.Reset())
	_, ok = sc.BindingDigest()
	require.False(t, ok, "Reset must clear the binding digest memo")
	sc.InitBindingDigest("norm-c", "digest-c")
	digest, ok = sc.BindingDigest()
	require.True(t, ok)
	require.Equal(t, "digest-c", digest)
}
