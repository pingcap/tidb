// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package texttree_test

import (
	"testing"

	"github.com/pingcap/tidb/util/texttree"
	"github.com/stretchr/testify/require"
)

func TestPrettyIdentifier(t *testing.T) {
	t.Parallel()
	require.Equal(t, "test", texttree.PrettyIdentifier("test", "", false))
	require.Equal(t, "  ├ ─test", texttree.PrettyIdentifier("test", "  │  ", false))
	require.Equal(t, "\t\t├\t─test", texttree.PrettyIdentifier("test", "\t\t│\t\t", false))
	require.Equal(t, "  └ ─test", texttree.PrettyIdentifier("test", "  │  ", true))
	require.Equal(t, "\t\t└\t─test", texttree.PrettyIdentifier("test", "\t\t│\t\t", true))
}

func TestIndent4Child(t *testing.T) {
	t.Parallel()
	require.Equal(t, "    │ ", texttree.Indent4Child("    ", false))
	require.Equal(t, "    │ ", texttree.Indent4Child("    ", true))
	require.Equal(t, "     │ ", texttree.Indent4Child("   │ ", true))
}
