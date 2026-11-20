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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func leadingHint(qb string) *ast.TableOptimizerHint {
	return &ast.TableOptimizerHint{
		QBName:   ast.NewCIStr(qb),
		HintName: ast.NewCIStr(hint.HintLeading),
	}
}

func namedHint(name string) *ast.TableOptimizerHint {
	return &ast.TableOptimizerHint{
		HintName: ast.NewCIStr(name),
	}
}

func TestDropOverriddenLeadingHints(t *testing.T) {
	// No plan-derived LEADING: originals pass through untouched.
	orig := []*ast.TableOptimizerHint{leadingHint(""), namedHint(hint.HintHJ)}
	require.Len(t, dropOverriddenLeadingHints(nil, orig), 2)

	// Same query block: the original LEADING is dropped, others kept.
	plan := []*ast.TableOptimizerHint{leadingHint("")}
	res := dropOverriddenLeadingHints(plan, orig)
	require.Len(t, res, 1)
	require.Equal(t, hint.HintHJ, res[0].HintName.L)

	// Different query block: the original LEADING is kept.
	plan = []*ast.TableOptimizerHint{leadingHint("sel_1")}
	orig = []*ast.TableOptimizerHint{leadingHint("sel_2")}
	require.Len(t, dropOverriddenLeadingHints(plan, orig), 1)

	// Same query block, QB-qualified on both sides.
	orig = []*ast.TableOptimizerHint{leadingHint("sel_1")}
	require.Empty(t, dropOverriddenLeadingHints(plan, orig))

	// Empty originals.
	require.Empty(t, dropOverriddenLeadingHints(plan, nil))
}
