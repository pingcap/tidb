// Copyright 2025 PingCAP, Inc.
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

package lbac

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestCheckLabelNameCaseInsensitive(t *testing.T) {
	component := Component{
		Name:   "comp",
		Type:   ast.LBACComponentTypeArray,
		Values: []string{"A", "B"},
	}
	policy := Policy{
		Name:           "pol",
		ComponentNames: []string{"comp"},
	}
	label := Label{
		Name:       "label_a",
		PolicyName: "pol",
		Components: map[string][]string{"comp": {"A"}},
	}
	cache := BuildCache(
		[]Component{component},
		[]Policy{policy},
		[]Label{label},
		[]UserLabel{
			{
				UserName:   "alice",
				Host:       "%",
				LabelName:  "label_a",
				AccessType: ast.SecurityLabelAccessTypeRead,
			},
		},
		nil,
	)

	checker, err := NewSecurityLabelAccessChecker(cache, "alice", "%", ast.SecurityLabelAccessTypeRead)
	require.NoError(t, err)

	ok, err := checker.CheckLabelName("POL", "LABEL_A")
	require.NoError(t, err)
	require.True(t, ok)

	_, err = checker.CheckLabelName("POL", "MISSING")
	require.ErrorIs(t, err, ErrLabelNotFound)
}
