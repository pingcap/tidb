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

package lbac

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestPolicyGrantGrantsAllDoesNotPolluteReadSlice(t *testing.T) {
	read := make([]*Label, 2)
	read[0] = &Label{Name: "read"}
	read = read[:1]
	require.Nil(t, read[:cap(read)][1])

	write := []*Label{{Name: "write"}}
	g := &policyGrant{
		read:  read,
		write: write,
	}

	grants := g.grants(ast.SecurityLabelAccessTypeAll)
	require.Len(t, grants, 2)
	require.Same(t, read[0], grants[0])
	require.Same(t, write[0], grants[1])

	// Ensure the ALL branch didn't append into g.read's underlying array.
	require.Nil(t, read[:cap(read)][1])
}

func TestBuildCacheGrantAllAccessNoDuplicateGrantEntries(t *testing.T) {
	cache := BuildCache(
		[]Component{
			{
				Name:   "classif",
				Type:   ast.LBACComponentTypeArray,
				Values: []string{"A", "B"},
			},
		},
		[]Policy{
			{
				Name:           "policy1",
				ComponentNames: []string{"classif"},
			},
		},
		[]Label{
			{
				Name:       "label1",
				PolicyName: "policy1",
				Components: map[string][]string{"classif": {"A"}},
			},
		},
		[]UserLabel{
			{
				UserName:   "alice",
				Host:       "%",
				LabelName:  "label1",
				AccessType: ast.SecurityLabelAccessTypeAll,
			},
		},
		nil,
	)

	pg := cache.policyGrant("alice", "%", "policy1")
	require.NotNil(t, pg)
	require.Len(t, pg.read, 1)
	require.Len(t, pg.write, 1)
}
