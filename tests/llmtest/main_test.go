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

package main_test

import (
	"testing"

	"github.com/pingcap/tidb/tests/llmtest/generator"
	"github.com/pingcap/tidb/tests/llmtest/testcase"
	"github.com/stretchr/testify/require"
)

// TestAllTestCaseInGroup ensures that all test cases recorded in the testdata directory belongs
// to a group of the corresponding generator.
func TestAllTestCaseInGroup(t *testing.T) {
	promptGenerators := generator.AllPromptGenerators()

	for _, g := range promptGenerators {
		name := g.Name()
		caseManager, err := testcase.Open("testdata/" + name + ".json")
		require.NoError(t, err)

		allGroups := make(map[string]struct{})
		for _, groupInGenerator := range g.Groups() {
			allGroups[groupInGenerator] = struct{}{}
		}

		for _, caseGroup := range caseManager.AllGroups() {
			_, ok := allGroups[caseGroup]
			require.True(t, ok, "group %s not found in generator %s", caseGroup, name)
		}
	}
}

func TestAllTestCasePassOrKnown(t *testing.T) {
	promptGenerators := generator.AllPromptGenerators()

	for _, g := range promptGenerators {
		name := g.Name()
		caseManager, err := testcase.Open("testdata/" + name + ".json")
		require.NoError(t, err)

		for _, group := range caseManager.AllGroups() {
			cases := caseManager.ExistCases(group)
			for _, c := range cases {
				require.True(t, c.Pass || c.Known, "case %s in group %s is not pass or known", c.SQL, group)
			}
		}
	}
}
