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

package indexadvisor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetBasic(t *testing.T) {
	s := NewSet[Query]()
	s.Add(Query{Text: "q1"}, Query{Text: "q2"}, Query{Text: "q3"})
	require.True(t, s.Contains(Query{Text: "q1"}))
	require.True(t, s.Contains(Query{Text: "q2"}))
	require.True(t, s.Contains(Query{Text: "q3"}))
	require.False(t, s.Contains(Query{Text: "q4"}))
	require.Equal(t, 3, s.Size())
	require.Equal(t, []Query{{Text: "q1"}, {Text: "q2"}, {Text: "q3"}}, s.ToList())
	s.Remove(Query{Text: "q2"})
	require.False(t, s.Contains(Query{Text: "q2"}))
	require.Equal(t, 2, s.Size())

	clonedS := s.Clone()
	require.True(t, clonedS.Contains(Query{Text: "q1"}))
	s.Remove(Query{Text: "q1"})
	require.False(t, s.Contains(Query{Text: "q1"}))
	require.True(t, clonedS.Contains(Query{Text: "q1"}))
	require.Equal(t, 2, clonedS.Size())
}

func TestSetOperation(t *testing.T) {
	s1 := NewSet[Query]()
	s1.Add(Query{Text: "q1"}, Query{Text: "q2"}, Query{Text: "q3"})
	s2 := NewSet[Query]()
	s2.Add(Query{Text: "q2"}, Query{Text: "q3"}, Query{Text: "q4"})
	unionSet := UnionSet(s1, s2)
	require.Equal(t, []Query{{Text: "q1"}, {Text: "q2"}, {Text: "q3"}, {Text: "q4"}}, unionSet.ToList())

	andSet := AndSet(s1, s2)
	require.Equal(t, []Query{{Text: "q2"}, {Text: "q3"}}, andSet.ToList())

	diffSet := DiffSet(s1, s2)
	require.Equal(t, []Query{{Text: "q1"}}, diffSet.ToList())
	diffSet = DiffSet(s2, s1)
	require.Equal(t, []Query{{Text: "q4"}}, diffSet.ToList())
}

func TestSetCombination(t *testing.T) {
	s := NewSet[Query]()
	s.Add(Query{Text: "q1"}, Query{Text: "q2"}, Query{Text: "q3"}, Query{Text: "q4"})

	setListStr := func(setList []Set[Query]) string {
		var tmp []string
		for _, set := range setList {
			tmp = append(tmp, set.String())
		}
		return strings.Join(tmp, ", ")
	}

	s1 := CombSet(s, 1)
	require.Equal(t, "{q1}, {q2}, {q3}, {q4}", setListStr(s1))

	s2 := CombSet(s, 2)
	require.Equal(t, "{q1, q2}, {q1, q3}, {q1, q4}, {q2, q3}, {q2, q4}, {q3, q4}", setListStr(s2))

	s3 := CombSet(s, 3)
	require.Equal(t, "{q1, q2, q3}, {q1, q2, q4}, {q1, q3, q4}, {q2, q3, q4}", setListStr(s3))

	s4 := CombSet(s, 4)
	require.Equal(t, "{q1, q2, q3, q4}", setListStr(s4))

	s5 := CombSet(s, 5)
	require.Equal(t, "", setListStr(s5))
}
