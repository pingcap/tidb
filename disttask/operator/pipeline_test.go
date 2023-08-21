// Copyright 2023 PingCAP, Inc.
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

package operator

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineAsyncMultiOperators(t *testing.T) {
	words := `Bob hiT a ball, the hIt BALL flew far after it was hit.`
	var mostCommonWord string
	splitter := makeSplitter(words)
	lower := makeLower()
	counter := makeCounter()
	collector := makeCollector(&mostCommonWord)

	Compose[string](splitter, lower)
	Compose[string](lower, counter)
	Compose[strCnt](counter, collector)

	pipeline := NewAsyncPipeline(splitter, lower, counter, collector)
	err := pipeline.Execute()
	require.NoError(t, err)
	pipeline.Close()
	require.Equal(t, mostCommonWord, "hit")
}

type strCnt struct {
	str string
	cnt int
}

func makeSplitter(s string) *simpleSource[string] {
	ss := strings.Split(s, " ")
	src := newSimpleSource(func() string {
		if len(ss) == 0 {
			return ""
		}
		ret := ss[0]
		ss = ss[1:]
		return ret
	})
	return src
}

func makeLower() *simpleOperator[string, string] {
	return newSimpleOperator(strings.ToLower, 3)
}

func makeCounter() *simpleOperator[string, strCnt] {
	strCntMap := make(map[string]int)
	strCntMapMu := sync.Mutex{}
	return newSimpleOperator(func(s string) strCnt {
		strCntMapMu.Lock()
		old := strCntMap[s]
		strCntMap[s] = old + 1
		strCntMapMu.Unlock()
		return strCnt{s, old + 1}
	}, 3)
}

func makeCollector(v *string) *simpleSink[strCnt] {
	maxCnt := 0
	maxMu := sync.Mutex{}
	return newSimpleSink(func(sc strCnt) {
		maxMu.Lock()
		if sc.cnt > maxCnt {
			maxCnt = sc.cnt
			*v = sc.str
		}
		maxMu.Unlock()
	})
}
