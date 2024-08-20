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
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineAsyncMultiOperators(t *testing.T) {
	words := `Bob hiT a ball, the hIt BALL flew far after it was hit.`
	var mostCommonWord stringTask
	splitter := makeSplitter(words)
	lower := makeLower()
	trimmer := makeTrimmer()
	counter := makeCounter()
	collector := makeCollector(&mostCommonWord)

	Compose[stringTask](splitter, lower)
	Compose[stringTask](lower, trimmer)
	Compose[stringTask](trimmer, counter)
	Compose[strCnt](counter, collector)

	pipeline := NewAsyncPipeline(splitter, lower, trimmer, counter, collector)
	require.Equal(
		t,
		"AsyncPipeline[simpleSource -> simpleOperator(AsyncOp[operator.stringTask, operator.stringTask]) -> simpleOperator(AsyncOp[operator.stringTask, operator.stringTask]) -> simpleOperator(AsyncOp[operator.stringTask, operator.strCnt]) -> simpleSink]",
		pipeline.String(),
	)
	err := pipeline.Execute()
	require.NoError(t, err)
	err = pipeline.Close()
	require.NoError(t, err)
	require.EqualValues(t, mostCommonWord, "hit")
}

type strCnt struct {
	str stringTask
	cnt int
}

func makeSplitter(s string) *simpleSource[stringTask] {
	ss := strings.Split(s, " ")
	src := newSimpleSource(func() stringTask {
		if len(ss) == 0 {
			return ""
		}
		ret := ss[0]
		ss = ss[1:]
		return stringTask(ret)
	})
	return src
}

type stringTask string

func (stringTask) RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool) {
	return "", "", nil, false
}

func makeLower() *simpleOperator[stringTask, stringTask] {
	return newSimpleOperator(func(task stringTask) stringTask {
		return stringTask(strings.ToLower(string(task)))
	}, 3)
}

func makeTrimmer() *simpleOperator[stringTask, stringTask] {
	var nonAlphaRegex = regexp.MustCompile(`[^a-zA-Z0-9]+`)
	return newSimpleOperator(func(s stringTask) stringTask {
		return stringTask(nonAlphaRegex.ReplaceAllString(string(s), ""))
	}, 3)
}

func makeCounter() *simpleOperator[stringTask, strCnt] {
	strCntMap := make(map[stringTask]int)
	strCntMapMu := sync.Mutex{}
	return newSimpleOperator(func(s stringTask) strCnt {
		strCntMapMu.Lock()
		old := strCntMap[s]
		strCntMap[s] = old + 1
		strCntMapMu.Unlock()
		return strCnt{s, old + 1}
	}, 3)
}

func makeCollector(v *stringTask) *simpleSink[strCnt] {
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
