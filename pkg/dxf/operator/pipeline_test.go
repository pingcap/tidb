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
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/stretchr/testify/require"
)

func TestPipelineAsyncMultiOperatorsWithoutError(t *testing.T) {
	words := `Bob hiT a ball, the hIt BALL flew far after it was hit.`
	splitted := strings.Split(words, " ")

	tasks := make([]stringTask, len(splitted))
	for i, word := range splitted {
		tasks[i] = stringTask(word)
	}

	for _, mockError := range []bool{false, true} {
		wctx := workerpool.NewContext(context.Background())

		var mostCommonWord stringTask
		source := NewSimpleDataSource(wctx, tasks)
		lower := makeLower(wctx)
		trimmer := makeTrimmer(wctx)
		counter := makeCounter(wctx, mockError)
		collector := makeCollector(wctx, &mostCommonWord)

		Compose[stringTask](source, lower)
		Compose[stringTask](lower, trimmer)
		Compose[stringTask](trimmer, counter)
		Compose[strCnt](counter, collector)

		pipeline := NewAsyncPipeline(source, lower, trimmer, counter, collector)
		require.Equal(
			t,
			"AsyncPipeline[SimpleDataSource[operator.stringTask] -> simpleOperator(AsyncOp[operator.stringTask, operator.stringTask]) -> simpleOperator(AsyncOp[operator.stringTask, operator.stringTask]) -> simpleOperator(AsyncOp[operator.stringTask, operator.strCnt]) -> simpleSink]",
			pipeline.String(),
		)
		err := pipeline.Execute()
		require.NoError(t, err)
		err = pipeline.Close()
		if mockError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.EqualValues(t, mostCommonWord, "hit")
		}
	}
}

type strCnt struct {
	str stringTask
	cnt int
}

type stringTask string

func (stringTask) RecoverArgs() (metricsLabel string, funcInfo string, err error) {
	return "", "", nil
}

func makeLower(ctx *workerpool.Context) *simpleOperator[stringTask, stringTask] {
	return newSimpleOperator(
		ctx,
		func(task stringTask) stringTask {
			return stringTask(strings.ToLower(string(task)))
		}, 3)
}

func makeTrimmer(ctx *workerpool.Context) *simpleOperator[stringTask, stringTask] {
	var nonAlphaRegex = regexp.MustCompile(`[^a-zA-Z0-9]+`)
	return newSimpleOperator(
		ctx,
		func(s stringTask) stringTask {
			return stringTask(nonAlphaRegex.ReplaceAllString(string(s), ""))
		}, 3)
}

func makeCounter(ctx *workerpool.Context, mockError bool) *simpleOperator[stringTask, strCnt] {
	strCntMap := make(map[stringTask]int)
	strCntMapMu := sync.Mutex{}
	return newSimpleOperator(
		ctx,
		func(s stringTask) strCnt {
			strCntMapMu.Lock()
			old := strCntMap[s]
			strCntMap[s] = old + 1
			strCntMapMu.Unlock()
			if mockError {
				ctx.OnError(errors.Errorf("mock error for testing"))
			}
			return strCnt{s, old + 1}
		}, 3)
}

func makeCollector(ctx *workerpool.Context, v *stringTask) *simpleSink[strCnt] {
	maxCnt := 0
	maxMu := sync.Mutex{}
	return newSimpleSink(
		ctx,
		func(sc strCnt) {
			maxMu.Lock()
			if sc.cnt > maxCnt {
				maxCnt = sc.cnt
				*v = sc.str
			}
			maxMu.Unlock()
		})
}
