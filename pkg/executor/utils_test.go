// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"runtime"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBatchRetrieverHelper(t *testing.T) {
	rangeStarts := make([]int, 0)
	rangeEnds := make([]int, 0)
	collect := func(start, end int) error {
		rangeStarts = append(rangeStarts, start)
		rangeEnds = append(rangeEnds, end)
		return nil
	}

	r := &batchRetrieverHelper{}
	err := r.nextBatch(collect)
	require.NoError(t, err)
	require.Equal(t, rangeStarts, []int{})
	require.Equal(t, rangeEnds, []int{})

	r = &batchRetrieverHelper{
		retrieved: true,
		batchSize: 3,
		totalRows: 10,
	}
	err = r.nextBatch(collect)
	require.NoError(t, err)
	require.Equal(t, rangeStarts, []int{})
	require.Equal(t, rangeEnds, []int{})

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 10,
	}
	err = r.nextBatch(func(start, end int) error {
		return errors.New("some error")
	})
	require.Error(t, err)
	require.True(t, r.retrieved)

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 10,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		require.NoError(t, err)
	}
	require.Equal(t, rangeStarts, []int{0, 3, 6, 9})
	require.Equal(t, rangeEnds, []int{3, 6, 9, 10})
	rangeStarts = rangeStarts[:0]
	rangeEnds = rangeEnds[:0]

	r = &batchRetrieverHelper{
		batchSize: 3,
		totalRows: 9,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		require.NoError(t, err)
	}
	require.Equal(t, rangeStarts, []int{0, 3, 6})
	require.Equal(t, rangeEnds, []int{3, 6, 9})
	rangeStarts = rangeStarts[:0]
	rangeEnds = rangeEnds[:0]

	r = &batchRetrieverHelper{
		batchSize: 100,
		totalRows: 10,
	}
	for !r.retrieved {
		err = r.nextBatch(collect)
		require.NoError(t, err)
	}
	require.Equal(t, rangeStarts, []int{0})
	require.Equal(t, rangeEnds, []int{10})
}

func TestEqualDatumsAsBinary(t *testing.T) {
	tests := []struct {
		a    []any
		b    []any
		same bool
	}{
		// Positive cases
		{[]any{1}, []any{1}, true},
		{[]any{1, "aa"}, []any{1, "aa"}, true},
		{[]any{1, "aa", 1}, []any{1, "aa", 1}, true},

		// negative cases
		{[]any{1}, []any{2}, false},
		{[]any{1, "a"}, []any{1, "aaaaaa"}, false},
		{[]any{1, "aa", 3}, []any{1, "aa", 2}, false},

		// Corner cases
		{[]any{}, []any{}, true},
		{[]any{nil}, []any{nil}, true},
		{[]any{}, []any{1}, false},
		{[]any{1}, []any{1, 1}, false},
		{[]any{nil}, []any{1}, false},
	}
	ctx := core.MockContext()
	base := exec.NewBaseExecutor(ctx, nil, 0)
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	e := &InsertValues{BaseExecutor: base}
	for _, tt := range tests {
		res, err := e.equalDatumsAsBinary(types.MakeDatums(tt.a...), types.MakeDatums(tt.b...))
		require.NoError(t, err)
		require.Equal(t, tt.same, res)
	}
}

func TestEncodePasswordWithPlugin(t *testing.T) {
	hashString := "*3D56A309CD04FA2EEF181462E59011F075C89548"
	u := &ast.UserSpec{
		User: &auth.UserIdentity{
			Username: "test",
		},
		AuthOpt: &ast.AuthOption{
			ByAuthString: false,
			AuthString:   "xxx",
			HashString:   hashString,
		},
	}

	p := &extension.AuthPlugin{
		ValidateAuthString: func(s string) bool {
			return false
		},
		GenerateAuthString: func(s string) (string, bool) {
			if s == "xxx" {
				return "xxxxxxx", true
			}
			return "", false
		},
	}

	u.AuthOpt.ByAuthString = false
	_, ok := encodePasswordWithPlugin(*u, p, "")
	require.False(t, ok)

	u.AuthOpt.AuthString = "xxx"
	u.AuthOpt.ByAuthString = true
	pwd, ok := encodePasswordWithPlugin(*u, p, "")
	require.True(t, ok)
	require.Equal(t, "xxxxxxx", pwd)

	u.AuthOpt = nil
	pwd, ok = encodePasswordWithPlugin(*u, p, "")
	require.True(t, ok)
	require.Equal(t, "", pwd)
}

func TestWorkerPool(t *testing.T) {
	var (
		list []int
		lock sync.Mutex
	)
	push := func(i int) {
		lock.Lock()
		list = append(list, i)
		lock.Unlock()
	}
	clean := func() {
		lock.Lock()
		list = list[:0]
		lock.Unlock()
	}

	t.Run("SingleWorker", func(t *testing.T) {
		clean()
		pool := &workerPool{
			needSpawn: func(workers, tasks uint32) bool {
				return workers < 1 && tasks > 0
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		pool.submit(func() {
			push(1)
			wg.Add(1)
			pool.submit(func() {
				push(3)
				runtime.Gosched()
				push(4)
				wg.Done()
			})
			runtime.Gosched()
			push(2)
			wg.Done()
		})
		wg.Wait()
		require.Equal(t, []int{1, 2, 3, 4}, list)
	})

	t.Run("TwoWorkers", func(t *testing.T) {
		clean()
		pool := &workerPool{
			needSpawn: func(workers, tasks uint32) bool {
				return workers < 2 && tasks > 0
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		pool.submit(func() {
			push(1)
			wg.Add(1)
			pool.submit(func() {
				push(3)
				runtime.Gosched()
				push(4)
				wg.Done()
			})
			runtime.Gosched()
			push(2)
			wg.Done()
		})
		wg.Wait()
		require.Equal(t, []int{1, 3, 2, 4}, list)
	})

	t.Run("TolerateOnePendingTask", func(t *testing.T) {
		clean()
		pool := &workerPool{
			needSpawn: func(workers, tasks uint32) bool {
				return workers < 2 && tasks > 1
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		pool.submit(func() {
			push(1)
			wg.Add(1)
			pool.submit(func() {
				push(3)
				runtime.Gosched()
				push(4)
				wg.Done()
			})
			runtime.Gosched()
			push(2)
			wg.Done()
		})
		wg.Wait()
		require.Equal(t, []int{1, 2, 3, 4}, list)
	})
}

func TestEncodedPassword(t *testing.T) {
	hashString := "*3D56A309CD04FA2EEF181462E59011F075C89548"
	hashCachingString := "0123456789012345678901234567890123456789012345678901234567890123456789"
	u := ast.UserSpec{
		User: &auth.UserIdentity{
			Username: "test",
		},
		AuthOpt: &ast.AuthOption{
			ByAuthString: false,
			AuthString:   "xxx",
			HashString:   hashString,
		},
	}
	pwd, ok := encodedPassword(&u, "")
	require.True(t, ok)
	require.Equal(t, u.AuthOpt.HashString, pwd)

	u.AuthOpt.HashString = "not-good-password-format"
	_, ok = encodedPassword(&u, "")
	require.False(t, ok)

	u.AuthOpt.ByAuthString = true
	// mysql_native_password
	pwd, ok = encodedPassword(&u, "")
	require.True(t, ok)
	require.Equal(t, hashString, pwd)
	// caching_sha2_password
	u.AuthOpt.HashString = hashCachingString
	pwd, ok = encodedPassword(&u, mysql.AuthCachingSha2Password)
	require.True(t, ok)
	require.Len(t, pwd, mysql.SHAPWDHashLen)

	u.AuthOpt.AuthString = ""
	pwd, ok = encodedPassword(&u, "")
	require.True(t, ok)
	require.Equal(t, "", pwd)
}
