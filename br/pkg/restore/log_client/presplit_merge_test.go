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

package logclient_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/stretchr/testify/require"
)

func randomDMLFiles(r *rand.Rand, n int) []*logclient.LogDataFileInfo {
	key := func() []byte { return fmt.Appendf(nil, "k%06d", r.Intn(5000)) }
	files := make([]*logclient.LogDataFileInfo, 0, n)
	for range n {
		tableID := int64(r.Intn(40))
		if r.Intn(2) == 0 {
			tableID = 7
		}
		start, end := key(), key()
		if r.Intn(20) != 0 && string(start) > string(end) {
			start, end = end, start
		}
		files = append(files, &logclient.LogDataFileInfo{DataFileInfo: &backuppb.DataFileInfo{
			TableId:         tableID,
			IsMeta:          r.Intn(30) == 0,
			StartKey:        start,
			EndKey:          end,
			Length:          uint64(r.Intn(1 << 20)),
			NumberOfEntries: int64(r.Intn(1000)),
		}})
	}
	return files
}

func mergeOneByOne(files []*logclient.LogDataFileInfo, rules map[int64]*restoreutils.RewriteRules) (map[int64]*split.SplitHelper, int) {
	helpers := make(map[int64]*split.SplitHelper)
	count := 0
	for _, f := range files {
		if f.IsMeta {
			continue
		}
		if _, ok := rules[f.TableId]; !ok {
			continue
		}
		h, ok := helpers[f.TableId]
		if !ok {
			h = split.NewSplitHelper()
			helpers[f.TableId] = h
		}
		h.Merge(split.Valued{
			Key:   split.Span{StartKey: f.StartKey, EndKey: f.EndKey},
			Value: split.Value{Size: f.Length, Number: f.NumberOfEntries},
		})
		count++
	}
	return helpers, count
}

func contents(helpers map[int64]*split.SplitHelper) map[int64][]split.Valued {
	out := make(map[int64][]split.Valued, len(helpers))
	for tableID, h := range helpers {
		h.Traverse(func(v split.Valued) bool {
			out[tableID] = append(out[tableID], v)
			return true
		})
	}
	return out
}

func TestMergeDMLFilesByTableMatchesOneByOne(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	files := randomDMLFiles(r, 30000)
	rules := make(map[int64]*restoreutils.RewriteRules)
	for tableID := range int64(40) {
		if tableID%9 != 4 {
			rules[tableID] = &restoreutils.RewriteRules{}
		}
	}
	want, wantCount := mergeOneByOne(files, rules)

	for _, workers := range []int{0, 1, 3, 16, 64} {
		got, count, err := logclient.MergeDMLFilesByTable(context.Background(), iter.FromSlice(files), rules, workers)
		require.NoError(t, err)
		require.Equal(t, wantCount, count, "workers=%d", workers)
		require.Equal(t, contents(want), contents(got), "workers=%d", workers)
	}
}

func TestMergeDMLFilesByTableStopsOnIterError(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	files := randomDMLFiles(r, 20000)
	rules := map[int64]*restoreutils.RewriteRules{7: {}}
	boom := errors.New("meta read failed")
	it := iter.ConcatAll(iter.FromSlice(files), iter.Fail[*logclient.LogDataFileInfo](boom))

	got, count, err := logclient.MergeDMLFilesByTable(context.Background(), it, rules, 4)
	require.ErrorContains(t, err, boom.Error())
	require.Nil(t, got)
	require.Zero(t, count)
}

func TestMergeDMLFilesByTableStopsOnCancel(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	files := randomDMLFiles(r, 50000)
	rules := map[int64]*restoreutils.RewriteRules{}
	for _, f := range files {
		rules[f.TableId] = &restoreutils.RewriteRules{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	seen := 0
	it := iter.Tap(iter.FromSlice(files), func(*logclient.LogDataFileInfo) {
		seen++
		if seen == 10000 {
			cancel()
		}
	})
	got, count, err := logclient.MergeDMLFilesByTable(ctx, it, rules, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, got)
	require.Zero(t, count)
	require.Less(t, seen, len(files))
}
