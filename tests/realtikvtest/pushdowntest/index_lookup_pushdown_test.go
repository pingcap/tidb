// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pushdowntest

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRealTiKVIndexLookUpPushDown(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id bigint primary key, a bigint, b bigint, index a(a))")
	seed := time.Now().UnixNano()
	logutil.BgLogger().Info("Run TestRealTiKVIndexLookUpPushDown with seed", zap.Int64("seed", seed))
	r := rand.New(rand.NewSource(seed))
	batch := 100
	total := batch * 20
	indexValEnd := 100
	randIndexVal := func() int {
		return r.Intn(indexValEnd)
	}
	for i := 0; i < total; i += batch {
		values := make([]string, 0, batch)
		for j := 0; j < batch; j++ {
			values = append(values, fmt.Sprintf("(%d, %d, %d)", i+j, randIndexVal(), r.Int63()))
		}
		tk.MustExec("insert into t values " + strings.Join(values, ","))
	}

	runSelectWithCheck := func(where string, limit int) {
		message := fmt.Sprintf("seed: %d, where: %s, limit: %d", seed, where, limit)

		var sb strings.Builder
		sb.WriteString("select /*+ index_lookup_pushdown(t, a)*/ * from t where ")
		sb.WriteString(where)
		if limit > 0 {
			sb.WriteString(" limit ")
			sb.WriteString(strconv.Itoa(limit))
		}

		// make sure the query uses index lookup
		analyzeResult := tk.MustQuery("explain analyze " + sb.String())
		require.Contains(t, analyzeResult.String(), "LocalIndexLookUp", analyzeResult.String())

		// get actual result
		rs := tk.MustQuery(sb.String())
		actual := rs.Rows()
		idSets := make(map[string]struct{}, len(actual))
		for _, row := range actual {
			id := row[0].(string)
			_, dup := idSets[id]
			require.False(t, dup, "dupID: "+id+", "+message)
			idSets[row[0].(string)] = struct{}{}
		}

		// use table scan
		expected := tk.MustQuery("select /*+ use_index(t) */* from t where " + where + " order by id").Rows()
		if limit > 0 && len(expected) > limit {
			// expected is a subset of actual
			require.Len(t, actual, limit)
			require.Subset(t, expected, actual, message)
		} else {
			// two results should have same members
			require.ElementsMatch(t, expected, actual, message)
		}

		// check in analyze the index is lookup locally
		localIndexLookUpIndex := -1
		analyzeVerified := false
		analyzeRows := analyzeResult.Rows()
		for i, row := range analyzeRows {
			if strings.Contains(row[0].(string), "LocalIndexLookUp") {
				localIndexLookUpIndex = i
				continue
			}

			if strings.Contains(row[0].(string), "TableRowIDScan") && strings.Contains(row[3].(string), "cop[tikv]") {
				localLookUpRowCnt, err := strconv.Atoi(row[2].(string))
				require.NoError(t, err, analyzeResult.String())
				require.Greater(t, localLookUpRowCnt, 0, analyzeResult.String())
				require.Equal(t, analyzeRows[i-1][2], row[2], analyzeResult.String())
				require.Equal(t, analyzeRows[localIndexLookUpIndex][2], row[2], analyzeResult.String())
				analyzeVerified = true
				break
			}
		}
		require.True(t, analyzeVerified, analyzeResult.String())
	}

	runSelectWithCheck("1", -1)
	runSelectWithCheck("1", r.Intn(256))
	runSelectWithCheck(fmt.Sprintf("a = %d", randIndexVal()), -1)
	runSelectWithCheck(fmt.Sprintf("a = %d", randIndexVal()), 25)
	runSelectWithCheck(fmt.Sprintf("a < %d", randIndexVal()), -1)
	runSelectWithCheck(fmt.Sprintf("a < %d", randIndexVal()), r.Intn(100)+1)
	runSelectWithCheck(fmt.Sprintf("a > %d", randIndexVal()), -1)
	runSelectWithCheck(fmt.Sprintf("a > %d", randIndexVal()), r.Intn(100)+1)
	start := randIndexVal()
	runSelectWithCheck(fmt.Sprintf("a >= %d and a < %d", start, start+r.Intn(5)+1), -1)
	start = randIndexVal()
	runSelectWithCheck(fmt.Sprintf("a >= %d and a < %d", start, start+r.Intn(5)+1), r.Intn(50)+1)
	runSelectWithCheck(fmt.Sprintf("a > %d and b < %d", randIndexVal(), r.Int63()), -1)
	runSelectWithCheck(fmt.Sprintf("a > %d and b < %d", randIndexVal(), r.Int63()), r.Intn(50)+1)
}
