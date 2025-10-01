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

	runSelectWithCheck := func(where string, skip, limit int) {
		require.GreaterOrEqual(t, skip, 0)
		if skip > 0 {
			require.GreaterOrEqual(t, limit, 0)
		}

		message := fmt.Sprintf("seed: %d, where: %s, limit: %d", seed, where, limit)
		var sb strings.Builder
		sb.WriteString("select /*+ index_lookup_pushdown(t, a)*/ * from t where ")
		sb.WriteString(where)
		if skip > 0 {
			sb.WriteString(fmt.Sprintf(" limit %d, %d", skip, limit))
		} else if limit >= 0 {
			sb.WriteString(fmt.Sprintf(" limit %d", limit))
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
		matchCondList := tk.MustQuery("select /*+ use_index(t) */* from t where " + where + " order by id").Rows()
		if limit == 0 || skip >= len(matchCondList) {
			require.Len(t, actual, 0, message)
		} else if limit < 0 {
			// no limit two results should have same members
			require.ElementsMatch(t, matchCondList, actual, message)
		} else {
			expectRowCnt := limit
			if skip+limit > len(matchCondList) {
				expectRowCnt = len(matchCondList) - skip
			}
			require.Len(t, actual, expectRowCnt, message)
			require.Subset(t, matchCondList, actual, message)
		}

		// check in analyze the index is lookup locally
		message = fmt.Sprintf("%s, %s", message, analyzeResult.String())
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
				require.NoError(t, err, message)
				if len(actual) > 0 {
					require.Greater(t, localLookUpRowCnt, 0, message)
				}
				require.Equal(t, analyzeRows[i-1][2], row[2], message)
				require.Equal(t, analyzeRows[localIndexLookUpIndex][2], row[2], message)
				analyzeVerified = true
				break
			}
		}
		require.True(t, analyzeVerified, analyzeResult.String())
	}

	runSelectWithCheck("1", 0, -1)
	runSelectWithCheck("1", 0, r.Intn(total*2))
	runSelectWithCheck("1", total/2, r.Intn(total))
	runSelectWithCheck("1", total-10, 20)
	runSelectWithCheck("1", total, 10)
	runSelectWithCheck("1", 10, 0)
	runSelectWithCheck(fmt.Sprintf("a = %d", randIndexVal()), 0, -1)
	runSelectWithCheck(fmt.Sprintf("a = %d", randIndexVal()), 0, 25)
	runSelectWithCheck(fmt.Sprintf("a < %d", randIndexVal()), 0, -1)
	runSelectWithCheck(fmt.Sprintf("a < %d", randIndexVal()), 0, r.Intn(100)+1)
	runSelectWithCheck(fmt.Sprintf("a > %d", randIndexVal()), 0, -1)
	runSelectWithCheck(fmt.Sprintf("a > %d", randIndexVal()), 0, r.Intn(100)+1)
	start := randIndexVal()
	runSelectWithCheck(fmt.Sprintf("a >= %d and a < %d", start, start+r.Intn(5)+1), 0, -1)
	start = randIndexVal()
	runSelectWithCheck(fmt.Sprintf("a >= %d and a < %d", start, start+r.Intn(5)+1), 0, r.Intn(50)+1)
	runSelectWithCheck(fmt.Sprintf("a > %d and b < %d", randIndexVal(), r.Int63()), 0, -1)
	runSelectWithCheck(fmt.Sprintf("a > %d and b < %d", randIndexVal(), r.Int63()), 0, r.Intn(50)+1)
}
