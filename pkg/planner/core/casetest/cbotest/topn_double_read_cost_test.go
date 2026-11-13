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

package cbotest

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestTopNChoosesGloballyCheaperDoubleReadPath(t *testing.T) {
	// This regression covers the classic optimizer's DataSource candidate loop,
	// so it intentionally does not run under the Cascades test wrapper.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_cost_model_version = 2")

	tk.MustExec(`CREATE TABLE test.masked_event_journal (
		rec_id BIGINT NOT NULL AUTO_INCREMENT,
		route_code VARCHAR(32) DEFAULT NULL,
		state_flag INT NOT NULL DEFAULT 0,
		seen_at DATETIME DEFAULT NULL,
		event_code VARCHAR(64) DEFAULT NULL,
		shard_no INT DEFAULT NULL,
		batch_at DATETIME DEFAULT NULL,
		extra_note VARCHAR(128) DEFAULT NULL,
		PRIMARY KEY (rec_id) CLUSTERED,
		KEY idx_route_state_seen (route_code, state_flag, seen_at),
		KEY idx_event_shard_batch_route_state (event_code, shard_no, batch_at, route_code, state_flag)
	)`)

	tk.MustExec(`INSERT INTO test.masked_event_journal
		(route_code, state_flag, seen_at, event_code, shard_no, batch_at, extra_note)
	SELECT
		CASE WHEN n % 32 = 0 THEN 'route_alpha' ELSE 'route_beta' END AS route_code,
		CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END AS state_flag,
		TIMESTAMPADD(SECOND, n, '2026-01-01 00:00:00') AS seen_at,
		CASE
			WHEN n % 4 = 0 THEN 'event_target_314'
			WHEN n % 7 = 0 THEN 'event_group_red'
			ELSE 'event_group_blue'
		END AS event_code,
		n % 128 AS shard_no,
		CASE
			WHEN n % 1600 = 0 THEN '2026-06-27 11:56:03'
			ELSE TIMESTAMPADD(SECOND, n % 86400, '2026-06-01 00:00:00')
		END AS batch_at,
		CONCAT('masked payload ', n) AS extra_note
	FROM (
		SELECT d0.i + d1.i * 10 + d2.i * 100 + d3.i * 1000 + d4.i * 10000 + 1 AS n
		FROM
			(SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d0,
			(SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d1,
			(SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d2,
			(SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d3,
			(SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d4
	) seq
	WHERE n <= 100000`)

	// Keep both ANALYZE statements to match the issue's supplied reproduction.
	tk.MustExec("ANALYZE TABLE test.masked_event_journal")
	tk.MustExec("ANALYZE TABLE test.masked_event_journal")
	require.NoError(t, dom.StatsHandle().Update(context.Background(), dom.InfoSchema()))

	const explainSQL = `EXPLAIN FORMAT='verbose'
		SELECT rec_id, extra_note
		FROM test.masked_event_journal
		WHERE route_code = 'route_alpha'
		  AND event_code = 'event_target_314'
		  AND batch_at = '2026-06-27 11:56:03'
		  AND state_flag = 1
		ORDER BY seen_at DESC
		LIMIT 1`
	rows := tk.MustQuery(explainSQL).Rows()
	plan := testdata.ConvertRowsToStrings(rows)
	require.NotEmpty(t, plan)
	require.Contains(t, plan[0], "TopN")

	var hasProbeTopN, hasBuildTopN, hasTargetIndex bool
	for i, row := range plan {
		if strings.Contains(row, "TopN") && strings.Contains(row, "(Probe)") {
			hasProbeTopN = true
		}
		if strings.Contains(row, "TopN") && strings.Contains(row, "(Build)") {
			hasBuildTopN = true
		}
		if strings.Contains(row, "IndexRangeScan") && strings.Contains(row, "index:idx_event_shard_batch_route_state") {
			hasTargetIndex = true
			estRows, err := strconv.ParseFloat(rows[i][1].(string), 64)
			require.NoError(t, err)
			require.Greater(t, estRows, 10000.0)
		}
	}
	require.True(t, hasTargetIndex)
	require.True(t, hasProbeTopN)
	require.False(t, hasBuildTopN)
	require.Equal(t, plan, testdata.ConvertRowsToStrings(tk.MustQuery(explainSQL).Rows()))
}
