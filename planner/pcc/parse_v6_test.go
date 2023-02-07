// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pcc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var explainV6SQL = `explain select * from t t1, (select ta.b, tb.a from t ta, t tb where ta.b=tb.a) t2 where t1.a=t2.b`

var explainV6Result = `
+--------------------------------+----------+-----------+----------------------+--------------------------------------------+
| id                             | estRows  | task      | access object        | operator info                              |
+--------------------------------+----------+-----------+----------------------+--------------------------------------------+
| HashJoin_24                    | 15609.38 | root      |                      | inner join, equal:[eq(test.t.b, test.t.a)] |
| ├─IndexReader_52(Build)        | 9990.00  | root      |                      | index:IndexFullScan_51                     |
| │ └─IndexFullScan_51           | 9990.00  | cop[tikv] | table:tb, index:a(a) | keep order:false, stats:pseudo             |
| └─HashJoin_40(Probe)           | 12487.50 | root      |                      | inner join, equal:[eq(test.t.a, test.t.b)] |
|   ├─TableReader_44(Build)      | 9990.00  | root      |                      | data:Selection_43                          |
|   │ └─Selection_43             | 9990.00  | cop[tikv] |                      | not(isnull(test.t.b))                      |
|   │   └─TableFullScan_42       | 10000.00 | cop[tikv] | table:ta             | keep order:false, stats:pseudo             |
|   └─TableReader_47(Probe)      | 9990.00  | root      |                      | data:Selection_46                          |
|     └─Selection_46             | 9990.00  | cop[tikv] |                      | not(isnull(test.t.a))                      |
|       └─TableFullScan_45       | 10000.00 | cop[tikv] | table:t1             | keep order:false, stats:pseudo             |
+--------------------------------+----------+-----------+----------------------+--------------------------------------------+
`

func TestParseV6(t *testing.T) {
	p, err := ParseText(explainV6SQL, explainV6Result, V6)
	require.NoError(t, err)
	require.Equal(t, p.SQL, explainV6SQL)
	require.Equal(t, p.Root.ID(), "HashJoin_24")
	require.Len(t, p.Root.Children(), 2)
	require.Equal(t, p.Root.Children()[0].ID(), "IndexReader_52(Build)")
	require.Equal(t, p.Root.Children()[0].Children()[0].ID(), "IndexFullScan_51")
}

func TestParsePointGetV6(t *testing.T) {
	result := `
+-------------------+---------+------+---------------+----------------------------------------------+
| id                | estRows | task | access object | operator info                                |
+-------------------+---------+------+---------------+----------------------------------------------+
| Batch_Point_Get_1 | 3.00    | root | table:t       | handle:[3 4 5], keep order:false, desc:false |
+-------------------+---------+------+---------------+----------------------------------------------+
`
	_, err := ParseText("", result, V6)
	require.NoError(t, err)
}

func TestParseAggV6(t *testing.T) {
	p1 := `
+---------------------------+----------+-----------+---------------+--------------------------------------------------+
| id                        | estRows  | task      | access object | operator info                                    |
+---------------------------+----------+-----------+---------------+--------------------------------------------------+
| HashAgg_9                 | 8000.00  | root      |               | group by:test.t.b, funcs:sum(Column#4)->Column#3 |
| └─TableReader_10          | 8000.00  | root      |               | data:HashAgg_5                                   |
|   └─HashAgg_5             | 8000.00  | cop[tikv] |               | group by:test.t.b, funcs:sum(test.t.a)->Column#4 |
|     └─TableFullScan_8     | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo                   |
+---------------------------+----------+-----------+---------------+--------------------------------------------------+
`
	p, err := ParseText("", p1, V6)
	require.NoError(t, err)
	require.Equal(t, p.Root.ID(), "HashAgg_9")
	require.Equal(t, p.Root.Type(), OpTypeHashAgg)
	require.Equal(t, p.Root.Children()[0].Children()[0].ID(), "HashAgg_5")
	require.Equal(t, p.Root.Children()[0].Children()[0].Type(), OpTypeHashAgg)

	p2 := `
+----------------------------+----------+-----------+---------------------+--------------------------------+
| id                         | estRows  | task      | access object       | operator info                  |
+----------------------------+----------+-----------+---------------------+--------------------------------+
| StreamAgg_15               | 1.00     | root      |                     | funcs:sum(Column#5)->Column#4  |
| └─IndexReader_16           | 1.00     | root      |                     | index:StreamAgg_8              |
|   └─StreamAgg_8            | 1.00     | cop[tikv] |                     | funcs:sum(test.t.a)->Column#5  |
|     └─IndexFullScan_14     | 10000.00 | cop[tikv] | table:t, index:a(a) | keep order:false, stats:pseudo |
+----------------------------+----------+-----------+---------------------+--------------------------------+
`
	p, err = ParseText("", p2, V6)
	require.NoError(t, err)
	require.Equal(t, p.Root.ID(), "StreamAgg_15")
	require.Equal(t, p.Root.Type(), OpTypeStreamAgg)
	require.Equal(t, p.Root.Children()[0].Children()[0].Children()[0].ID(), "IndexFullScan_14")
}

func TestParseSelectLockV4(t *testing.T) {
	p := `
+-----------------------------+----------+-----------+---------------+--------------------------------+
| id                          | estRows  | task      | access object | operator info                  |
+-----------------------------+----------+-----------+---------------+--------------------------------+
| Projection_5                | 10.00    | root      |               | test.t.a                       |
| └─SelectLock_6              | 10.00    | root      |               | for update 0                   |
|   └─TableReader_9           | 10.00    | root      |               | data:Selection_8               |
|     └─Selection_8           | 10.00    | cop[tikv] |               | eq(test.t.a, 1)                |
|       └─TableFullScan_7     | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo |
+-----------------------------+----------+-----------+---------------+--------------------------------+
`
	plan, err := ParseText("", p, V6)
	require.NoError(t, err)
	require.Equal(t, plan.Root.ID(), "Projection_5")
	require.Equal(t, plan.Root.Children()[0].Type(), OpTypeSelectLock)
}
