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

package session

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/sortexec"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
)

func prepareTPCHLineitemSortBenchData(se sessiontypes.Session, rowCount int) {
	mustExecute(se, "drop table if exists lineitem")
	mustExecute(se, `
create table lineitem (
  l_orderkey bigint not null,
  l_linenumber int not null,
  l_returnflag char(1) not null,
  l_linestatus char(1) not null,
  l_shipdate date not null,
  primary key (l_orderkey, l_linenumber)
)`)

	rng := rand.New(rand.NewSource(1))
	base := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)

	const (
		linesPerOrder = 5
		batchSize     = 500
	)

	var bld []byte
	bld = make([]byte, 0, 1<<20)
	for start := 0; start < rowCount; start += batchSize {
		end := start + batchSize
		if end > rowCount {
			end = rowCount
		}

		bld = bld[:0]
		bld = append(bld, "insert into lineitem (l_orderkey, l_linenumber, l_returnflag, l_linestatus, l_shipdate) values "...)

		for i := start; i < end; i++ {
			if i != start {
				bld = append(bld, ',')
			}
			orderKey := i / linesPerOrder
			lineNo := (i % linesPerOrder) + 1

			var returnFlag byte
			switch i % 3 {
			case 0:
				returnFlag = 'N'
			case 1:
				returnFlag = 'R'
			default:
				returnFlag = 'A'
			}
			var lineStatus byte
			if i%2 == 0 {
				lineStatus = 'O'
			} else {
				lineStatus = 'F'
			}

			shipDate := base.AddDate(0, 0, rng.Intn(7*365)).Format("2006-01-02")

			bld = append(bld, '(')
			bld = strconv.AppendInt(bld, int64(orderKey), 10)
			bld = append(bld, ',')
			bld = strconv.AppendInt(bld, int64(lineNo), 10)
			bld = append(bld, ',', '\'')
			bld = append(bld, returnFlag)
			bld = append(bld, '\'', ',', '\'')
			bld = append(bld, lineStatus)
			bld = append(bld, '\'', ',', '\'')
			bld = append(bld, shipDate...)
			bld = append(bld, '\'', ')')
		}

		mustExecute(se, string(bld))
	}
}

func BenchmarkTPCHLineitemOrderBySort(b *testing.B) {
	defer sortexec.SetAQSortEnabled(false)

	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		do.Close()
		st.Close()
	}()

	const rowCount = 100_000
	prepareTPCHLineitemSortBenchData(se, rowCount)

	workloads := []struct {
		name     string
		query    string
		readRows int
	}{
		{
			name: "OrderByFullResult",
			query: `
select
  l_returnflag, l_linestatus, l_shipdate, l_orderkey, l_linenumber
from lineitem
order by l_returnflag, l_linestatus, l_shipdate, l_orderkey, l_linenumber`,
			readRows: rowCount,
		},
		{
			name: "OrderByRowNumberSum",
			query: `
select sum(rn) from (
  select row_number() over (
    order by l_returnflag, l_linestatus, l_shipdate, l_orderkey, l_linenumber
  ) as rn
  from lineitem
) t`,
			readRows: 1,
		},
	}

	for _, workload := range workloads {
		// Warm up once to populate caches and avoid first-run compilation effects.
		{
			rs, err := se.Execute(ctx, workload.query)
			if err != nil {
				b.Fatal(err)
			}
			readResult(ctx, rs[0], workload.readRows)
		}

		b.Run(workload.name+"/StdSortExec", func(b *testing.B) {
			sortexec.SetAQSortEnabled(false)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rs, err := se.Execute(ctx, workload.query)
				if err != nil {
					b.Fatal(err)
				}
				readResult(ctx, rs[0], workload.readRows)
			}
			b.StopTimer()
		})

		b.Run(workload.name+"/AQSortExec", func(b *testing.B) {
			sortexec.SetAQSortEnabled(true)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rs, err := se.Execute(ctx, workload.query)
				if err != nil {
					b.Fatal(err)
				}
				readResult(ctx, rs[0], workload.readRows)
			}
			b.StopTimer()
			sortexec.SetAQSortEnabled(false)
		})
	}
}
