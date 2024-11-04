package instanceplancache

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

const (
	tableSysbench = `CREATE TABLE sbtest1 (
    id INT NOT NULL AUTO_INCREMENT,
    k INT NOT NULL DEFAULT '0',
    c CHAR(120) NOT NULL DEFAULT '',
    pad CHAR(60) NOT NULL DEFAULT '',
    PRIMARY KEY (id),
    KEY k_1 (k));`
)

func prepareSysbenchData(tk *testkit.TestKit, nRows int) {
	tk.MustExec(`use test`)
	tk.MustExec(tableSysbench)
	batch := 100
	nBatch := (nRows + batch - 1) / batch

	randK := func() int {
		return 1000000 + rand.Intn(8000000)
	}
	randC := func() string {
		return strings.Repeat("6848793219-", 10)
	}
	randPad := func() string {
		return strings.Repeat("2219520708-", 5)
	}
	for b := 0; b < nBatch; b++ {
		vals := make([]string, 0, batch)
		for i := 0; i < batch; i++ {
			vals = append(vals, fmt.Sprintf("(%d, '%s', '%s')", randK(), randC(), randPad()))
		}
		sql := fmt.Sprintf("insert into sbtest1 (k, c, pad) values %s", strings.Join(vals, ","))
		tk.MustExec(sql)
	}
}

func BenchmarkSysbenchSelectRandomRanges(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	prepareSysbenchData(tk, 1000)

	selectRandomRangesStmt := `prepare st from 'select count(k) from sbtest1 where k between ? and ? or
                               k between ? and ? or k between ? and ? or k between ? and ? or
                               k between ? and ? or k between ? and ? or k between ? and ? or
                               k between ? and ? or k between ? and ? or k between ? and ?'`
	params := make([]string, 0, 20)
	execParms := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		params = append(params, fmt.Sprintf("@k%d=%d", i, 1000000+rand.Intn(8000000)))
		execParms = append(execParms, fmt.Sprintf("@k%d", i))
	}
	setParamStmt := fmt.Sprintf("set %s", strings.Join(params, ","))
	execStmt := fmt.Sprintf("execute st using %s", strings.Join(execParms, ","))
	tk.MustExec(selectRandomRangesStmt)
	tk.MustExec(setParamStmt)
	tk.MustQueryWithContext(context.Background(), execStmt) // no error

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustQueryWithContext(context.Background(), execStmt) // no error
	}
	b.StopTimer()
}

func BenchmarkSysbenchSelectRandomRangesX(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	prepareSysbenchData(tk, 1000)

	selectRandomRangesStmt := `select count(k) from sbtest1 where k between ? and ? or
                               k between ? and ? or k between ? and ? or k between ? and ? or
                               k between ? and ? or k between ? and ? or k between ? and ? or
                               k between ? and ? or k between ? and ? or k between ? and ?`
	se := tk.Session()
	stmtID, _, _, err := se.PrepareStmt(selectRandomRangesStmt)
	if err != nil {
		b.Fatal(err)
	}

	ks := make([]any, 0, 20)
	for i := 0; i < 20; i++ {
		ks = append(ks, 1000000+rand.Intn(8000000))
	}
	params := expression.Args2Expressions4Test(ks...)
	alloc := chunk.NewAllocator()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.ExecutePreparedStmt(context.Background(), stmtID, params)
		if err != nil {
			b.Fatal(err)
		}
		_, err = drainRecordSet(context.Background(), rs, alloc)
		if err != nil {
			b.Fatal(err)
		}
		alloc.Reset()
	}
	b.StopTimer()
}

func drainRecordSet(ctx context.Context, rs sqlexec.RecordSet, alloc chunk.Allocator) ([]chunk.Row, error) {
	var rows []chunk.Row
	var req *chunk.Chunk
	req = rs.NewChunk(alloc)
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, 1024)
	}
}
