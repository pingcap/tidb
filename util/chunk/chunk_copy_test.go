package chunk

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

var (
	numRows = 1024
)

func TestCopyFieldByField(t *testing.T) {
	it1, row, dst := prepareChks()

	dst.Reset()
	for lhs := it1.Begin(); lhs != it1.End(); lhs = it1.Next() {
		dst.AppendRow(lhs)
		dst.AppendPartialRow(lhs.Len(), row)
	}
	if !checkDstChk(dst) {
		t.Fail()
	}
}

func TestCopyShadow(t *testing.T) {
	it1, row, mutRow := prepareChksForShadowCopy()

	rowIdx := 0
	for lhs := it1.Begin(); lhs != it1.End(); lhs = it1.Next() {
		mutRow.ShallowCopyPartialRow(0, lhs)
		mutRow.ShallowCopyPartialRow(lhs.Len(), row)

		if !checkDstChkRow(mutRow.ToRow(), rowIdx) {
			t.Fail()
		}
		rowIdx++
	}
}

func TestCopyBatchColumnByColumn(t *testing.T) {
	it1, row, dst := prepareChks()

	dst.Reset()
	rowsCap := 1024
	rows := make([]Row, 0, rowsCap)
	for lhs := it1.Begin(); lhs != it1.End(); lhs = it1.Next() {
		rows = append(rows, lhs)
		if len(rows) == cap(rows) {
			BatchCopyJoinRowToChunk(true, rows, row, dst)
			rows = rows[:0]
		}
	}
	BatchCopyJoinRowToChunk(true, rows, row, dst)
	if !checkDstChk(dst) {
		t.Fail()
	}
}

func BenchmarkCopyFieldByField(b *testing.B) {
	b.ReportAllocs()
	it1, row, dst := prepareChks()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst.Reset()
		for lhs := it1.Begin(); lhs != it1.End(); lhs = it1.Next() {
			dst.AppendRow(lhs)
			dst.AppendPartialRow(lhs.Len(), row)
		}
	}
}

func BenchmarkCopyShadow(b *testing.B) {
	b.ReportAllocs()
	it1, row, mutRow := prepareChksForShadowCopy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lhs := it1.Begin()
		for ; lhs != it1.End(); lhs = it1.Next() {
			mutRow.ShallowCopyPartialRow(0, lhs)
			mutRow.ShallowCopyPartialRow(lhs.Len(), row)
		}
	}
}

func BenchmarkCopyBatchColumnByColumn(b *testing.B) {
	b.ReportAllocs()
	it1, row, dst := prepareChks()

	b.ResetTimer()
	rowsCap := 1024
	rows := make([]Row, 0, rowsCap)
	for i := 0; i < b.N; i++ {
		dst.Reset()
		for lhs := it1.Begin(); lhs != it1.End(); lhs = it1.Next() {
			rows = append(rows, lhs)
			if len(rows) == cap(rows) {
				BatchCopyJoinRowToChunk(true, rows, row, dst)
				rows = rows[:0]
			}

		}
		BatchCopyJoinRowToChunk(true, rows, row, dst)
	}
}

func getChunk() *Chunk {
	chk := newChunkWithInitCap(numRows, 8, 8, 0, 0)
	for i := 0; i < numRows; i++ {
		//chk.AppendNull(0)
		chk.AppendInt64(0, int64(i))
		if i%3 == 0 {
			chk.AppendNull(1)
		} else {
			chk.AppendInt64(1, int64(i))
		}

		chk.AppendString(2, fmt.Sprintf("abcd-%d", i))
		chk.AppendBytes(3, []byte(fmt.Sprintf("01234567890zxcvbnmqwer-%d", i)))
	}
	return chk
}

func prepareChks() (it1 Iterator, row Row, dst *Chunk) {
	chk1 := getChunk()
	row = chk1.GetRow(0)
	it1 = NewIterator4Chunk(chk1)
	it1.Begin()
	dst = newChunkWithInitCap(numRows, 8, 8, 0, 0, 8, 8, 0, 0)
	return it1, row, dst
}

func prepareChksForShadowCopy() (it1 Iterator, row Row, mutRow MutRow) {
	chk1 := getChunk()
	row = chk1.GetRow(0)
	it1 = NewIterator4Chunk(chk1)
	it1.Begin()

	colTypes := make([]*types.FieldType, 0, 8)
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})

	mutRow = MutRowFromTypes(colTypes)
	return it1, row, mutRow
}

func checkDstChk(dst *Chunk) bool {
	for i := 0; i < 8; i++ {
		if dst.columns[i].length != numRows {
			return false
		}
	}
	for j := 0; j < numRows; j++ {
		row := dst.GetRow(j)
		if !checkDstChkRow(row, j) {
			return false
		}
	}
	return true
}
func checkDstChkRow(row Row, j int) bool {
	if row.GetInt64(0) != int64(j) {
		return false
	}
	if j%3 == 0 {
		if !row.IsNull(1) {
			return false
		}
	} else {
		if row.GetInt64(1) != int64(j) {
			return false
		}
	}
	if row.GetString(2) != fmt.Sprintf("abcd-%d", j) {
		return false
	}
	if string(row.GetBytes(3)) != fmt.Sprintf("01234567890zxcvbnmqwer-%d", j) {
		return false
	}
	if row.GetInt64(4) != 0 {
		return false
	}
	if !row.IsNull(5) {
		return false
	}
	if row.GetString(6) != fmt.Sprintf("abcd-%d", 0) {
		return false
	}
	if string(row.GetBytes(7)) != fmt.Sprintf("01234567890zxcvbnmqwer-%d", 0) {
		return false
	}
	return true
}
