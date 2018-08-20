package chunk

import (
	"testing"
)

var (
	numRows = 1024
)

func newChunkWithInitCap(cap int, elemLen ...int) *Chunk {
	chk := &Chunk{}
	for _, l := range elemLen {
		if l > 0 {
			chk.addFixedLenColumn(l, cap)
		} else {
			chk.addVarLenColumn(cap)
		}
	}
	return chk
}

func getChunk() *Chunk {
	chk := newChunkWithInitCap(numRows, 8, 8, 0, 0)
	for i := 0; i < numRows; i++ {
		//chk.AppendNull(0)
		chk.AppendInt64(0, int64(i))
		chk.AppendInt64(1, 1)
		chk.AppendString(2, "abcd")
		chk.AppendBytes(3, []byte("01234567890zxcvbnmqwer"))
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

func checkDstChk(dst *Chunk) bool {
	for i := 0; i < 8; i++ {
		if dst.columns[i].length != numRows {
			return false
		}
	}
	for j := 0; j < numRows; j++ {
		row := dst.GetRow(j)
		if row.GetInt64(0) != int64(j) {
			return false
		}
		if row.GetInt64(1) != 1 {
			return false
		}
		if row.GetString(2) != "abcd" {
			return false
		}
		if string(row.GetBytes(3)) != "01234567890zxcvbnmqwer" {
			return false
		}

		if row.GetInt64(4) != 0 {
			return false
		}
		if row.GetInt64(5) != 1 {
			return false
		}
		if row.GetString(6) != "abcd" {
			return false
		}
		if string(row.GetBytes(7)) != "01234567890zxcvbnmqwer" {
			return false
		}
	}
	return true
}

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

func TestCopyColumnByColumn(t *testing.T) {
	it1, row, dst := prepareChks()

	dst.Reset()
	for it1.Begin(); it1.Current() != it1.End(); {
		dst.AppendRightMultiRows(it1, row, 128)
	}
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
		checkDstChk(dst)
	}
}

func BenchmarkCopyColumnByColumn(b *testing.B) {
	b.ReportAllocs()
	it1, row, dst := prepareChks()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst.Reset()
		for it1.Begin(); it1.Current() != it1.End(); {
			dst.AppendRightMultiRows(it1, row, 128)
		}
		checkDstChk(dst)
	}
}
