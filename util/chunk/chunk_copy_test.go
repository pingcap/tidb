package chunk

import (
	"fmt"
	"testing"
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
	it1, row, dst := prepareChks()

	dst.Reset()

	lhs := it1.Begin()

	for _, c := range dst.columns {
		c.nullBitmap = append(c.nullBitmap, 0)
		c.offsets = append(c.offsets, 0)
		c.length = 1
	}
	rowIdx := 0
	for ; lhs != it1.End(); lhs = it1.Next() {
		ShadowCopyPartialRow(0, lhs, dst)
		ShadowCopyPartialRow(lhs.Len(), row, dst)

		if !checkDstChkRow(dst.GetRow(0), rowIdx) {
			t.Fail()
		}
		rowIdx++
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
	it1, row, dst := prepareChks()

	b.ResetTimer()
	for _, c := range dst.columns {
		c.nullBitmap = append(c.nullBitmap, 0)
		c.offsets = append(c.offsets, 0)
		c.length = 1
	}

	for i := 0; i < b.N; i++ {
		lhs := it1.Begin()
		for ; lhs != it1.End(); lhs = it1.Next() {
			ShadowCopyPartialRow(0, lhs, dst)
			ShadowCopyPartialRow(lhs.Len(), row, dst)
		}
	}
}

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
