package chunk

import (
	"fmt"
	"github.com/juju/errors"
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

func checkDstChk(dst *Chunk) error {
	for i := 0; i < 8; i++ {
		if dst.columns[i].length != numRows {
			return errors.Errorf("col-%d length no equal", i)
		}
	}
	for j := 0; j < numRows; j++ {
		row := dst.GetRow(j)
		if err := checkDstChkRow(row, j); err != nil {
			return err
		}
	}
	return nil
}
func checkDstChkRow(row Row, j int) error {
	if row.GetInt64(0) != int64(j) {
		return errors.Errorf("row-%d col-%d expect: %d, but get: %d", j, 0, j, row.GetInt64(0))
	}
	if j%3 == 0 {
		if !row.IsNull(1) {
			return errors.Errorf("row-%d col-%d expect: null, but get: not null", j, 1)
		}
	} else {
		if row.GetInt64(1) != int64(j) {
			return errors.Errorf("row-%d col-%d expect: %d, but get: %d", j, 1, j, row.GetInt64(1))
		}
	}

	if row.GetString(2) != fmt.Sprintf("abcd-%d", j) {
		return errors.Errorf("row-%d col-%d expect: %s, but get: %s", j, 2, fmt.Sprintf("abcd-%d", j), row.GetString(2))
	}
	if string(row.GetBytes(3)) != fmt.Sprintf("01234567890zxcvbnmqwer-%d", j) {
		return errors.Errorf("row-%d col-%d expect: %s, but get: %s", j, 3, fmt.Sprintf("01234567890zxcvbnmqwer-%d", j), string(row.GetBytes(3)))
	}

	if row.GetInt64(4) != 0 {
		return errors.Errorf("row-%d col-%d expect: %d, but get: %d", j, 4, 0, row.GetInt64(0))
	}

	if !row.IsNull(5) {
		return errors.Errorf("row-%d col-%d expect: null, but get: not null", j, 5)
	}
	if row.GetString(6) != fmt.Sprintf("abcd-%d", 0) {
		return errors.Errorf("row-%d col-%d expect: %s, but get: %s", j, 6, fmt.Sprintf("abcd-%d", 0), row.GetString(6))
	}
	if string(row.GetBytes(7)) != fmt.Sprintf("01234567890zxcvbnmqwer-%d", 0) {
		return errors.Errorf("row-%d col-%d expect: %s, but get: %s", j, 7, fmt.Sprintf("01234567890zxcvbnmqwer-%d", 0), string(row.GetBytes(7)))
	}
	return nil
}

func printRow(row Row) {
	fmt.Printf("%d\t%d\t%s\t%s\t%d\t%d\t%s\t%s \n", row.GetInt64(0), row.GetInt64(1), row.GetString(2), string(row.GetBytes(3)),
		row.GetInt64(4), row.GetInt64(5), row.GetString(6), string(row.GetBytes(7)))
}

func TestCopyFieldByField(t *testing.T) {
	it1, row, dst := prepareChks()

	dst.Reset()
	for lhs := it1.Begin(); lhs != it1.End(); lhs = it1.Next() {
		dst.AppendRow(lhs)
		dst.AppendPartialRow(lhs.Len(), row)
	}
	if err := checkDstChk(dst); err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestCopyColumnByColumn(t *testing.T) {
	it1, row, dst := prepareChks()

	dst.Reset()
	for it1.Begin(); it1.Current() != it1.End(); {
		dst.AppendRightMultiRows(it1, row, 128)
	}
	if err := checkDstChk(dst); err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestCopyFieldByFieldOne(t *testing.T) {
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
		appendPartialRow(0, lhs, dst)
		appendPartialRow(lhs.Len(), row, dst)

		if err := checkDstChkRow(dst.GetRow(0), rowIdx); err != nil {
			t.Log(err)
			t.Fail()
		}
		rowIdx++
	}
}

func appendPartialRow(colIdx int, row Row, dst *Chunk) {
	for i, rowCol := range row.c.columns {
		chkCol := dst.columns[colIdx+i]
		if !rowCol.isNull(row.idx) {
			chkCol.nullBitmap[0] = 1
		} else {
			chkCol.nullBitmap[0] = 0
		}

		if rowCol.isFixed() {
			elemLen := len(rowCol.elemBuf)
			offset := row.idx * elemLen
			chkCol.data = rowCol.data[offset : offset+elemLen]
		} else {
			start, end := rowCol.offsets[row.idx], rowCol.offsets[row.idx+1]
			chkCol.data = rowCol.data[start:end]
			chkCol.offsets[1] = int32(len(chkCol.data))
		}
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

func BenchmarkCopyColumnByColumn(b *testing.B) {
	b.ReportAllocs()
	it1, row, dst := prepareChks()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst.Reset()
		for it1.Begin(); it1.Current() != it1.End(); {
			dst.AppendRightMultiRows(it1, row, 128)
		}
	}
}

func BenchmarkCopyFieldByFieldOne(b *testing.B) {
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
			appendPartialRow(0, lhs, dst)
			appendPartialRow(lhs.Len(), row, dst)
		}
	}
}
