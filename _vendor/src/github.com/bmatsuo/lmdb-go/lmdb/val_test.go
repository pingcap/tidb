package lmdb

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMultiVal(t *testing.T) {
	data := []byte("abcdef")
	m := WrapMulti(data, 2)
	vals := m.Vals()
	if !reflect.DeepEqual(vals, [][]byte{{'a', 'b'}, {'c', 'd'}, {'e', 'f'}}) {
		t.Errorf("unexpected vals: %q", vals)
	}
	size := m.Size()
	if size != 6 {
		t.Errorf("unexpected size: %v (!= %v)", size, 6)
	}
	length := m.Len()
	if length != 3 {
		t.Errorf("unexpected length: %v (!= %v)", length, 3)
	}
	stride := m.Stride()
	if stride != 2 {
		t.Errorf("unexpected stride: %v (!= %v)", stride, 2)
	}
	page := m.Page()
	if !bytes.Equal(page, data) {
		t.Errorf("unexpected page: %v (!= %v)", page, data)
	}
}

func TestMultiVal_panic(t *testing.T) {
	var p bool
	defer func() {
		if e := recover(); e != nil {
			p = true
		}
		if !p {
			t.Errorf("expected a panic")
		}
	}()
	WrapMulti([]byte("123"), 2)
}

func TestValBytes(t *testing.T) {
	ptr, n := valBytes(nil)
	if len(ptr) == 0 {
		t.Errorf("unexpected unaddressable slice")
	}
	if n != 0 {
		t.Errorf("unexpected length: %d (expected 0)", n)
	}

	b := []byte("abc")
	ptr, n = valBytes(b)
	if len(ptr) == 0 {
		t.Errorf("unexpected unaddressable slice")
	}
	if n != 3 {
		t.Errorf("unexpected length: %d (expected %d)", n, len(b))
	}
}

func TestVal(t *testing.T) {
	orig := []byte("hey hey")
	val := wrapVal(orig)

	p := getBytes(val)
	if !bytes.Equal(p, orig) {
		t.Errorf("getBytes() not the same as original data: %q", p)
	}
	if &p[0] != &orig[0] {
		t.Errorf("getBytes() is not the same slice as original")
	}

	p = getBytesCopy(val)
	if !bytes.Equal(p, orig) {
		t.Errorf("getBytesCopy() not the same as original data: %q", p)
	}
	if &p[0] == &orig[0] {
		t.Errorf("getBytesCopy() overlaps with orignal slice")
	}
}
