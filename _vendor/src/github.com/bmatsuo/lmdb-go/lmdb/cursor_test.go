package lmdb

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"testing"
)

func TestCursor_Txn(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}

		_txn := cur.Txn()
		if _txn == nil {
			t.Errorf("nil cursor txn")
		}

		cur.Close()

		_txn = cur.Txn()
		if _txn != nil {
			t.Errorf("non-nil cursor txn")
		}

		return err
	})
	if err != nil {
		t.Error(err)
		return
	}
}

func TestCursor_DBI(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	err := env.Update(func(txn *Txn) (err error) {
		db, err := txn.OpenDBI("db", Create)
		if err != nil {
			return err
		}
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		dbcur := cur.DBI()
		if dbcur != db {
			cur.Close()
			return fmt.Errorf("unequal db: %v != %v", dbcur, db)
		}
		cur.Close()
		dbcur = cur.DBI()
		if dbcur == db {
			return fmt.Errorf("db: %v", dbcur)
		}
		if dbcur != ^DBI(0) {
			return fmt.Errorf("db: %v", dbcur)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestCursor_Close(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	db, err := txn.OpenDBI("testing", Create)
	if err != nil {
		t.Fatal(err)
	}

	cur, err := txn.OpenCursor(db)
	if err != nil {
		t.Fatal(err)
	}
	cur.Close()
	cur.Close()
	err = cur.Put([]byte("closedput"), []byte("shouldfail"), 0)
	if err == nil {
		t.Fatalf("expected error: put on closed cursor")
	}
}

func TestCursor_bytesBuffer(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	db, err := openRoot(env, 0)
	if err != nil {
		t.Error(err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		defer cur.Close()
		k := new(bytes.Buffer)
		k.WriteString("hello")
		v := new(bytes.Buffer)
		v.WriteString("world")
		return cur.Put(k.Bytes(), v.Bytes(), 0)
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		defer cur.Close()
		k := new(bytes.Buffer)
		k.WriteString("hello")
		_k, v, err := cur.Get(k.Bytes(), nil, SetKey)
		if err != nil {
			return err
		}
		if !bytes.Equal(_k, k.Bytes()) {
			return fmt.Errorf("unexpected key: %q", _k)
		}
		if !bytes.Equal(v, []byte("world")) {
			return fmt.Errorf("unexpected value: %q", v)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
}

func TestCursor_PutReserve(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	key := "reservekey"
	val := "reserveval"
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.CreateDBI("testing")
		if err != nil {
			return err
		}

		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		defer cur.Close()

		p, err := cur.PutReserve([]byte(key), len(val), 0)
		if err != nil {
			return err
		}
		copy(p, val)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = env.View(func(txn *Txn) (err error) {
		dbval, err := txn.Get(db, []byte(key))
		if err != nil {
			return err
		}
		if !bytes.Equal(dbval, []byte(val)) {
			return fmt.Errorf("unexpected val %q != %q", dbval, val)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCursor_Get_KV(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var dbi DBI
	err := env.Update(func(txn *Txn) (err error) {
		dbi, err = txn.OpenDBI("testdb", Create|DupSort)
		return err
	})
	if err != nil {
		t.Errorf("%s", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		put := func(k, v []byte) {
			if err == nil {
				err = txn.Put(dbi, k, v, 0)
			}
		}
		put([]byte("key"), []byte("1"))
		put([]byte("key"), []byte("2"))
		put([]byte("key"), []byte("3"))
		return err
	})
	if err != nil {
		t.Errorf("%s", err)
	}

	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		k, v, err := cur.Get([]byte("key"), []byte("0"), GetBothRange)
		if err != nil {
			return err
		}
		if string(k) != "key" {
			t.Errorf("unexpected key: %q (not %q)", k, "key")
		}
		if string(v) != "1" {
			t.Errorf("unexpected value: %q (not %q)", k, "1")
		}

		_, _, err = cur.Get([]byte("key"), []byte("1"), GetBoth)
		return err
	})
	if err != nil {
		t.Errorf("%s", err)
	}
}

func TestCursor_Get_op_Set_bytesBuffer(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var dbi DBI
	err := env.Update(func(txn *Txn) (err error) {
		dbi, err = txn.OpenDBI("testdb", Create|DupSort)
		return err
	})
	if err != nil {
		t.Errorf("%s", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		put := func(k, v []byte) {
			if err == nil {
				err = txn.Put(dbi, k, v, 0)
			}
		}
		put([]byte("k1"), []byte("v11"))
		put([]byte("k1"), []byte("v12"))
		put([]byte("k1"), []byte("v13"))
		put([]byte("k2"), []byte("v21"))
		put([]byte("k2"), []byte("v22"))
		put([]byte("k2"), []byte("v23"))
		return err
	})
	if err != nil {
		t.Errorf("%s", err)
	}

	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		// Create bytes.Buffer values containing a amount of bytes.  Byte
		// slices returned from buf.Bytes() have a history of tricking the cgo
		// argument checker.
		var kbuf bytes.Buffer
		kbuf.WriteString("k2")

		k, _, err := cur.Get(kbuf.Bytes(), nil, Set)
		if err != nil {
			return err
		}
		if string(k) != kbuf.String() {
			t.Errorf("unexpected key: %q (not %q)", k, kbuf.String())
		}

		// No guarantee is made about the return value of mdb_cursor_get when
		// MDB_SET is the op, so its value is not checked as part of this test.
		// That said, it is important that Cursor.Get not panic if given a
		// short buffer as an input value for a Set op (despite that not really
		// having any significance)
		var vbuf bytes.Buffer
		vbuf.WriteString("v22")

		k, _, err = cur.Get(kbuf.Bytes(), vbuf.Bytes(), Set)
		if err != nil {
			return err
		}
		if string(k) != kbuf.String() {
			t.Errorf("unexpected key: %q (not %q)", k, kbuf.String())
		}

		return nil
	})
	if err != nil {
		t.Errorf("%s", err)
	}
}

func TestCursor_Get_DupFixed(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	const datasize = 16
	pagesize := os.Getpagesize()
	numitems := (2 * pagesize / datasize) + 1

	var dbi DBI
	key := []byte("key")
	err := env.Update(func(txn *Txn) (err error) {
		dbi, err = txn.OpenRoot(DupSort | DupFixed)
		if err != nil {
			return err
		}

		for i := int64(0); i < int64(numitems); i++ {
			err = txn.Put(dbi, key, []byte(fmt.Sprintf("%016x", i)), 0)
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}

	var items [][]byte
	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, first, err := cur.Get(nil, nil, NextNoDup)
			if IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			if string(k) != string(key) {
				return fmt.Errorf("key: %s", k)
			}

			stride := len(first)

			for {
				_, v, err := cur.Get(nil, nil, NextMultiple)
				if IsNotFound(err) {
					break
				}
				if err != nil {
					return err
				}

				multi := WrapMulti(v, stride)
				for i := 0; i < multi.Len(); i++ {
					items = append(items, multi.Val(i))
				}
			}
		}
	})
	if err != nil {
		t.Error(err)
	}

	if len(items) != numitems {
		t.Errorf("unexpected number of items: %d (!= %d)", len(items), numitems)
	}

	for i, b := range items {
		expect := fmt.Sprintf("%016x", i)
		if string(b) != expect {
			t.Errorf("unexpected value: %q (!= %q)", b, expect)
		}
	}
}

func TestCursor_Get_reverse(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var dbi DBI
	err := env.Update(func(txn *Txn) (err error) {
		dbi, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		err = txn.Put(dbi, []byte("k0"), []byte("v0"), 0)
		if err != nil {
			return err
		}
		err = txn.Put(dbi, []byte("k1"), []byte("v1"), 0)
		if err != nil {
			return err
		}
		return err
	})
	if err != nil {
		t.Error(err)
	}

	type Item struct{ k, v []byte }
	var items []Item

	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, v, err := cur.Get(nil, nil, Prev)
			if IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			items = append(items, Item{k, v})
		}
	})
	if err != nil {
		t.Error(err)
	}

	expect := []Item{
		{[]byte("k1"), []byte("v1")},
		{[]byte("k0"), []byte("v0")},
	}
	if !reflect.DeepEqual(items, expect) {
		t.Errorf("unexpected items %q (!= %q)", items, expect)
	}
}

func TestCursor_PutMulti(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	key := []byte("k")
	items := [][]byte{
		[]byte("v0"),
		[]byte("v2"),
		[]byte("v1"),
	}
	page := bytes.Join(items, nil)
	stride := 2

	var dbi DBI
	err := env.Update(func(txn *Txn) (err error) {
		dbi, err = txn.OpenRoot(Create | DupSort | DupFixed)
		if err != nil {
			return err
		}

		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		return cur.PutMulti(key, page, stride, 0)
	})
	if err != nil {
		t.Error(err)
	}

	expect := [][]byte{
		[]byte("v0"),
		[]byte("v1"),
		[]byte("v2"),
	}
	var dbitems [][]byte
	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, v, err := cur.Get(nil, nil, Next)
			if IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if string(k) != "k" {
				return fmt.Errorf("key: %q", k)
			}
			dbitems = append(dbitems, v)
		}
	})
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(dbitems, expect) {
		t.Errorf("unexpected items: %q (!= %q)", dbitems, items)
	}
}

func TestCursor_Del(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	type Item struct{ k, v string }
	items := []Item{
		{"k0", "k0"},
		{"k1", "k1"},
		{"k2", "k2"},
	}
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.CreateDBI("testing")
		if err != nil {
			return err
		}

		for _, item := range items {
			err := txn.Put(db, []byte(item.k), []byte(item.v), 0)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}

	err = env.Update(func(txn *Txn) (err error) {
		txn.RawRead = true
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}

		item := items[1]
		k, v, err := cur.Get([]byte(item.k), nil, SetKey)
		if err != nil {
			return err
		}
		if !bytes.Equal(k, []byte(item.k)) {
			return fmt.Errorf("found key %q (!= %q)", k, item.k)
		}
		if !bytes.Equal(v, []byte(item.v)) {
			return fmt.Errorf("found value %q (!= %q)", k, item.v)
		}

		err = cur.Del(0)
		if err != nil {
			return err
		}

		k, v, err = cur.Get(nil, nil, Next)
		if err != nil {
			return fmt.Errorf("post-delete: %v", err)
		}
		item = items[2]
		if err != nil {
			return err
		}
		if !bytes.Equal(k, []byte(item.k)) {
			return fmt.Errorf("found key %q (!= %q)", k, item.k)
		}
		if !bytes.Equal(v, []byte(item.v)) {
			return fmt.Errorf("found value %q (!= %q)", k, item.v)
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}

	var newitems []Item
	err = env.View(func(txn *Txn) (err error) {
		txn.RawRead = true
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		next := func(cur *Cursor) (k, v []byte, err error) { return cur.Get(nil, nil, Next) }
		for k, v, err := next(cur); !IsNotFound(err); k, v, err = next(cur) {
			if err != nil {
				return err
			}
			newitems = append(newitems, Item{string(k), string(v)})
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	expectitems := []Item{
		items[0],
		items[2],
	}
	if !reflect.DeepEqual(newitems, expectitems) {
		t.Errorf("unexpected items %q (!= %q)", newitems, expectitems)
	}
}

// This test verifies the behavior of Cursor.Count when DupSort is provided.
func TestCursor_Count_DupSort(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenDBI("testingdup", Create|DupSort)
		if err != nil {
			return err
		}

		put := func(k, v string) {
			if err != nil {
				return
			}
			err = txn.Put(db, []byte(k), []byte(v), 0)
		}
		put("k", "v0")
		put("k", "v1")

		return err
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		defer cur.Close()

		_, _, err = cur.Get(nil, nil, First)
		if err != nil {
			return err
		}
		numdup, err := cur.Count()
		if err != nil {
			return err
		}

		if numdup != 2 {
			t.Errorf("unexpected count: %d != %d", numdup, 2)
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

// This test verifies the behavior of Cursor.Count when DupSort is not enabled
// on the database.
func TestCursor_Count_noDupSort(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenDBI("testingnodup", Create)
		if err != nil {
			return err
		}

		return txn.Put(db, []byte("k"), []byte("v1"), 0)
	})
	if err != nil {
		t.Error(err)
	}

	// it is an error to call Count if the underlying database does not allow
	// duplicate keys.
	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		defer cur.Close()

		_, _, err = cur.Get(nil, nil, First)
		if err != nil {
			return err
		}
		_, err = cur.Count()
		if err == nil {
			t.Error("expected error")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestCursor_Renew(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(0)
		return err
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		put := func(k, v string) {
			if err == nil {
				err = txn.Put(db, []byte(k), []byte(v), 0)
			}
		}
		put("k1", "v1")
		put("k2", "v2")
		put("k3", "v3")
		return err
	})
	if err != nil {
		t.Error("err")
	}

	var cur *Cursor
	err = env.View(func(txn *Txn) (err error) {
		cur, err = txn.OpenCursor(db)
		if err != nil {
			return err
		}

		k, v, err := cur.Get(nil, nil, Next)
		if err != nil {
			return err
		}
		if string(k) != "k1" {
			return fmt.Errorf("key: %q", k)
		}
		if string(v) != "v1" {
			return fmt.Errorf("val: %q", v)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *Txn) (err error) {
		err = cur.Renew(txn)
		if err != nil {
			return err
		}

		k, v, err := cur.Get(nil, nil, Next)
		if err != nil {
			return err
		}
		if string(k) != "k1" {
			return fmt.Errorf("key: %q", k)
		}
		if string(v) != "v1" {
			return fmt.Errorf("val: %q", v)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkCursor(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	var db DBI
	err := env.View(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()

		for i := 0; i < b.N; i++ {
			cur, err := txn.OpenCursor(db)
			if err != nil {
				return err
			}
			cur.Close()
		}
		return
	})
	if err != nil {
		b.Error(err)
		return
	}
}

func BenchmarkCursor_Renew(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	var cur *Cursor
	err := env.View(func(txn *Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		cur, err = txn.OpenCursor(db)
		return err
	})
	if err != nil {
		b.Error(err)
		return
	}

	env.View(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()

		for i := 0; i < b.N; i++ {
			err = cur.Renew(txn)
			if err != nil {
				return err
			}
		}

		return nil
	})
}
