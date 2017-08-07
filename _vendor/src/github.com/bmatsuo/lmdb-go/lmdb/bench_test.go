package lmdb

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"sync/atomic"
	"testing"
)

func BenchmarkEnv_ReaderList(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	var txns []*Txn
	defer func() {
		for i, txn := range txns {
			if txn != nil {
				txn.Abort()
				txns[i] = nil
			}
		}
	}()

	const numreaders = 100
	for i := 0; i < numreaders; i++ {
		txn, err := env.BeginTxn(nil, Readonly)
		if err != nil {
			b.Error(err)
			return
		}
		txns = append(txns, txn)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list := new(readerList)
		err := env.ReaderList(list.Next)
		if err != nil {
			b.Error(err)
			return
		}
		if list.Len() != numreaders+1 {
			b.Errorf("reader list length: %v", list.Len())
		}
	}
}

type readerList struct {
	ln []string
}

func (r *readerList) Len() int {
	return len(r.ln)
}

func (r *readerList) Next(ln string) error {
	r.ln = append(r.ln, ln)
	return nil
}

// repeatedly put (overwrite) keys.
func BenchmarkTxn_Put(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			k := ps[rand.Intn(len(ps)/2)*2]
			v := makeBenchDBVal(&rc)
			err := txn.Put(dbi, k, v, 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}
}

// repeatedly put (overwrite) keys using the PutReserve method.
func BenchmarkTxn_PutReserve(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			k := ps[rand.Intn(len(ps)/2)*2]
			v := makeBenchDBVal(&rc)
			buf, err := txn.PutReserve(dbi, k, len(v), 0)
			if err != nil {
				return err
			}
			copy(buf, v)
		}
		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}
}

// repeatedly put (overwrite) keys using the PutReserve method on an
// environment with WriteMap.
func BenchmarkTxn_PutReserve_writemap(b *testing.B) {
	initRandSource(b)
	env := setupFlags(b, WriteMap)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			k := ps[rand.Intn(len(ps)/2)*2]
			v := makeBenchDBVal(&rc)
			buf, err := txn.PutReserve(dbi, k, len(v), 0)
			if err != nil {
				return err
			}
			copy(buf, v)
		}
		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}
}

// repeatedly put (overwrite) keys.
func BenchmarkTxn_Put_writemap(b *testing.B) {
	initRandSource(b)
	env := setupFlags(b, WriteMap)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	var ps [][]byte

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			k := ps[rand.Intn(len(ps)/2)*2]
			v := makeBenchDBVal(&rc)
			err := txn.Put(dbi, k, v, 0)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		b.Error(err)
	}
}

// repeatedly get random keys.
func BenchmarkTxn_Get_ro(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			_, err := txn.Get(dbi, ps[rand.Intn(len(ps))])
			if IsNotFound(err) {
				continue
			}
			if err != nil {
				b.Fatalf("error getting data: %v", err)
			}
		}

		return nil
	})
	if err != nil {
		b.Error(err)
	}
}

// like BenchmarkTxnGetReadonly but txn.RawRead is set to true.
func BenchmarkTxn_Get_raw_ro(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		txn.RawRead = true
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			_, err := txn.Get(dbi, ps[rand.Intn(len(ps))])
			if IsNotFound(err) {
				continue
			}
			if err != nil {
				b.Fatalf("error getting data: %v", err)
			}
		}
		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}
}

func BenchmarkGet_1_alloc_rw_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			err = benchmarkGetBatch(txn, dbi, i, 1, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_1_alloc_rw_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			txn.RawRead = true

			err = benchmarkGetBatch(txn, dbi, i, 1, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_1_alloc_ro_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			err = benchmarkGetBatch(txn, dbi, i, 1, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_1_alloc_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			txn.RawRead = true

			err = benchmarkGetBatch(txn, dbi, i, 1, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_1_renew_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	defer txn.Abort()

	// We can get by only setting RawRead one time because Reset/Renew will not
	// alter its value.
	txn.RawRead = true

	txn.Reset()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}

		err = benchmarkGetBatch(txn, dbi, i, 1, recordSet.Len())
		if err != nil && !IsNotFound(err) {
			b.Error(err)
			return
		}

		txn.Reset()
	}
}

func BenchmarkGet_5_alloc_rw_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			err = benchmarkGetBatch(txn, dbi, i, 5, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_5_alloc_rw_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			txn.RawRead = true

			err = benchmarkGetBatch(txn, dbi, i, 5, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_5_alloc_ro_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			err = benchmarkGetBatch(txn, dbi, i, 5, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_5_alloc_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			txn.RawRead = true

			err = benchmarkGetBatch(txn, dbi, i, 5, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_5_renew_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	defer txn.Abort()

	// We can get by only setting RawRead one time because Reset/Renew will not
	// alter its value.
	txn.RawRead = true

	txn.Reset()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}

		err = benchmarkGetBatch(txn, dbi, i, 5, recordSet.Len())
		if err != nil && !IsNotFound(err) {
			b.Error(err)
			return
		}

		txn.Reset()
	}
}

func BenchmarkGet_25_alloc_rw_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			err = benchmarkGetBatch(txn, dbi, i, 25, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_25_alloc_rw_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			txn.RawRead = true

			err = benchmarkGetBatch(txn, dbi, i, 25, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_25_alloc_ro_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			err = benchmarkGetBatch(txn, dbi, i, 25, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_25_alloc_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			txn.RawRead = true

			err = benchmarkGetBatch(txn, dbi, i, 25, recordSet.Len())
			if err != nil && !IsNotFound(err) {
				return err
			}

			return nil
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkGet_25_renew_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	recordSet := testRecordSetSized(benchmarkScanDBSize)
	if !populateDBI(b, env, dbi, recordSet) {
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	defer txn.Abort()

	// We can get by only setting RawRead one time because Reset/Renew will not
	// alter its value.
	txn.RawRead = true

	txn.Reset()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}

		err = benchmarkGetBatch(txn, dbi, i, 25, recordSet.Len())
		if err != nil && !IsNotFound(err) {
			b.Error(err)
			return
		}

		txn.Reset()
	}
}

func benchmarkGetBatch(txn *Txn, dbi DBI, i, batch, n int) error {
	for j := 0; j < batch; j++ {
		_, err := txn.Get(dbi, benchmarkGetKey(i, j, batch, n))
		if err != nil && !IsNotFound(err) {
			return err
		}
	}

	return nil
}

func benchmarkGetKey(i, j, batch, n int) []byte {
	var k [8]byte
	u64 := uint64(((batch*i)+j)*7) % uint64(n)
	binary.BigEndian.PutUint64(k[:], u64)
	return k[:]
}

const benchmarkScanDBSize = 1 << 20

func BenchmarkScan_10_alloc_rw_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 10)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_10_alloc_rw_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			txn.RawRead = true

			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 10)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_10_alloc_ro_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 10)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_10_alloc_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			txn.RawRead = true

			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 10)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_10_renew_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	defer txn.Abort()

	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		b.Error(err)
		return
	}
	defer cur.Close()

	// We can get by only setting RawRead one time because Reset/Renew will not
	// alter its value.
	txn.RawRead = true

	txn.Reset()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}

		err = cur.Renew(txn)
		if err != nil {
			b.Error(err)
			return
		}

		err = benchmarkScanDBI(cur, dbi, 10)
		if err != nil {
			b.Error(err)
			return
		}

		txn.Reset()
	}
}

func BenchmarkScan_100_alloc_rw_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 100)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_100_alloc_rw_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			txn.RawRead = true

			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 100)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_100_alloc_ro_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 100)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_100_alloc_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			txn.RawRead = true

			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 100)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_100_renew_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	defer txn.Abort()

	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		b.Error(err)
		return
	}
	defer cur.Close()

	// We can get by only setting RawRead one time because Reset/Renew will not
	// alter its value.
	txn.RawRead = true

	txn.Reset()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}

		err = cur.Renew(txn)
		if err != nil {
			b.Error(err)
			return
		}

		err = benchmarkScanDBI(cur, dbi, 100)
		if err != nil {
			b.Error(err)
			return
		}

		txn.Reset()
	}
}

func BenchmarkScan_1000_alloc_rw_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 1000)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_1000_alloc_rw_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			txn.RawRead = true

			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 1000)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_1000_alloc_ro_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 1000)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_1000_alloc_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			txn.RawRead = true

			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 1000)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_1000_renew_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	defer txn.Abort()

	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		b.Error(err)
		return
	}
	defer cur.Close()

	// We can get by only setting RawRead one time because Reset/Renew will not
	// alter its value.
	txn.RawRead = true

	txn.Reset()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}

		err = cur.Renew(txn)
		if err != nil {
			b.Error(err)
			return
		}

		err = benchmarkScanDBI(cur, dbi, 1000)
		if err != nil {
			b.Error(err)
			return
		}

		txn.Reset()
	}
}

func BenchmarkScan_10000_alloc_rw_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 10000)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_10000_alloc_rw_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.Update(func(txn *Txn) (err error) {
			txn.RawRead = true

			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 10000)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

// repeatedly scan all the values in a database.
func BenchmarkScan_10000_alloc_ro_copy(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 10000)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

// like BenchmarkCursoreScanReadonly but txn.RawRead is set to true.
func BenchmarkScan_10000_alloc_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := env.View(func(txn *Txn) (err error) {
			txn.RawRead = true

			cur, err := txn.OpenCursor(dbi)
			if err != nil {
				return err
			}
			defer cur.Close()

			err = benchmarkScanDBI(cur, dbi, 10000)
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkScan_10000_renew_ro_raw(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	if !populateDBI(b, env, dbi, testRecordSetSized(benchmarkScanDBSize)) {
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	defer txn.Abort()

	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		b.Error(err)
		return
	}
	defer cur.Close()

	// We can get by only setting RawRead one time because Reset/Renew will not
	// alter its value.
	txn.RawRead = true

	txn.Reset()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}

		err = cur.Renew(txn)
		if err != nil {
			b.Error(err)
			return
		}

		err = benchmarkScanDBI(cur, dbi, 10000)
		if err != nil {
			b.Error(err)
			return
		}

		txn.Reset()
	}
}

// populateBenchmarkDB fills env with data.
//
// populateBenchmarkDB calls env.SetMapSize and must not be called concurrent
// with other transactions.
func populateBenchmarkDB(env *Env, dbi DBI, rc *randSourceCursor) ([][]byte, error) {
	var ps [][]byte

	err := env.SetMapSize(benchDBMapSize)
	if err != nil {
		return nil, err
	}

	err = env.Update(func(txn *Txn) (err error) {
		for i := 0; i < benchDBNumKeys; i++ {
			k := makeBenchDBKey(rc)
			v := makeBenchDBVal(rc)
			err := txn.Put(dbi, k, v, 0)
			ps = append(ps, k, v)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ps, nil
}

func benchmarkScanDBI(cur *Cursor, dbi DBI, n int) error {
	for i := 0; n < 0 || i < n; i++ {
		_, _, err := cur.Get(nil, nil, Next)
		if IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func openBenchDBI(b *testing.B, env *Env) DBI {
	var dbi DBI
	err := env.Update(func(txn *Txn) (err error) {
		dbi, err = txn.OpenDBI("benchmark", Create)
		return err
	})
	if err != nil {
		b.Errorf("unable to open benchmark database")
	}
	return dbi
}

func randBytes(n int) []byte {
	p := make([]byte, n)
	crand.Read(p)
	return p
}

func bMust(b *testing.B, err error, action string) {
	if err != nil {
		b.Fatalf("error %s: %v", action, err)
	}
}

const randSourceSize = 10 << 20  // size of the 'entropy pool' for random byte generation.
const benchDBMapSize = 100 << 20 // size of a benchmark db memory map
const benchDBNumKeys = 1 << 12   // number of keys to store in benchmark databases
const benchDBMaxKeyLen = 30      // maximum length for database keys (size is limited by MDB)
const benchDBMaxValLen = 4096    // maximum lengh for database values

func makeBenchDBKey(c *randSourceCursor) []byte {
	return c.NBytes(rand.Intn(benchDBMaxKeyLen) + 1)
}

func makeBenchDBVal(c *randSourceCursor) []byte {
	return c.NBytes(rand.Intn(benchDBMaxValLen) + 1)
}

// holds a bunch of random bytes so repeated generation of 'random' slices is
// cheap.  acts as a ring which can be read from (although doesn't implement io.Reader).
var _initRand int32
var randSource [randSourceSize]byte

func initRandSource(b *testing.B) {
	if atomic.AddInt32(&_initRand, 1) > 1 {
		return
	}
	b.Logf("initializing random source data")
	n, err := crand.Read(randSource[:])
	bMust(b, err, "initializing random source")
	if n < len(randSource) {
		b.Fatalf("unable to read enough random source data %d", n)
	}
}

// acts as a simple byte slice generator.
type randSourceCursor int

func newRandSourceCursor() randSourceCursor {
	i := rand.Intn(randSourceSize)
	return randSourceCursor(i)
}

func (c *randSourceCursor) NBytes(n int) []byte {
	i := int(*c)
	if n >= randSourceSize {
		panic("rand size too big")
	}
	*c = (*c + randSourceCursor(n)) % randSourceSize
	_n := i + n - randSourceSize
	if _n > 0 {
		p := make([]byte, n)
		m := copy(p, randSource[i:])
		copy(p[m:], randSource[:])
		return p
	}
	return randSource[i : i+n]
}

func populateDBIString(t testing.TB, env *Env, dbi DBI, records []testRecordString) (ok bool) {
	return populateDBI(t, env, dbi, testRecordSetString(records))
}

func populateDBIBytes(t testing.TB, env *Env, dbi DBI, records []testRecordBytes) (ok bool) {
	return populateDBI(t, env, dbi, testRecordSetBytes(records))
}

func populateDBI(t testing.TB, env *Env, dbi DBI, records testRecordSet) (ok bool) {
	err := env.SetMapSize(benchDBMapSize)
	if err != nil {
		t.Error(err)
		return false
	}

	// TODO:
	// Batch transactions that become too large.  I'm not sure where that is
	// anymore.
	n := records.Len()
	i := 0

	err = env.Update(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for ; i < n; i++ {
			err = writeTestRecord(cur, records.TestRecord(i))
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Errorf("populateDBI: %v", err)
		return false
	}

	return true
}

func writeTestRecord(cur *Cursor, r testRecord) error {
	return cur.Put(r.Key(), r.Data(), 0)
}

func testRecordSetSized(numBytes int64) testRecordSet {
	const recordSize = 16
	numRecord := numBytes / recordSize
	return &testRecordGen{
		n: int(numRecord),
		fn: func(i int) testRecord {
			var k, d [8]byte
			k64 := uint64(i)
			d64 := (^k64) + 1
			binary.BigEndian.PutUint64(k[:], k64)
			binary.BigEndian.PutUint64(d[:], d64)
			return testRecordBytes{k[:], d[:]}
		},
	}
}

type testRecordSet interface {
	Len() int
	TestRecord(i int) testRecord
}

type testRecordSetString []testRecordString

func (s testRecordSetString) Len() int                    { return len(s) }
func (s testRecordSetString) TestRecord(i int) testRecord { return s[i] }

type testRecordSetBytes []testRecordBytes

func (s testRecordSetBytes) Len() int                    { return len(s) }
func (s testRecordSetBytes) TestRecord(i int) testRecord { return s[i] }

type testRecordGen struct {
	n  int
	fn testRecordFn
}

func (g *testRecordGen) Len() int                    { return g.n }
func (g *testRecordGen) TestRecord(i int) testRecord { return g.fn(i) }

type testRecordFn func(i int) testRecord

type testRecord interface {
	Key() []byte
	Data() []byte
}

type testRecordBytes [2][]byte

func (r testRecordBytes) Key() []byte  { return r[0] }
func (r testRecordBytes) Data() []byte { return r[1] }

type testRecordString [2][]byte

func (r testRecordString) Key() []byte  { return []byte(r[0]) }
func (r testRecordString) Data() []byte { return []byte(r[1]) }
