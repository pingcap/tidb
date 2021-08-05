// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package filesort

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func nextRow(r *rand.Rand, keySize int, valSize int) (key []types.Datum, val []types.Datum, handle int64) {
	key = make([]types.Datum, keySize)
	for i := range key {
		key[i] = types.NewDatum(r.Int())
	}

	val = make([]types.Datum, valSize)
	for j := range val {
		val[j] = types.NewDatum(r.Int())
	}

	handle = r.Int63()
	return
}

func TestLessThan(t *testing.T) {
	t.Parallel()

	sc := new(stmtctx.StatementContext)

	d0 := types.NewDatum(0)
	d1 := types.NewDatum(1)

	tblOneColumn := []struct {
		Arg1 []types.Datum
		Arg2 []types.Datum
		Arg3 []bool
		Ret  bool
	}{
		{[]types.Datum{d0}, []types.Datum{d0}, []bool{false}, false},
		{[]types.Datum{d0}, []types.Datum{d1}, []bool{false}, true},
		{[]types.Datum{d1}, []types.Datum{d0}, []bool{false}, false},
		{[]types.Datum{d0}, []types.Datum{d0}, []bool{true}, false},
		{[]types.Datum{d0}, []types.Datum{d1}, []bool{true}, false},
		{[]types.Datum{d1}, []types.Datum{d0}, []bool{true}, true},
	}

	for _, T := range tblOneColumn {
		ret, err := lessThan(sc, T.Arg1, T.Arg2, T.Arg3)
		require.NoError(t, err)
		require.Equal(t, T.Ret, ret)
	}

	tblTwoColumns := []struct {
		Arg1 []types.Datum
		Arg2 []types.Datum
		Arg3 []bool
		Ret  bool
	}{
		{[]types.Datum{d0, d0}, []types.Datum{d1, d1}, []bool{false, false}, true},
		{[]types.Datum{d0, d1}, []types.Datum{d1, d1}, []bool{false, false}, true},
		{[]types.Datum{d0, d0}, []types.Datum{d1, d1}, []bool{false, false}, true},
		{[]types.Datum{d0, d0}, []types.Datum{d0, d1}, []bool{false, false}, true},
		{[]types.Datum{d0, d1}, []types.Datum{d0, d1}, []bool{false, false}, false},
		{[]types.Datum{d0, d1}, []types.Datum{d0, d0}, []bool{false, false}, false},
		{[]types.Datum{d1, d0}, []types.Datum{d0, d1}, []bool{false, false}, false},
		{[]types.Datum{d1, d1}, []types.Datum{d0, d1}, []bool{false, false}, false},
		{[]types.Datum{d1, d1}, []types.Datum{d0, d0}, []bool{false, false}, false},
	}

	for _, T := range tblTwoColumns {
		ret, err := lessThan(sc, T.Arg1, T.Arg2, T.Arg3)
		require.NoError(t, err)
		require.Equal(t, T.Ret, ret)
	}
}

func TestInMemory(t *testing.T) {
	t.Parallel()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := r.Intn(10) + 1 // random int in range [1, 10]
	valSize := r.Intn(20) + 1 // random int in range [1, 20]
	bufSize := 40             // hold up to 40 items per file
	byDesc := make([]bool, keySize)
	for i := range byDesc {
		byDesc[i] = r.Intn(2) == 0
	}

	var (
		err    error
		fs     *FileSorter
		pkey   []types.Datum
		key    []types.Datum
		tmpDir string
		ret    bool
	)

	tmpDir, err = os.MkdirTemp("", "util_filesort_test")
	require.NoError(t, err)

	fsBuilder := new(Builder)
	fs, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmpDir).Build()
	require.NoError(t, err)
	defer fs.Close()

	nRows := r.Intn(bufSize-1) + 1 // random int in range [1, bufSize - 1]
	for i := 1; i <= nRows; i++ {
		err = fs.Input(nextRow(r, keySize, valSize))
		require.NoError(t, err)
	}

	pkey, _, _, err = fs.Output()
	require.NoError(t, err)
	for i := 1; i < nRows; i++ {
		key, _, _, err = fs.Output()
		require.NoError(t, err)
		//require.Nil(t, err)
		ret, err = lessThan(sc, key, pkey, byDesc)
		require.NoError(t, err)
		require.False(t, ret)
		pkey = key
	}
}

func TestMultipleFiles(t *testing.T) {
	t.Parallel()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := r.Intn(10) + 1 // random int in range [1, 10]
	valSize := r.Intn(20) + 1 // random int in range [1, 20]
	bufSize := 40             // hold up to 40 items per file
	byDesc := make([]bool, keySize)
	for i := range byDesc {
		byDesc[i] = r.Intn(2) == 0
	}

	var (
		err    error
		fs     *FileSorter
		pkey   []types.Datum
		key    []types.Datum
		tmpDir string
		ret    bool
	)

	tmpDir, err = os.MkdirTemp("", "util_filesort_test")
	require.NoError(t, err)

	fsBuilder := new(Builder)

	// Test for basic function.
	_, err = fsBuilder.Build()
	require.Equal(t, "StatementContext is nil", err.Error())
	fsBuilder.SetSC(sc)
	_, err = fsBuilder.Build()
	require.Equal(t, "key size is not positive", err.Error())
	fsBuilder.SetDesc(byDesc)
	_, err = fsBuilder.Build()
	require.Equal(t, "mismatch in key size and byDesc slice", err.Error())
	fsBuilder.SetSchema(keySize, valSize)
	_, err = fsBuilder.Build()
	require.Equal(t, "buffer size is not positive", err.Error())
	fsBuilder.SetBuf(bufSize)
	_, err = fsBuilder.Build()
	require.Equal(t, "tmpDir does not exist", err.Error())
	fsBuilder.SetDir(tmpDir)

	fs, err = fsBuilder.SetWorkers(1).Build()
	require.NoError(t, err)
	defer fs.Close()

	nRows := (r.Intn(bufSize) + 1) * (r.Intn(10) + 2)
	for i := 1; i <= nRows; i++ {
		err = fs.Input(nextRow(r, keySize, valSize))
		require.NoError(t, err)
	}

	pkey, _, _, err = fs.Output()
	require.NoError(t, err)
	for i := 1; i < nRows; i++ {
		key, _, _, err = fs.Output()
		require.NoError(t, err)
		ret, err = lessThan(sc, key, pkey, byDesc)
		require.NoError(t, err)
		require.False(t, ret)
		pkey = key
	}
}

func TestMultipleWorkers(t *testing.T) {
	t.Parallel()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := r.Intn(10) + 1 // random int in range [1, 10]
	valSize := r.Intn(20) + 1 // random int in range [1, 20]
	bufSize := 40             // hold up to 40 items per file
	byDesc := make([]bool, keySize)
	for i := range byDesc {
		byDesc[i] = r.Intn(2) == 0
	}

	var (
		err    error
		fs     *FileSorter
		pkey   []types.Datum
		key    []types.Datum
		tmpDir string
		ret    bool
	)

	tmpDir, err = os.MkdirTemp("", "util_filesort_test")
	require.NoError(t, err)

	fsBuilder := new(Builder)
	fs, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(4).SetDesc(byDesc).SetDir(tmpDir).Build()
	require.NoError(t, err)
	defer fs.Close()

	nRows := (r.Intn(bufSize) + 1) * (r.Intn(10) + 2)
	for i := 1; i <= nRows; i++ {
		err = fs.Input(nextRow(r, keySize, valSize))
		require.NoError(t, err)
	}

	pkey, _, _, err = fs.Output()
	require.NoError(t, err)
	for i := 1; i < nRows; i++ {
		key, _, _, err = fs.Output()
		require.NoError(t, err)
		ret, err = lessThan(sc, key, pkey, byDesc)
		require.NoError(t, err)
		require.False(t, ret)
		pkey = key
	}
}

func TestClose(t *testing.T) {
	t.Parallel()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := 2
	valSize := 2
	bufSize := 40
	byDesc := []bool{false, false}

	var (
		err     error
		fs0     *FileSorter
		fs1     *FileSorter
		tmpDir0 string
		tmpDir1 string
		errmsg  = "FileSorter has been closed"
	)

	// Prepare two FileSorter instances for tests
	fsBuilder := new(Builder)
	tmpDir0, err = os.MkdirTemp("", "util_filesort_test")
	require.NoError(t, err)
	fs0, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmpDir0).Build()
	require.NoError(t, err)
	defer fs0.Close()

	tmpDir1, err = os.MkdirTemp("", "util_filesort_test")
	require.NoError(t, err)
	fs1, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmpDir1).Build()
	require.NoError(t, err)
	defer fs1.Close()

	// 1. Close after some Input
	err = fs0.Input(nextRow(r, keySize, valSize))
	require.NoError(t, err)

	err = fs0.Close()
	require.NoError(t, err)

	_, err = os.Stat(tmpDir0)
	require.True(t, os.IsNotExist(err))

	_, _, _, err = fs0.Output()
	require.EqualError(t, err, errmsg)

	err = fs0.Input(nextRow(r, keySize, valSize))
	require.EqualError(t, err, errmsg)

	err = fs0.Close()
	require.NoError(t, err)

	// 2. Close after some Output
	err = fs1.Input(nextRow(r, keySize, valSize))
	require.NoError(t, err)
	err = fs1.Input(nextRow(r, keySize, valSize))
	require.NoError(t, err)

	_, _, _, err = fs1.Output()
	require.NoError(t, err)

	err = fs1.Close()
	require.NoError(t, err)

	_, err = os.Stat(tmpDir1)
	require.True(t, os.IsNotExist(err))

	_, _, _, err = fs1.Output()
	require.EqualError(t, err, errmsg)

	err = fs1.Input(nextRow(r, keySize, valSize))
	require.EqualError(t, err, errmsg)

	err = fs1.Close()
	require.NoError(t, err)
}

func TestMismatchedUsage(t *testing.T) {
	t.Parallel()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := 2
	valSize := 2
	bufSize := 40
	byDesc := []bool{false, false}

	var (
		err    error
		fs0    *FileSorter
		fs1    *FileSorter
		key    []types.Datum
		tmpDir string
		errmsg = "call input after output"
	)

	// Prepare two FileSorter instances for tests
	fsBuilder := new(Builder)
	tmpDir, err = os.MkdirTemp("", "util_filesort_test")
	require.NoError(t, err)
	fs0, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmpDir).Build()
	require.NoError(t, err)
	defer fs0.Close()

	tmpDir, err = os.MkdirTemp("", "util_filesort_test")
	require.NoError(t, err)
	fs1, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmpDir).Build()
	require.NoError(t, err)
	defer fs1.Close()

	// 1. call Output after fetched all rows
	err = fs0.Input(nextRow(r, keySize, valSize))
	require.NoError(t, err)

	key, _, _, err = fs0.Output()
	require.NoError(t, err)
	require.NotNil(t, key)

	key, _, _, err = fs0.Output()
	require.NoError(t, err)
	require.Nil(t, key)

	// 2. call Input after Output
	err = fs1.Input(nextRow(r, keySize, valSize))
	require.NoError(t, err)

	key, _, _, err = fs1.Output()
	require.NoError(t, err)
	require.NotNil(t, key)

	err = fs1.Input(nextRow(r, keySize, valSize))
	require.EqualError(t, err, errmsg)
}
