package filesort

import (
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testFileSortSuite{})

type testFileSortSuite struct {
}

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

func (s *testFileSortSuite) TestLessThan(c *C) {
	defer testleak.AfterTest(c)()

	sc := new(variable.StatementContext)

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

	for _, t := range tblOneColumn {
		ret, err := lessThan(sc, t.Arg1, t.Arg2, t.Arg3)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.Ret)
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

	for _, t := range tblTwoColumns {
		ret, err := lessThan(sc, t.Arg1, t.Arg2, t.Arg3)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testFileSortSuite) TestSingleFile(c *C) {
	defer testleak.AfterTest(c)()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(variable.StatementContext)
	keySize := r.Intn(10) + 1 // random int in range [1, 10]
	valSize := r.Intn(20) + 1 // random int in range [1, 20]
	bufSize := 40             // hold up to 40 items per file
	byDesc := make([]bool, keySize)
	for i := range byDesc {
		byDesc[i] = (r.Intn(2) == 0)
	}

	var (
		err    error
		fs     *FileSorter
		pkey   []types.Datum
		key    []types.Datum
		tmpDir string
		ret    bool
	)

	tmpDir, err = ioutil.TempDir("", "util_filesort_test")
	if err != nil {
		panic(err)
	}

	fsBuilder := NewBuilder()
	fs, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetDesc(byDesc).SetDir(tmpDir).Build()
	c.Assert(err, IsNil)

	nRows := r.Intn(bufSize-1) + 1 // random int in range [1, bufSize - 1]
	for i := 1; i <= nRows; i++ {
		err = fs.Input(nextRow(r, keySize, valSize))
		c.Assert(err, IsNil)
	}

	pkey, _, _, err = fs.Output()
	c.Assert(err, IsNil)
	for i := 1; i < nRows; i++ {
		key, _, _, err = fs.Output()
		c.Assert(err, IsNil)
		ret, err = lessThan(sc, key, pkey, byDesc)
		c.Assert(err, IsNil)
		c.Assert(ret, IsFalse)
		pkey = key
	}
}

func (s *testFileSortSuite) TestMultipleFiles(c *C) {
	defer testleak.AfterTest(c)()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(variable.StatementContext)
	keySize := r.Intn(10) + 1 // random int in range [1, 10]
	valSize := r.Intn(20) + 1 // random int in range [1, 20]
	bufSize := 40             // hold up to 40 items per file
	byDesc := make([]bool, keySize)
	for i := range byDesc {
		byDesc[i] = (r.Intn(2) == 0)
	}

	var (
		err    error
		fs     *FileSorter
		pkey   []types.Datum
		key    []types.Datum
		tmpDir string
		ret    bool
	)

	tmpDir, err = ioutil.TempDir("", "util_filesort_test")
	if err != nil {
		panic(err)
	}

	fsBuilder := NewBuilder()
	fs, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetDesc(byDesc).SetDir(tmpDir).Build()
	c.Assert(err, IsNil)

	nRows := (r.Intn(bufSize) + 1) * (r.Intn(10) + 2)
	for i := 1; i <= nRows; i++ {
		err = fs.Input(nextRow(r, keySize, valSize))
		c.Assert(err, IsNil)
	}

	pkey, _, _, err = fs.Output()
	c.Assert(err, IsNil)
	for i := 1; i < nRows; i++ {
		key, _, _, err = fs.Output()
		c.Assert(err, IsNil)
		ret, err = lessThan(sc, key, pkey, byDesc)
		c.Assert(err, IsNil)
		c.Assert(ret, IsFalse)
		pkey = key
	}
}

func (s *testFileSortSuite) TestClose(c *C) {
	defer testleak.AfterTest(c)()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(variable.StatementContext)
	keySize := 2
	valSize := 2
	bufSize := 40
	byDesc := []bool{false, false}

	var (
		err    error
		fs0    *FileSorter
		fs1    *FileSorter
		tmpDir string
		errmsg = "FileSorter has been closed"
	)

	// Prepare two FileSorter instances for tests
	fsBuilder := NewBuilder()
	tmpDir, err = ioutil.TempDir("", "util_filesort_test")
	if err != nil {
		panic(err)
	}
	fs0, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetDesc(byDesc).SetDir(tmpDir).Build()
	c.Assert(err, IsNil)

	tmpDir, err = ioutil.TempDir("", "util_filesort_test")
	if err != nil {
		panic(err)
	}
	fs1, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetDesc(byDesc).SetDir(tmpDir).Build()
	c.Assert(err, IsNil)

	// 1. Close after some Input
	err = fs0.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)

	err = fs0.Close()
	c.Assert(err, IsNil)

	_, _, _, err = fs0.Output()
	c.Assert(err, ErrorMatches, errmsg)

	err = fs0.Input(nextRow(r, keySize, valSize))
	c.Assert(err, ErrorMatches, errmsg)

	err = fs0.Close()
	c.Assert(err, ErrorMatches, errmsg)

	// 2. Close after some Output
	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)
	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)

	_, _, _, err = fs1.Output()
	c.Assert(err, IsNil)

	err = fs1.Close()
	c.Assert(err, IsNil)

	_, _, _, err = fs1.Output()
	c.Assert(err, ErrorMatches, errmsg)

	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, ErrorMatches, errmsg)

	err = fs1.Close()
	c.Assert(err, ErrorMatches, errmsg)
}

func (s *testFileSortSuite) TestMismatchedUsage(c *C) {
	defer testleak.AfterTest(c)()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(variable.StatementContext)
	keySize := 2
	valSize := 2
	bufSize := 40
	byDesc := []bool{false, false}

	var (
		err     error
		fs0     *FileSorter
		fs1     *FileSorter
		tmpDir  string
		errmsg0 = "all rows have been fetched"
		errmsg1 = "call input after output"
	)

	// Prepare two FileSorter instances for tests
	fsBuilder := NewBuilder()
	tmpDir, err = ioutil.TempDir("", "util_filesort_test")
	if err != nil {
		panic(err)
	}
	fs0, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetDesc(byDesc).SetDir(tmpDir).Build()
	c.Assert(err, IsNil)

	tmpDir, err = ioutil.TempDir("", "util_filesort_test")
	if err != nil {
		panic(err)
	}
	fs1, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetDesc(byDesc).SetDir(tmpDir).Build()
	c.Assert(err, IsNil)

	// 1. call Output after fetched all rows
	err = fs0.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)

	_, _, _, err = fs0.Output()
	c.Assert(err, IsNil)

	_, _, _, err = fs0.Output()
	c.Assert(err, ErrorMatches, errmsg0)

	// 2. call Input after Output
	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)

	_, _, _, err = fs1.Output()
	c.Assert(err, IsNil)

	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, ErrorMatches, errmsg1)
}
