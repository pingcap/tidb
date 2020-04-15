// Copyright 2015 PingCAP, Inc.
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

// +build !codes

package testutil

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/config"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/types"
)

// CompareUnorderedStringSlice compare two string slices.
// If a and b is exactly the same except the order, it returns true.
// In otherwise return false.
func CompareUnorderedStringSlice(a []string, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, i := range a {
		_, ok := m[i]
		if !ok {
			m[i] = 1
		} else {
			m[i]++
		}
	}

	for _, i := range b {
		_, ok := m[i]
		if !ok {
			return false
		}
		m[i]--
		if m[i] == 0 {
			delete(m, i)
		}
	}
	return len(m) == 0
}

// datumEqualsChecker is a checker for DatumEquals.
type datumEqualsChecker struct {
	*check.CheckerInfo
}

// DatumEquals checker verifies that the obtained value is equal to
// the expected value.
// For example:
//     c.Assert(value, DatumEquals, NewDatum(42))
var DatumEquals check.Checker = &datumEqualsChecker{
	&check.CheckerInfo{Name: "DatumEquals", Params: []string{"obtained", "expected"}},
}

func (checker *datumEqualsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	defer func() {
		if v := recover(); v != nil {
			result = false
			error = fmt.Sprint(v)
		}
	}()
	paramFirst, ok := params[0].(types.Datum)
	if !ok {
		panic("the first param should be datum")
	}
	paramSecond, ok := params[1].(types.Datum)
	if !ok {
		panic("the second param should be datum")
	}
	sc := new(stmtctx.StatementContext)
	res, err := paramFirst.CompareDatum(sc, &paramSecond)
	if err != nil {
		panic(err)
	}
	return res == 0, ""
}

// RowsWithSep is a convenient function to wrap args to a slice of []interface.
// The arg represents a row, split by sep.
func RowsWithSep(sep string, args ...string) [][]interface{} {
	rows := make([][]interface{}, len(args))
	for i, v := range args {
		strs := strings.Split(v, sep)
		row := make([]interface{}, len(strs))
		for j, s := range strs {
			row[j] = s
		}
		rows[i] = row
	}
	return rows
}

// record is a flag used for generate test result.
var record bool

func init() {
	flag.BoolVar(&record, "record", false, "to generate test result")
}

type testCases struct {
	Name       string
	Cases      *json.RawMessage // For delayed parse.
	decodedOut interface{}      // For generate output.
}

// TestData stores all the data of a test suite.
type TestData struct {
	input          []testCases
	output         []testCases
	filePathPrefix string
	funcMap        map[string]int
}

// LoadTestSuiteData loads test suite data from file.
func LoadTestSuiteData(dir, suiteName string) (res TestData, err error) {
	res.filePathPrefix = filepath.Join(dir, suiteName)
	res.input, err = loadTestSuiteCases(fmt.Sprintf("%s_in.json", res.filePathPrefix))
	if err != nil {
		return res, err
	}
	if record {
		res.output = make([]testCases, len(res.input))
		for i := range res.input {
			res.output[i].Name = res.input[i].Name
		}
	} else {
		res.output, err = loadTestSuiteCases(fmt.Sprintf("%s_out.json", res.filePathPrefix))
		if err != nil {
			return res, err
		}
		if len(res.input) != len(res.output) {
			return res, errors.New(fmt.Sprintf("Number of test input cases %d does not match test output cases %d", len(res.input), len(res.output)))
		}
	}
	res.funcMap = make(map[string]int, len(res.input))
	for i, test := range res.input {
		res.funcMap[test.Name] = i
		if test.Name != res.output[i].Name {
			return res, errors.New(fmt.Sprintf("Input name of the %d-case %s does not match output %s", i, test.Name, res.output[i].Name))
		}
	}
	return res, nil
}

func loadTestSuiteCases(filePath string) (res []testCases, err error) {
	jsonFile, err := os.Open(filePath)
	if err != nil {
		return res, err
	}
	defer func() {
		if err1 := jsonFile.Close(); err == nil && err1 != nil {
			err = err1
		}
	}()
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return res, err
	}
	// Remove comments, since they are not allowed in json.
	re := regexp.MustCompile("(?s)//.*?\n")
	err = json.Unmarshal(re.ReplaceAll(byteValue, nil), &res)
	return res, err
}

// GetTestCasesByName gets the test cases for a test function by its name.
func (t *TestData) GetTestCasesByName(caseName string, c *check.C, in interface{}, out interface{}) {
	casesIdx, ok := t.funcMap[caseName]
	c.Assert(ok, check.IsTrue, check.Commentf("Must get test %s", caseName))
	err := json.Unmarshal(*t.input[casesIdx].Cases, in)
	c.Assert(err, check.IsNil)
	if !record {
		err = json.Unmarshal(*t.output[casesIdx].Cases, out)
		c.Assert(err, check.IsNil)
	} else {
		// Init for generate output file.
		inputLen := reflect.ValueOf(in).Elem().Len()
		v := reflect.ValueOf(out).Elem()
		if v.Kind() == reflect.Slice {
			v.Set(reflect.MakeSlice(v.Type(), inputLen, inputLen))
		}
	}
	t.output[casesIdx].decodedOut = out
}

// GetTestCases gets the test cases for a test function.
func (t *TestData) GetTestCases(c *check.C, in interface{}, out interface{}) {
	// Extract caller's name.
	pc, _, _, ok := runtime.Caller(1)
	c.Assert(ok, check.IsTrue)
	details := runtime.FuncForPC(pc)
	funcNameIdx := strings.LastIndex(details.Name(), ".")
	funcName := details.Name()[funcNameIdx+1:]

	casesIdx, ok := t.funcMap[funcName]
	c.Assert(ok, check.IsTrue, check.Commentf("Must get test %s", funcName))
	err := json.Unmarshal(*t.input[casesIdx].Cases, in)
	c.Assert(err, check.IsNil)
	if !record {
		err = json.Unmarshal(*t.output[casesIdx].Cases, out)
		c.Assert(err, check.IsNil)
	} else {
		// Init for generate output file.
		inputLen := reflect.ValueOf(in).Elem().Len()
		v := reflect.ValueOf(out).Elem()
		if v.Kind() == reflect.Slice {
			v.Set(reflect.MakeSlice(v.Type(), inputLen, inputLen))
		}
	}
	t.output[casesIdx].decodedOut = out
}

// OnRecord execute the function to update result.
func (t *TestData) OnRecord(updateFunc func()) {
	if record {
		updateFunc()
	}
}

// ConvertRowsToStrings converts [][]interface{} to []string.
func (t *TestData) ConvertRowsToStrings(rows [][]interface{}) (rs []string) {
	for _, row := range rows {
		s := fmt.Sprintf("%v", row)
		// Trim the leftmost `[` and rightmost `]`.
		s = s[1 : len(s)-1]
		rs = append(rs, s)
	}
	return rs
}

// ConvertSQLWarnToStrings converts []SQLWarn to []string.
func (t *TestData) ConvertSQLWarnToStrings(warns []stmtctx.SQLWarn) (rs []string) {
	for _, warn := range warns {
		rs = append(rs, fmt.Sprintf(warn.Err.Error()))
	}
	return rs
}

// GenerateOutputIfNeeded generate the output file.
func (t *TestData) GenerateOutputIfNeeded() error {
	if !record {
		return nil
	}

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	for i, test := range t.output {
		err := enc.Encode(test.decodedOut)
		if err != nil {
			return err
		}
		res := make([]byte, len(buf.Bytes()))
		copy(res, buf.Bytes())
		buf.Reset()
		rm := json.RawMessage(res)
		t.output[i].Cases = &rm
	}
	err := enc.Encode(t.output)
	if err != nil {
		return err
	}
	file, err := os.Create(fmt.Sprintf("%s_out.json", t.filePathPrefix))
	if err != nil {
		return err
	}
	defer func() {
		if err1 := file.Close(); err == nil && err1 != nil {
			err = err1
		}
	}()
	_, err = file.Write(buf.Bytes())
	return err
}

// ConfigTestUtils contains a set of set-up/restore methods related to config used in tests.
var ConfigTestUtils configTestUtils

type configTestUtils struct {
	autoRandom
}

type autoRandom struct {
	originAllowAutoRandom bool
	originAlterPrimaryKey bool
}

// SetupAutoRandomTestConfig set alter-primary-key to false, and set allow-auto-random to true and save their origin values.
// This method should only be used for the tests in SerialSuite.
func (a *autoRandom) SetupAutoRandomTestConfig() {
	globalCfg := config.GetGlobalConfig()
	a.originAllowAutoRandom = globalCfg.Experimental.AllowAutoRandom
	a.originAlterPrimaryKey = globalCfg.AlterPrimaryKey
	globalCfg.AlterPrimaryKey = false
	globalCfg.Experimental.AllowAutoRandom = true
}

// RestoreAutoRandomTestConfig restore the values had been saved in SetupTestConfig.
// This method should only be used for the tests in SerialSuite.
func (a *autoRandom) RestoreAutoRandomTestConfig() {
	globalCfg := config.GetGlobalConfig()
	globalCfg.Experimental.AllowAutoRandom = a.originAllowAutoRandom
	globalCfg.AlterPrimaryKey = a.originAlterPrimaryKey
}

// MaskSortHandles masks highest shard_bits numbers of table handles and sort it.
func (a *autoRandom) MaskSortHandles(handles []int64, shardBitsCount int, fieldType byte) []int64 {
	typeBitsLength := mysql.DefaultLengthOfMysqlTypes[fieldType] * 8
	const signBitCount = 1
	shiftBitsCount := 64 - typeBitsLength + shardBitsCount + signBitCount
	ordered := make([]int64, len(handles))
	for i, h := range handles {
		ordered[i] = h << shiftBitsCount >> shiftBitsCount
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered
}
