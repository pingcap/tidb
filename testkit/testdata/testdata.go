// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !codes
// +build !codes

package testdata

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

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

func loadTestSuiteData(dir, suiteName string) (res TestData, err error) {
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
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return res, err
	}
	// Remove comments, since they are not allowed in json.
	re := regexp.MustCompile("(?s)//.*?\n")
	err = json.Unmarshal(re.ReplaceAll(byteValue, nil), &res)
	return res, err
}

// OnRecord execute the function to update result.
func OnRecord(updateFunc func()) {
	if record {
		updateFunc()
	}
}

// ConvertRowsToStrings converts [][]interface{} to []string.
func ConvertRowsToStrings(rows [][]interface{}) (rs []string) {
	for _, row := range rows {
		s := fmt.Sprintf("%v", row)
		// Trim the leftmost `[` and rightmost `]`.
		s = s[1 : len(s)-1]
		rs = append(rs, s)
	}
	return rs
}

// GetTestCases gets the test cases for a test function.
func (td *TestData) GetTestCases(t *testing.T, in interface{}, out interface{}) {
	// Extract caller's name.
	pc, _, _, ok := runtime.Caller(1)
	require.True(t, ok)
	details := runtime.FuncForPC(pc)
	funcNameIdx := strings.LastIndex(details.Name(), ".")
	funcName := details.Name()[funcNameIdx+1:]

	casesIdx, ok := td.funcMap[funcName]
	require.Truef(t, ok, "Must get test %s", funcName)
	err := json.Unmarshal(*td.input[casesIdx].Cases, in)
	require.NoError(t, err)
	if !record {
		err = json.Unmarshal(*td.output[casesIdx].Cases, out)
		require.NoError(t, err)
	} else {
		// Init for generate output file.
		inputLen := reflect.ValueOf(in).Elem().Len()
		v := reflect.ValueOf(out).Elem()
		if v.Kind() == reflect.Slice {
			v.Set(reflect.MakeSlice(v.Type(), inputLen, inputLen))
		}
	}
	td.output[casesIdx].decodedOut = out
}

func (td *TestData) generateOutputIfNeeded() error {
	if !record {
		return nil
	}

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	for i, test := range td.output {
		err := enc.Encode(test.decodedOut)
		if err != nil {
			return err
		}
		res := make([]byte, len(buf.Bytes()))
		copy(res, buf.Bytes())
		buf.Reset()
		rm := json.RawMessage(res)
		td.output[i].Cases = &rm
	}
	err := enc.Encode(td.output)
	if err != nil {
		return err
	}
	file, err := os.Create(fmt.Sprintf("%s_out.json", td.filePathPrefix))
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

// BookKeeper does TestData suite bookkeeping.
type BookKeeper map[string]TestData

// LoadTestSuiteData loads test suite data from file and bookkeeping in the map.
func (m *BookKeeper) LoadTestSuiteData(dir, suiteName string) {
	testData, err := loadTestSuiteData(dir, suiteName)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "testdata: Errors on loading test data from file: %v\n", err)
		os.Exit(1)
	}
	(*m)[suiteName] = testData
}

// GenerateOutputIfNeeded generate the output file from data bookkeeping in the map.
func (m *BookKeeper) GenerateOutputIfNeeded() {
	for _, testData := range *m {
		err := testData.generateOutputIfNeeded()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "testdata: Errors on generating output: %v\n", err)
			os.Exit(1)
		}
	}
}

// Record is a temporary method for testutil to avoid "flag redefined: record" error,
// After we migrate all tests based on former testdata, we should remove testutil and this method.
func Record() bool {
	return record
}
