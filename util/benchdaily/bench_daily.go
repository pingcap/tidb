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

package benchdaily

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// BenchOutput is the json format for the final output file.
type BenchOutput struct {
	Date   string
	Commit string
	Result []BenchResult
}

// BenchResult is one benchmark result.
type BenchResult struct {
	Name        string
	NsPerOp     int64
	AllocsPerOp int64
	BytesPerOp  int64
}

func benchmarkResultToJSON(name string, r testing.BenchmarkResult) BenchResult {
	return BenchResult{
		Name:        name,
		NsPerOp:     r.NsPerOp(),
		AllocsPerOp: r.AllocsPerOp(),
		BytesPerOp:  r.AllocedBytesPerOp(),
	}
}

func callerName(f func(b *testing.B)) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	idx := strings.LastIndexByte(fullName, '.')
	if idx > 0 && idx+1 < len(fullName) {
		return fullName[idx+1:]
	}
	return fullName
}

var outfile = flag.String("outfile", "", "specify the output file")

// Run runs some benchmark tests, write the result to a JSON file.
func Run(tests ...func(b *testing.B)) {
	if !flag.Parsed() {
		flag.Parse()
	}

	// Avoiding slow down the CI.
	if *outfile == "" {
		return
	}

	res := make([]BenchResult, 0, len(tests))
	for _, t := range tests {
		name := callerName(t)
		r1 := testing.Benchmark(t)
		r2 := benchmarkResultToJSON(name, r1)
		res = append(res, r2)
	}

	writeBenchResultToFile(res, *outfile)
}

func readBenchResultFromFile(file string) []BenchResult {
	f, err := os.Open(file)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	res := make([]BenchResult, 0, 100)
	dec := json.NewDecoder(f)
	err = dec.Decode(&res)
	if err != nil {
		log.Panic(err)
	}
	return res
}

func writeBenchResultToFile(res []BenchResult, file string) {
	out, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := out.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	enc := json.NewEncoder(out)
	err = enc.Encode(res)
	if err != nil {
		log.Fatal(err)
	}
}
