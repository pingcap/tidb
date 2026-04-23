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

package benchdaily_test

import (
	"encoding/json"
	"flag"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/util/benchdaily"
)

var (
	date       = flag.String("date", "", " commit date")
	commitHash = flag.String("commit", "unknown", "brief git commit hash")
)

func combineFiles(commitHash string, dateInUnix string, inputFiles []string, outputFile string) {
	res := make([]benchdaily.ExportedBenchResult, 0, 100)
	for _, file := range inputFiles {
		tmp := benchdaily.ExportedReadBenchResultFromFile(file)
		res = append(res, tmp...)
	}

	output := benchdaily.ExportedBenchOutput{
		Date:   dateInUnix,
		Commit: commitHash,
		Result: res,
	}

	out, err := os.Create(outputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	enc.Encode(output)
}

func RunBenchDaily(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}

	// Avoiding slow down the CI.
	outfilePtr := benchdaily.ExportedOutfile
	if *date == "" || outfilePtr == nil || **outfilePtr == "" {
		return
	}
	outfile := **outfilePtr

	fileList := make([]string, 0, 20)
	filepath.Walk("../..", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && info.Name() == ".git" {
			return filepath.SkipDir
		}
		if info.Name() == "bench_daily.json" {
			fileList = append(fileList, path)
		}
		return nil
	})

	combineFiles(*commitHash, *date, fileList, outfile)
}
