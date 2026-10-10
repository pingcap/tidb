// Copyright 2026 PingCAP, Inc.
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

// benchdata streams reproducible id,body CSV to stdout without connecting to a cluster.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/pingcap/tidb/tests/ftse2e/benchdata"
)

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("benchdata", flag.ContinueOnError)
	rows := flags.Int("rows", 10000, "number of rows")
	size := flags.Int("bytes", 4096, "UTF-8 document bytes per row (at least 128)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *rows <= 0 || *size < 128 || flags.NArg() != 0 {
		return fmt.Errorf("require -rows > 0 and -bytes >= 128, with no positional arguments")
	}
	w := csv.NewWriter(output)
	for row := range *rows {
		if err := w.Write([]string{strconv.Itoa(row + 1), benchdata.Document(row, *size)}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
