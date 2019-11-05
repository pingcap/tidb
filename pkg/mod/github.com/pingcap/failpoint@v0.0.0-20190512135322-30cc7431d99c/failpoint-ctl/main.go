// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pingcap/failpoint/code"
	"github.com/pingcap/failpoint/failpoint-ctl/version"
)

func main() {
	if len(os.Args) < 2 || (os.Args[1] != "enable" && os.Args[1] != "disable" && os.Args[1] != "-V") {
		usage()
	}

	if os.Args[1] == "-V" {
		version.PrintVersion()
		os.Exit(0)
	}

	// Use current work path if user does not specify any path
	paths := os.Args[2:]
	if len(paths) == 0 {
		wd, err := os.Getwd()
		if err != nil {
			fmt.Println("Get work directory error: " + err.Error())
			os.Exit(1)
		}
		paths = append(paths, wd)
	}

	// Expand all paths to its absolute path form
	for i := range paths {
		absPath, err := filepath.Abs(paths[i])
		if err != nil {
			fmt.Println("Error occurred in absolute path " + paths[i] + " with " + err.Error())
			os.Exit(1)
		}
		paths[i] = absPath
	}

	switch os.Args[1] {
	case "enable":
		var rewritePath []string
		var errOccurred bool
		for _, path := range paths {
			rewritePath = append(rewritePath, path)
			rewriter := code.NewRewriter(path)
			if err := rewriter.Rewrite(); err != nil {
				fmt.Println("Rewrite error " + err.Error())
				errOccurred = true
				break
			}
		}
		// Restore all paths which have been rewrited if any error occurred
		// to avoid partial rewrite state which maybe make user strange.
		if errOccurred {
			restoreFiles(rewritePath)
			os.Exit(1)
		}
	case "disable":
		restoreFiles(paths)
	}
}

func usage() {
	fmt.Println("failpoint-ctl enable/disable /target/path [/target/path2 /target/path3 ...]")
	os.Exit(1)
}

func restoreFiles(paths []string) {
	for i := range paths {
		restorer := code.NewRestorer(paths[i])
		err := restorer.Restore()
		if err != nil {
			fmt.Println("Restore error " + err.Error())
		}
	}
}
