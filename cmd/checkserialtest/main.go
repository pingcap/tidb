// Copyright 2020 PingCAP, Inc.
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
)

func main() {
	path, err := os.Getwd()
	if err != nil {
		fmt.Println("Get work directory error: " + err.Error())
		os.Exit(1)
	}

	Check(path, false)
}

func Check(path string, test bool) *FailpointChecker {
	absPath, err := filepath.Abs(path)
	if err != nil {
		fmt.Println("Error occurred in absolute path " + path + " with " + err.Error())
		os.Exit(1)
	}
	realPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		fmt.Println("Error resolving symbolic link "+absPath+", ", err.Error())
		os.Exit(1)
	}

	checker := NewChecker(realPath, CollateTestSuiteInfo)
	if err := checker.check(test); err != nil {
		fmt.Println("Check error " + err.Error())
		os.Exit(1)
	}
	checker.Mode = CheckFailPoint
	if err := checker.check(test); err != nil {
		fmt.Println("Check error " + err.Error())
		os.Exit(1)
	}

	if test {
		return checker
	}

	if len(checker.errList) != 0 {
		for _, err := range checker.errList {
			fmt.Println("Check error " + err.Error())
		}
		os.Exit(1)
	}
	return nil
}

