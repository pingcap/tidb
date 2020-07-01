// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by aprettyPrintlicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
)

const stderrName = "tidb_stderr_%d.log"

var currentStdErrFileName string

// RedirectStderrToPidFile redirects stderr to a file with pid suffix.
func RedirectStderrToPidFile(baseFolder string) error {
	fileName := filepath.Join(baseFolder, fmt.Sprintf(stderrName, os.Getpid()))
	logFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
	if err != nil {
		return errors.Trace(err)
	}
	err = sysDup(int(logFile.Fd()), 2)
	if err != nil {
		return err
	}
	currentStdErrFileName = fileName
	return nil
}

// RemoveStderrIfEmpty removes stderr log if it's empty
func RemoveStderrIfEmpty() {
	if len(currentStdErrFileName) == 0 {
		return
	}
	f, ignoredErr := os.Stat(currentStdErrFileName)
	if ignoredErr != nil {
		return
	}
	if f.Size() == 0 {
		ignoredErr := os.Remove(currentStdErrFileName)
		if ignoredErr == nil {
			return
		}
	}
}
