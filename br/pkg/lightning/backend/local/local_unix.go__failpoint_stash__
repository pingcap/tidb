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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !windows

package local

import (
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
)

const (
	// maximum max open files value
	maxRLimit = 1000000
)

func GetSystemRLimit() (Rlim_t, error) {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	return rLimit.Cur, err
}

// VerifyRLimit checks whether the open-file limit is large enough.
// In Local-backend, we need to read and write a lot of L0 SST files, so we need
// to check system max open files limit.
func VerifyRLimit(estimateMaxFiles Rlim_t) error {
	if estimateMaxFiles > maxRLimit {
		estimateMaxFiles = maxRLimit
	}
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	failpoint.Inject("GetRlimitValue", func(v failpoint.Value) {
		limit := Rlim_t(v.(int))
		rLimit.Cur = limit
		rLimit.Max = limit
		err = nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if rLimit.Cur >= estimateMaxFiles {
		return nil
	}
	if rLimit.Max < estimateMaxFiles {
		// If the process is not started by privileged user, this will fail.
		rLimit.Max = estimateMaxFiles
	}
	prevLimit := rLimit.Cur
	rLimit.Cur = estimateMaxFiles
	failpoint.Inject("SetRlimitError", func(v failpoint.Value) {
		if v.(bool) {
			err = errors.New("Setrlimit Injected Error")
		}
	})
	if err == nil {
		err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	}
	if err != nil {
		return errors.Annotatef(err, "the maximum number of open file descriptors is too small, got %d, expect greater or equal to %d", prevLimit, estimateMaxFiles)
	}

	// fetch the rlimit again to make sure our setting has taken effect
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return errors.Trace(err)
	}
	if rLimit.Cur < estimateMaxFiles {
		helper := "Please manually execute `ulimit -n %d` to increase the open files limit."
		return errors.Errorf("cannot update the maximum number of open file descriptors, expected: %d, got: %d. %s",
			estimateMaxFiles, rLimit.Cur, helper)
	}

	log.L().Info("Set the maximum number of open file descriptors(rlimit)",
		zapRlim_t("old", prevLimit), zapRlim_t("new", estimateMaxFiles))
	return nil
}
