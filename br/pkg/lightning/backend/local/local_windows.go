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

//go:build windows

package local

import (
	"math"

	"github.com/pingcap/errors"
)

type Rlim_t = uint64

// return a big value as unlimited, since rlimit verify is skipped in windows.
func GetSystemRLimit() (uint64, error) {
	return math.MaxInt32, nil
}

func VerifyRLimit(estimateMaxFiles uint64) error {
	return errors.New("Local-backend is not tested on Windows. Run with --check-requirements=false to disable this check, but you are on your own risk.")
}
