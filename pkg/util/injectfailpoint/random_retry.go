// Copyright 2025 PingCAP, Inc.
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

package injectfailpoint

import (
	"io"
	"math/rand"
	"runtime"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
)

func getFunctionName() string {
	pc, _, _, _ := runtime.Caller(2)
	return runtime.FuncForPC(pc).Name()
}

// DXFRandomErrorWithOnePercent returns an error with probability 0.01. It controls the DXF's failpoint.
func DXFRandomErrorWithOnePercent() error {
	if _, _err_ := failpoint.Eval(_curpkg_("DXFRandomError")); _err_ == nil {
		return RandomError(0.01, errors.Errorf("injected random error, caller: %s", getFunctionName()))
	}
	return nil
}

// DXFRandomErrorWithOnePercentWrapper returns an error with probability 0.01. It controls the DXF's failpoint.
func DXFRandomErrorWithOnePercentWrapper(err error) error {
	if err != nil {
		return err
	}
	if _, _err_ := failpoint.Eval(_curpkg_("DXFRandomError")); _err_ == nil {
		return RandomError(0.01, errors.Errorf("injected random error, caller: %s", getFunctionName()))
	}
	return nil
}

// DXFRandomErrorWithOnePerThousand returns an error with probability 0.001. It controls the DXF's failpoint.
func DXFRandomErrorWithOnePerThousand() error {
	if _, _err_ := failpoint.Eval(_curpkg_("DXFRandomError")); _err_ == nil {
		return RandomError(0.001, errors.Errorf("injected random error, caller: %s", getFunctionName()))
	}
	return nil
}

// RandomErrorForReadWithOnePerPercent returns a read error with probability 0.01. It controls the DXF's failpoint.
func RandomErrorForReadWithOnePerPercent(n int, err error) (int, error) {
	if _, _err_ := failpoint.Eval(_curpkg_("DXFRandomError")); _err_ == nil {
		if n == 0 || err != nil || rand.Float64() > 0.01 {
			return n, err
		}
		if rand.Float64() < 0.2 {
			return 0, io.ErrUnexpectedEOF
		}
		return rand.Intn(n), io.ErrUnexpectedEOF
	}
	return n, err
}

// RandomError returns an error with the given probability.
func RandomError(probability float64, err error) error {
	if rand.Float64() < probability {
		return err
	}
	return nil
}
