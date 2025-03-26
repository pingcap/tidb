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
	"math/rand"

	"github.com/pingcap/failpoint"
)

// DXFRandomError returns an error with the given probability. It controls the DXF's failpoint.
func DXFRandomError(probability float64, err error) error {
	failpoint.Inject("DXFRandomError", func() {
		failpoint.Return(RandomError(probability, err))
	})
	return nil
}

// RandomError returns an error with the given probability.
func RandomError(probability float64, err error) error {
	if rand.Float64() < probability {
		return err
	}
	return nil
}
