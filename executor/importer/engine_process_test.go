// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"fmt"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
)

var actualOrders = []int{}

type testCloser struct{}

func (t testCloser) Close() error {
	actualOrders = append(actualOrders, 11)
	return nil
}

func TestMultiCloser(t *testing.T) {
	expectedOrder := []int{3, 11, 2, 1}
	closer := multiCloser{logger: log.L()}
	closer.addFn(func() error {
		actualOrders = append(actualOrders, 1)
		return nil
	})
	closer.addFn(func() error {
		actualOrders = append(actualOrders, 2)
		return fmt.Errorf("xx")
	})
	closer.add(testCloser{})
	closer.addFn(func() error {
		actualOrders = append(actualOrders, 3)
		return nil
	})
	closer.Close()
	require.Equal(t, expectedOrder, actualOrders)
}
