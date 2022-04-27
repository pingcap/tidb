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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common_test

import (
	"errors"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func TestOnceError(t *testing.T) {
	var err common.OnceError

	require.Nil(t, err.Get())

	err.Set(nil)
	require.Nil(t, err.Get())

	e := errors.New("1")
	err.Set(e)
	require.Equal(t, e, err.Get())

	e2 := errors.New("2")
	err.Set(e2)
	require.Equal(t, e, err.Get()) // e, not e2.

	err.Set(nil)
	require.Equal(t, e, err.Get())

	ch := make(chan struct{})
	go func() {
		err.Set(nil)
		ch <- struct{}{}
	}()
	<-ch
	require.Equal(t, e, err.Get())
}
