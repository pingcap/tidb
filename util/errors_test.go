// Copyright 2022 PingCAP, Inc.
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

package util_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestOriginError(t *testing.T) {
	require.Nil(t, util.OriginError(nil))

	err1 := errors.New("err1")
	require.Equal(t, err1, util.OriginError(err1))

	err2 := errors.Trace(err1)
	require.Equal(t, err1, util.OriginError(err2))

	err3 := errors.Trace(err2)
	require.Equal(t, err1, util.OriginError(err3))
}
