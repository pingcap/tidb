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
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/precheck"
	"github.com/stretchr/testify/require"
)

func TestPreCheckCollector(t *testing.T) {
	c := newPreCheckCollector()
	require.True(t, c.success())

	c.fail(precheck.CheckTargetTableEmpty, "target table is not empty")
	require.False(t, c.success())
}
