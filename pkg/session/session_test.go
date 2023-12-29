// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainsTarget(t *testing.T) {
	s := "create table sbtest1 xxx"
	require.True(t, containsTarget(s))
	s = "create table sbtest1123 xxx"
	require.False(t, containsTarget(s))
	s = "DELETE FROM sbtest5 WHERE id=?"
	require.False(t, containsTarget(s))
	s = "SELECT c FROM sbtest36 WHERE id=?"
	require.False(t, containsTarget(s))
	s = "SELECT c FROM sbtest1 WHERE id=?"
	require.True(t, containsTarget(s))
	s = "DELETE FROM sbtest1"
	require.True(t, containsTarget(s))
}
