// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package servicescope

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScope(t *testing.T) {
	require.NoError(t, CheckServiceScope("789z-_"))
	require.Error(t, CheckServiceScope("789z-_)"))
	require.Error(t, CheckServiceScope("78912345678982u7389217897238917389127893781278937128973812728397281378932179837"))
	require.NoError(t, CheckServiceScope("scope1"))
	require.NoError(t, CheckServiceScope(""))
	require.NoError(t, CheckServiceScope("-----"))
}
