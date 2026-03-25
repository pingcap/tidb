// Copyright 2026 PingCAP, Inc.
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

package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistrationFilterHashInput(t *testing.T) {
	legacy := RegistrationInfo{
		FilterStrings: []string{"db1.*", "db2.tbl"},
	}
	repoV1A := RegistrationInfo{
		FilterStrings: legacy.FilterStrings,
		BackupID:      "0000000000001234",
	}
	repoV1B := RegistrationInfo{
		FilterStrings: legacy.FilterStrings,
		BackupID:      "0000000000005678",
	}

	require.Equal(t, "db1.*"+FilterSeparator+"db2.tbl", registrationFilterHashInput(legacy))
	require.NotEqual(t, registrationFilterHashInput(legacy), registrationFilterHashInput(repoV1A))
	require.NotEqual(t, registrationFilterHashInput(repoV1A), registrationFilterHashInput(repoV1B))
}
