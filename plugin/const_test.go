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
// See the License for the specific language governing permissions and
// limitations under the License.

package plugin

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConstToString(t *testing.T) {
	t.Parallel()
	kinds := map[fmt.Stringer]string{
		Audit:                     "Audit",
		Authentication:            "Authentication",
		Schema:                    "Schema",
		Daemon:                    "Daemon",
		Uninitialized:             "Uninitialized",
		Ready:                     "Ready",
		Dying:                     "Dying",
		Disable:                   "Disable",
		Connected:                 "Connected",
		Disconnect:                "Disconnect",
		ChangeUser:                "ChangeUser",
		PreAuth:                   "PreAuth",
		Reject:                    "Reject",
		ConnectionEvent(byte(15)): "",
	}
	for key, value := range kinds {
		require.Equal(t, value, key.String())
	}
}
