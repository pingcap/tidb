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

package plugin

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConstToString(t *testing.T) {
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

	generalEvents := map[fmt.Stringer]string{
		// GeneralEvent
		Starting:  "STARTING",
		Completed: "COMPLETED",
		Error:     "ERROR",
	}
	require.Equal(t, int(GeneralEventCount), len(generalEvents))
	for key, value := range generalEvents {
		require.Equal(t, value, key.String())
	}
}

func TestGeneralEventString(t *testing.T) {
	for i := 0; i < int(GeneralEventCount); i++ {
		event := GeneralEvent(i)
		str := event.String()
		// event string should be upper case
		require.Equal(t, strings.ToUpper(str), str)
		// GeneralEventFromString return the right event
		got, err := GeneralEventFromString(event.String())
		require.NoError(t, err)
		require.Equal(t, got, event)
	}

	// case insensitive
	event, err := GeneralEventFromString("starting")
	require.NoError(t, err)
	require.Equal(t, Starting, event)

	event, err = GeneralEventFromString("starTing")
	require.NoError(t, err)
	require.Equal(t, Starting, event)

	// invalid string
	_, err = GeneralEventFromString("")
	require.Error(t, err)

	_, err = GeneralEventFromString("xx")
	require.Error(t, err)
}
