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

package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTimerValidate(t *testing.T) {
	// invalid insert
	record := &TimerRecord{}
	err := record.Validate()
	require.EqualError(t, err, "field 'Namespace' should not be empty")

	record.Namespace = "n1"
	err = record.Validate()
	require.EqualError(t, err, "field 'Key' should not be empty")

	record.Key = "k1"
	err = record.Validate()
	require.EqualError(t, err, "field 'SchedPolicyType' should not be empty")

	record.SchedPolicyType = "aa"
	err = record.Validate()
	require.EqualError(t, err, "schedule event configuration is not valid: invalid schedule event type: 'aa'")

	record.SchedPolicyType = SchedEventInterval
	record.SchedPolicyExpr = "1x"
	err = record.Validate()
	require.EqualError(t, err, "schedule event configuration is not valid: invalid schedule event expr '1x': unknown unit x")

	record.SchedPolicyExpr = "1h"
	require.Nil(t, record.Validate())
}
