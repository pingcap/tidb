// Copyright 2022 PingCAP, Inc.
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

package resourcegroup

import (
	"fmt"
	"testing"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

func TestNewResourceGroupFromOptions(t *testing.T) {
	type TestCase struct {
		name      string
		groupName string
		input     *model.ResourceGroupSettings
		output    *rmpb.ResourceGroup
		err       error
	}
	var tests []TestCase
	groupName := "test"
	tests = append(tests, TestCase{
		name:  "empty 1",
		input: &model.ResourceGroupSettings{},
		err:   ErrUnknownResourceGroupMode,
	})

	tests = append(tests, TestCase{
		name:  "empty 2",
		input: nil,
		err:   ErrInvalidGroupSettings,
	})

	tests = append(tests, TestCase{
		name: "normal case: ru case 1",
		input: &model.ResourceGroupSettings{
			RURate: 2000,
		},
		output: &rmpb.ResourceGroup{
			Name: groupName,
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 2000}},
			},
		},
	})

	tests = append(tests, TestCase{
		name: "normal case: ru case 2",
		input: &model.ResourceGroupSettings{
			RURate: 5000,
		},
		output: &rmpb.ResourceGroup{
			Name: groupName,
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 5000}},
			},
		},
	})

	tests = append(tests, TestCase{
		name: "error case: native case 1",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8",
			IOReadBandwidth:  "3000MB/s",
			IOWriteBandwidth: "3000Mi",
		},
		err: ErrUnknownResourceGroupMode,
	})

	tests = append(tests, TestCase{
		name: "error case: native case 2",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8c",
			IOReadBandwidth:  "3000Mi",
			IOWriteBandwidth: "3000Mi",
		},
		err: ErrUnknownResourceGroupMode,
	})

	tests = append(tests, TestCase{
		name: "error case: native case 3",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8",
			IOReadBandwidth:  "3000G",
			IOWriteBandwidth: "3000MB",
		},
		err: ErrUnknownResourceGroupMode,
	})

	tests = append(tests, TestCase{
		name: "error case: duplicated mode",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8",
			IOReadBandwidth:  "3000Mi",
			IOWriteBandwidth: "3000Mi",
			RURate:           1000,
		},
		err: ErrInvalidResourceGroupDuplicatedMode,
	})

	tests = append(tests, TestCase{
		name:      "error case: duplicated mode",
		groupName: "test_group_too_looooooooooooooooooooooooooooooooooooooooooooooooong",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8",
			IOReadBandwidth:  "3000Mi",
			IOWriteBandwidth: "3000Mi",
			RURate:           1000,
		},
		err: ErrTooLongResourceGroupName,
	})

	for _, test := range tests {
		name := groupName
		if len(test.groupName) > 0 {
			name = test.groupName
		}
		group, err := NewGroupFromOptions(name, test.input)
		comment := fmt.Sprintf("[%s]\nerr1 %s\nerr2 %s", test.name, err, test.err)
		if test.err != nil {
			require.ErrorIs(t, err, test.err, comment)
		} else {
			require.NoError(t, err, comment)
			require.Equal(t, test.output, group)
		}
	}
}
