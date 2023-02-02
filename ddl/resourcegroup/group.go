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
	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/parser/model"
	"k8s.io/apimachinery/pkg/api/resource"
)

const maxGroupNameLength = 32

// NewGroupFromOptions creates a new resource group from the given options.
func NewGroupFromOptions(groupName string, options *model.ResourceGroupSettings) (*rmpb.ResourceGroup, error) {
	if options == nil {
		return nil, ErrInvalidGroupSettings
	}
	if len(groupName) > maxGroupNameLength {
		return nil, ErrTooLongResourceGroupName
	}
	group := &rmpb.ResourceGroup{
		Name: groupName,
	}
	var isRUMode bool
	if options.RURate > 0 {
		isRUMode = true
		group.Mode = rmpb.GroupMode_RUMode
		group.RUSettings = &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: options.RURate,
				},
			},
		}
	}
	if len(options.CPULimiter) > 0 || len(options.IOReadBandwidth) > 0 || len(options.IOWriteBandwidth) > 0 {
		if isRUMode {
			return nil, ErrInvalidResourceGroupDuplicatedMode
		}
		parseF := func(s string, scale resource.Scale) (uint64, error) {
			if len(s) == 0 {
				return 0, nil
			}
			q, err := resource.ParseQuantity(s)
			if err != nil {
				return 0, err
			}
			return uint64(q.ScaledValue(scale)), nil
		}
		cpuRate, err := parseF(options.CPULimiter, resource.Milli)
		if err != nil {
			return nil, errors.Annotate(ErrInvalidResourceGroupFormat, err.Error())
		}
		ioReadRate, err := parseF(options.IOReadBandwidth, resource.Scale(0))
		if err != nil {
			return nil, errors.Annotate(ErrInvalidResourceGroupFormat, err.Error())
		}
		ioWriteRate, err := parseF(options.IOWriteBandwidth, resource.Scale(0))
		if err != nil {
			return nil, errors.Annotate(ErrInvalidResourceGroupFormat, err.Error())
		}

		group.Mode = rmpb.GroupMode_RawMode
		group.RawResourceSettings = &rmpb.GroupRawResourceSettings{
			Cpu: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: cpuRate,
				},
			},
			IoRead: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: ioReadRate,
				},
			},
			IoWrite: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: ioWriteRate,
				},
			},
		}
		return group, nil
	}
	if isRUMode {
		return group, nil
	}
	return nil, ErrUnknownResourceGroupMode
}
