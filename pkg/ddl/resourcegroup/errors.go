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
)

var (
	// ErrInvalidGroupSettings is from group.go.
	ErrInvalidGroupSettings = errors.New("invalid group settings")
	// ErrTooLongResourceGroupName is from group.go.
	ErrTooLongResourceGroupName = errors.New("resource group name too long")
	// ErrInvalidResourceGroupFormat is from group.go.
	ErrInvalidResourceGroupFormat = errors.New("group settings with invalid format")
	// ErrInvalidResourceGroupDuplicatedMode is from group.go.
	ErrInvalidResourceGroupDuplicatedMode = errors.New("cannot set RU mode and Raw mode options at the same time")
	// ErrUnknownResourceGroupMode is from group.go.
	ErrUnknownResourceGroupMode = errors.New("unknown resource group mode")
	// ErrDroppingInternalResourceGroup is from group.go
	ErrDroppingInternalResourceGroup = errors.New("can't drop reserved resource group")
	// ErrInvalidResourceGroupRunawayExecElapsedTime is from group.go.
	ErrInvalidResourceGroupRunawayExecElapsedTime = errors.New("invalid exec elapsed time")
	// ErrUnknownResourceGroupRunawayAction is from group.go.
	ErrUnknownResourceGroupRunawayAction = errors.New("unknown resource group runaway action")
)
