// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
)

// ResourceGroupRunawaySettings is the runaway settings of the resource group
type ResourceGroupRunawaySettings struct {
	ExecElapsedTimeMs uint64                  `json:"exec_elapsed_time_ms"`
	ProcessedKeys     int64                   `json:"processed_keys"`
	RequestUnit       int64                   `json:"request_unit"`
	Action            model.RunawayActionType `json:"action"`
	SwitchGroupName   string                  `json:"switch_group_name"`
	WatchType         model.RunawayWatchType  `json:"watch_type"`
	WatchDurationMs   int64                   `json:"watch_duration_ms"`
}

// ResourceGroupBackgroundSettings is the background settings of the resource group.
type ResourceGroupBackgroundSettings struct {
	JobTypes          []string `json:"job_types"`
	ResourceUtilLimit uint64   `json:"utilization_limit"`
}

// ResourceGroupSettings is the settings of the resource group
type ResourceGroupSettings struct {
	RURate           uint64                           `json:"ru_per_sec"`
	Priority         uint64                           `json:"priority"`
	CPULimiter       string                           `json:"cpu_limit"`
	IOReadBandwidth  string                           `json:"io_read_bandwidth"`
	IOWriteBandwidth string                           `json:"io_write_bandwidth"`
	BurstLimit       int64                            `json:"burst_limit"`
	Runaway          *ResourceGroupRunawaySettings    `json:"runaway"`
	Background       *ResourceGroupBackgroundSettings `json:"background"`
}

// NewResourceGroupSettings creates a new ResourceGroupSettings.
func NewResourceGroupSettings() *ResourceGroupSettings {
	return &ResourceGroupSettings{
		RURate:           0,
		Priority:         model.MediumPriorityValue,
		CPULimiter:       "",
		IOReadBandwidth:  "",
		IOWriteBandwidth: "",
		BurstLimit:       0,
	}
}

// String implements the fmt.Stringer interface.
func (p *ResourceGroupSettings) String() string {
	sb := new(strings.Builder)
	separatorFn := func() {
		sb.WriteString(", ")
	}
	if p.RURate != 0 {
		writeSettingIntegerToBuilder(sb, "RU_PER_SEC", p.RURate, separatorFn)
	}
	writeSettingItemToBuilder(sb, "PRIORITY="+model.PriorityValueToName(p.Priority), separatorFn)
	if len(p.CPULimiter) > 0 {
		writeSettingStringToBuilder(sb, "CPU", p.CPULimiter, separatorFn)
	}
	if len(p.IOReadBandwidth) > 0 {
		writeSettingStringToBuilder(sb, "IO_READ_BANDWIDTH", p.IOReadBandwidth, separatorFn)
	}
	if len(p.IOWriteBandwidth) > 0 {
		writeSettingStringToBuilder(sb, "IO_WRITE_BANDWIDTH", p.IOWriteBandwidth, separatorFn)
	}
	// Once burst limit is negative, meaning allow burst with unlimit.
	if p.BurstLimit < 0 {
		writeSettingItemToBuilder(sb, "BURSTABLE", separatorFn)
	}
	if p.Runaway != nil {
		fmt.Fprintf(sb, ", QUERY_LIMIT=(")
		// rule settings
		firstParam := true
		if p.Runaway.ExecElapsedTimeMs > 0 {
			fmt.Fprintf(sb, "EXEC_ELAPSED=\"%s\"", (time.Duration(p.Runaway.ExecElapsedTimeMs) * time.Millisecond).String())
			firstParam = false
		}
		if p.Runaway.ProcessedKeys > 0 {
			if !firstParam {
				sb.WriteString(" ")
			}
			fmt.Fprintf(sb, "PROCESSED_KEYS=%d", p.Runaway.ProcessedKeys)
			firstParam = false
		}
		if p.Runaway.RequestUnit > 0 {
			if !firstParam {
				sb.WriteString(" ")
			}
			fmt.Fprintf(sb, "RU=%d", p.Runaway.RequestUnit)
		}
		// action settings
		if p.Runaway.Action == model.RunawayActionSwitchGroup {
			writeSettingItemToBuilder(sb, fmt.Sprintf("ACTION=%s(%s)", p.Runaway.Action.String(), p.Runaway.SwitchGroupName))
		} else {
			writeSettingItemToBuilder(sb, "ACTION="+p.Runaway.Action.String())
		}
		if p.Runaway.WatchType != model.WatchNone {
			writeSettingItemToBuilder(sb, "WATCH="+p.Runaway.WatchType.String())
			if p.Runaway.WatchDurationMs > 0 {
				writeSettingDurationToBuilder(sb, "DURATION", time.Duration(p.Runaway.WatchDurationMs)*time.Millisecond)
			} else {
				writeSettingItemToBuilder(sb, "DURATION=UNLIMITED")
			}
		}
		sb.WriteString(")")
	}
	if p.Background != nil {
		sb.WriteString(", BACKGROUND=(")
		first := true
		if len(p.Background.JobTypes) > 0 {
			fmt.Fprintf(sb, "TASK_TYPES='%s'", strings.Join(p.Background.JobTypes, ","))
			first = false
		}
		if p.Background.ResourceUtilLimit > 0 {
			if !first {
				sb.WriteString(", ")
			}
			fmt.Fprintf(sb, "UTILIZATION_LIMIT=%d", p.Background.ResourceUtilLimit)
		}
		sb.WriteRune(')')
	}

	return sb.String()
}

// Adjust adjusts the resource group settings.
func (p *ResourceGroupSettings) Adjust() {
	// Curretly we only support ru_per_sec sytanx, so BurstLimit(capicity) is always same as ru_per_sec except burstable.
	if p.BurstLimit >= 0 {
		p.BurstLimit = int64(p.RURate)
	}
}

// Clone clones the resource group settings.
func (p *ResourceGroupSettings) Clone() *ResourceGroupSettings {
	cloned := *p
	return &cloned
}

// ResourceGroupInfo is the struct to store the resource group.
type ResourceGroupInfo struct {
	*ResourceGroupSettings
	ID    int64       `json:"id"`
	Name  model.CIStr `json:"name"`
	State SchemaState `json:"state"`
}

// Clone clones the ResourceGroupInfo.
func (p *ResourceGroupInfo) Clone() *ResourceGroupInfo {
	cloned := *p
	cloned.ResourceGroupSettings = p.ResourceGroupSettings.Clone()
	return &cloned
}
