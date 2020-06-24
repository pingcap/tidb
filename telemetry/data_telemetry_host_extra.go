// Copyright 2020 PingCAP, Inc.
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

package telemetry

import (
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/host"
)

// Some information that is not included in current system tables. There is no easy way to retrieve information
// of all hosts, so current host only.
type telemetryHostExtraInfo struct {
	CPUFlags             []string `json:"cpuFlags,omitempty"`             // ex: fpu, vme, de, ...
	CPUModelName         string   `json:"cpuModelName,omitempty"`         // ex: Intel(R) Core(TM) i7-2640M CPU @ 2.80GHz
	OS                   string   `json:"os,omitempty"`                   // ex: freebsd, linux
	Platform             string   `json:"platform,omitempty"`             // ex: ubuntu, linuxmint
	PlatformFamily       string   `json:"platformFamily,omitempty"`       // ex: debian, rhel
	PlatformVersion      string   `json:"platformVersion,omitempty"`      // version of the complete OS
	KernelVersion        string   `json:"kernelVersion,omitempty"`        // version of the OS kernel (if available)
	KernelArch           string   `json:"kernelArch,omitempty"`           // native cpu architecture queried at runtime, as returned by `uname -m` or empty string in case of error
	VirtualizationSystem string   `json:"virtualizationSystem,omitempty"` // ex: kvm
	VirtualizationRole   string   `json:"virtualizationRole,omitempty"`   // guest or host
}

func getTelemetryHostExtraInfo() *telemetryHostExtraInfo {
	r := &telemetryHostExtraInfo{}

	cpusInfo, err := cpu.Info()
	if err == nil && len(cpusInfo) > 0 {
		r.CPUFlags = cpusInfo[0].Flags
		r.CPUModelName = cpusInfo[0].ModelName
	}

	hostInfo, err := host.Info()
	if err == nil && hostInfo != nil {
		r.OS = hostInfo.OS
		r.Platform = hostInfo.Platform
		r.PlatformFamily = hostInfo.PlatformFamily
		r.PlatformVersion = hostInfo.PlatformVersion
		r.KernelVersion = hostInfo.KernelVersion
		r.KernelArch = hostInfo.KernelArch
		r.VirtualizationSystem = hostInfo.VirtualizationSystem
		r.VirtualizationRole = hostInfo.VirtualizationRole
	}

	return r
}
