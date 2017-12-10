// Copyright 2017 PingCAP, Inc.
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

package core

// PriorityLevel lower level means higher priority
type PriorityLevel int

// Built-in priority level
const (
	HighPriority PriorityLevel = iota
	NormalPriority
	LowPriority
)

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	// UnKnownKind indicates the unknown kind resource
	UnKnownKind ResourceKind = iota
	// AdminKind indicates that specify by admin
	AdminKind
	// LeaderKind indicates the leader kind resource
	LeaderKind
	// RegionKind indicates the region kind resource
	RegionKind
	// HotRegionKind indicates the hot region kind resource
	HotRegionKind
	// AdjacentLeaderKind indicates the adjacent resource
	AdjacentLeaderKind
	// AdjacentPeerKind indicates the adjacent resource
	AdjacentPeerKind
	// OtherKind indicates the other kind resource
	OtherKind
)

var resourceKindToName = map[ResourceKind]string{
	0: "unknown",
	1: "admin",
	2: "leader",
	3: "region",
	4: "hot",
	5: "adjacent-leader",
	6: "adjacent-peer",
	7: "other",
}

var resourceNameToValue = map[string]ResourceKind{
	"unknown":         UnKnownKind,
	"admin":           AdminKind,
	"leader":          LeaderKind,
	"region":          RegionKind,
	"hot":             HotRegionKind,
	"adjacent-leader": AdjacentLeaderKind,
	"adjacent-peer":   AdjacentPeerKind,
	"other":           OtherKind,
}

func (k ResourceKind) String() string {
	s, ok := resourceKindToName[k]
	if ok {
		return s
	}
	return resourceKindToName[UnKnownKind]
}

// ParseResourceKind convert string to ResourceKind
func ParseResourceKind(name string) ResourceKind {
	k, ok := resourceNameToValue[name]
	if ok {
		return k
	}
	return UnKnownKind
}
