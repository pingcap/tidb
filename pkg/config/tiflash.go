// Copyright 2025 PingCAP, Inc.
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

package config

const (
	// MockASStr is string value for mock AutoScaler.
	MockASStr = "mock"
	// AWSASStr is string value for aws AutoScaler.
	AWSASStr = "aws"
	// GCPASStr is string value for gcp AutoScaler.
	GCPASStr = "gcp"
	// TestASStr is string value for test AutoScaler.
	TestASStr = "test"
	// InvalidASStr is string value for invalid AutoScaler.
	InvalidASStr = "invalid"
)

const (
	// DefAWSAutoScalerAddr is default address for aws AutoScaler.
	DefAWSAutoScalerAddr = "tiflash-autoscale-lb.tiflash-autoscale.svc.cluster.local:8081"
	// DefASStr is default AutoScaler.
	DefASStr = AWSASStr
)

const (
	// MockASType is int value for mock AutoScaler.
	MockASType int = iota
	// AWSASType is int value for aws AutoScaler.
	AWSASType
	// GCPASType is int value for gcp AutoScaler.
	GCPASType
	// TestASType is for local tidb test AutoScaler.
	TestASType
	// InvalidASType is int value for invalid check.
	InvalidASType
)

// IsValidAutoScalerConfig return true if user config of autoscaler type is valid.
func IsValidAutoScalerConfig(typ string) bool {
	t := GetAutoScalerType(typ)
	return t == MockASType || t == AWSASType || t == GCPASType
}

// GetAutoScalerType return topo fetcher type.
func GetAutoScalerType(typ string) int {
	switch typ {
	case MockASStr:
		return MockASType
	case AWSASStr:
		return AWSASType
	case GCPASStr:
		return GCPASType
	case TestASStr:
		return TestASType
	default:
		return InvalidASType
	}
}
