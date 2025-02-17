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

package tiflashcompute

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

// DispatchPolicy means different policy to dispatching task to tiflash_compute nods.
type DispatchPolicy int

const (
	// DispatchPolicyRR means dispatching by RoundRobin.
	DispatchPolicyRR DispatchPolicy = iota
	// DispatchPolicyConsistentHash means dispatching by ConsistentHash.
	DispatchPolicyConsistentHash
	// DispatchPolicyInvalid is invalid policy.
	DispatchPolicyInvalid
)

// GetValidDispatchPolicy return all valid policy string.
func GetValidDispatchPolicy() []string {
	return []string{vardef.DispatchPolicyConsistentHashStr, vardef.DispatchPolicyRRStr}
}

// GetDispatchPolicyByStr return corresponding policy.
func GetDispatchPolicyByStr(str string) (DispatchPolicy, error) {
	switch str {
	case vardef.DispatchPolicyConsistentHashStr:
		return DispatchPolicyConsistentHash, nil
	case vardef.DispatchPolicyRRStr:
		return DispatchPolicyRR, nil
	default:
		return DispatchPolicyInvalid,
			errors.Errorf("unexpected tiflash_compute dispatch policy, expect %v, got %v", GetValidDispatchPolicy(), str)
	}
}

// GetDispatchPolicy return corresponding policy string.
func GetDispatchPolicy(p DispatchPolicy) string {
	switch p {
	case DispatchPolicyConsistentHash:
		return vardef.DispatchPolicyConsistentHashStr
	case DispatchPolicyRR:
		return vardef.DispatchPolicyRRStr
	default:
		return vardef.DispatchPolicyInvalidStr
	}
}
