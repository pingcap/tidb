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

package schedule

import (
	"strings"

	"github.com/juju/errors"
)

// OperatorKind is a bit field to identify operator types.
type OperatorKind uint32

// Flags for operators.
const (
	OpLeader    OperatorKind = 1 << iota // Include leader transfer.
	OpRegion                             // Include peer movement.
	OpAdmin                              // Initiated by admin.
	OpHotRegion                          // Initiated by hot region scheduler.
	OpAdjacent                           // Initiated by adjacent region scheduler.
	OpReplica                            // Initiated by replica checkers.
	OpBalance                            // Initiated by balancers.
	opMax
)

var flagToName = map[OperatorKind]string{
	OpLeader:    "leader",
	OpRegion:    "region",
	OpAdmin:     "admin",
	OpHotRegion: "hotRegion",
	OpAdjacent:  "adjacent",
	OpReplica:   "replica",
	OpBalance:   "balance",
}

var nameToFlag = map[string]OperatorKind{
	"leader":    OpLeader,
	"region":    OpRegion,
	"admin":     OpAdmin,
	"hotRegion": OpHotRegion,
	"adjacent":  OpAdjacent,
	"replica":   OpReplica,
	"balance":   OpBalance,
}

func (k OperatorKind) String() string {
	var flagNames []string
	for flag := OperatorKind(1); flag < opMax; flag <<= 1 {
		if k&flag != 0 {
			flagNames = append(flagNames, flagToName[flag])
		}
	}
	if len(flagNames) == 0 {
		return "unknown"
	}
	return strings.Join(flagNames, ",")
}

// ParseOperatorKind converts string (flag name list concat by ',') to OperatorKind.
func ParseOperatorKind(str string) (OperatorKind, error) {
	var k OperatorKind
	for _, flagName := range strings.Split(str, ",") {
		flag, ok := nameToFlag[flagName]
		if !ok {
			return 0, errors.Errorf("unknown flag name: %s", flagName)
		}
		k |= flag
	}
	return k, nil

}
