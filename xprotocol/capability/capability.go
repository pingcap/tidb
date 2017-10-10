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

package capability

import (
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-mysqlx/Connection"
)

// DecodeCapabilitiesSetMsg decodes capabilities to a map.
func DecodeCapabilitiesSetMsg(msg []byte) (map[string]bool, error) {
	var set Mysqlx_Connection.CapabilitiesSet
	if err := set.Unmarshal(msg); err != nil {
		return nil, errors.Trace(err)
	}
	if set.GetCapabilities() == nil {
		return nil, nil
	}
	caps := set.GetCapabilities().GetCapabilities()
	if caps == nil {
		return nil, nil
	}
	vals := map[string]bool{}
	for _, v := range caps {
		vals[v.GetName()] = v.GetValue().GetScalar().GetVBool()
	}
	return vals, nil
}
