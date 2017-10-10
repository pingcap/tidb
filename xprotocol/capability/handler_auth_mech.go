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
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Connection"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
)

// HandlerAuthMechanisms is read only value handler.
type HandlerAuthMechanisms struct {
	Values []string
}

// IsSupport implements Handler interface.
func (h *HandlerAuthMechanisms) IsSupport() bool {
	return true
}

// GetName implements Handler interface.
func (h *HandlerAuthMechanisms) GetName() string {
	return "authentication.mechanisms"
}

// Get implements Handler interface.
func (h *HandlerAuthMechanisms) Get() *Mysqlx_Connection.Capability {
	meths := []Mysqlx_Datatypes.Any{}
	for _, v := range h.Values {
		meths = append(meths, util.SetString([]byte(v)))
	}
	val := util.SetScalarArray(meths)
	str := h.GetName()
	c := Mysqlx_Connection.Capability{
		Name:  &str,
		Value: &val,
	}
	return &c
}

// Set implements Handler interface.
func (h *HandlerAuthMechanisms) Set(any *Mysqlx_Datatypes.Any) bool {
	return false
}
