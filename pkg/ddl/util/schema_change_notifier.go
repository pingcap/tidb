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

package util

import "github.com/pingcap/tidb/pkg/meta/model"

// SchemaChangeEvent stands for a schema change event. DDL will generate one event or multiple events (only for multi-schema change DDL).
// The caller should check the Type field of SchemaChange and call the corresponding getter function to retrieve the needed information.
type SchemaChangeEvent struct {
	// todo: field and method will be added in the next few pr on demand
	tp model.ActionType
}

// GetType returns the type of the schema change event.
func (s *SchemaChangeEvent) GetType() model.ActionType {
	return s.tp
}
