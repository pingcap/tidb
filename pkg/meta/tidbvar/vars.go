// Copyright 2025 PingCAP, Inc.
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

// Package tidbvar contains variable definition related to mysql.tidb table.
// we want to have a unified location to define them to avoid name conflicts.
// currently, mysql.tidb table is mainly used by bootstrap, GC and DXF.
package tidbvar

const (
	// DXFSchedulePauseScaleIn stores info about pause-scale-in flag to let the
	// cluster controller pause scale-in of DXF worker resources as a workaround
	// to the scale-in/schedule conflict issue.
	DXFSchedulePauseScaleIn = "dxf_schedule_pause_scale_in"
)
