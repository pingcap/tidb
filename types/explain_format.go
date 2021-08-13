// Copyright 2021 PingCAP, Inc.
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

package types

var (
	// ExplainFormatBrief is the same as ExplainFormatRow, but it ignores explain ID suffix.
	ExplainFormatBrief = "brief"
	// ExplainFormatDOT indicates that using DotGraph to explain.
	ExplainFormatDOT = "dot"
	// ExplainFormatHint is to show hint information.
	ExplainFormatHint = "hint"
	// ExplainFormatJSON displays the information in JSON format.
	ExplainFormatJSON = "json"
	// ExplainFormatROW presents the output in tabular format.
	ExplainFormatROW = "row"
	// ExplainFormatVerbose display additional information regarding the plan.
	ExplainFormatVerbose = "verbose"
	// ExplainFormatTraditional is the same as ExplainFormatROW.
	ExplainFormatTraditional = "traditional"

	// ExplainFormats stores the valid formats for explain statement, used by validator.
	ExplainFormats = []string{
		ExplainFormatBrief,
		ExplainFormatDOT,
		ExplainFormatHint,
		ExplainFormatJSON,
		ExplainFormatROW,
		ExplainFormatVerbose,
		ExplainFormatTraditional,
	}
)
