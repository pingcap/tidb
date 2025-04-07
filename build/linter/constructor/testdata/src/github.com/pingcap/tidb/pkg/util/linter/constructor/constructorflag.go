// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package constructor

// This file is copied from `/util/linter/constructor` to help test

// Constructor is an empty struct to mark the constructor function
// Example:
//
//	type StatementContext struct {
//	    _ constructor.Constructor `ctor:"NewStmtCtx"`
//	}
//
// The linter `constructor` will then ignore all manual construction of the struct in `NewStmtCtx`, and return error
// for all other constructions.
type Constructor struct{}
