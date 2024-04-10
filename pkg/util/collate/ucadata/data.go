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

package ucadata

const (
	// LongRune8 means the rune has at most 8 collation elements
	LongRune8 = 0xFFFD
)

//go:generate go run ./generator/ -- unicode_0900_ai_ci_data_generated.go
//go:generate go run ./generator/ -- unicode_ci_data_generated.go
