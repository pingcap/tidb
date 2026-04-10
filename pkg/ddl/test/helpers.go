// Copyright 2026 PingCAP, Inc.
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

package ddl_test

// NewJobSubmitterForTest creates a JobSubmitter for testing purposes.
// This is a wrapper around the function from ddl/ddl_test.go.
// Since ddl/ddl_test.go is not compiled when building ddl/test, we create a zero-value JobSubmitter here.
// Tests that rely on the internal state may need to be updated.
