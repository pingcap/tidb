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

package errdef

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTooManyDataFilesErrorContract(t *testing.T) {
	err := ErrTooManyDataFiles.GenWithStackByArgs(10, 2, 3)
	require.EqualError(t, err, "[GlobalSort:TooManyDataFiles]cannot merge 10 data files with concurrency 2 into at most 3 target files")
	require.Equal(t, "GlobalSort:TooManyDataFiles", string(ErrTooManyDataFiles.RFCCode()))
}
