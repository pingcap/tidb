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

package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogicalJobImporterGetLastInsertID(t *testing.T) {
	importer := &logicalJobImporter{
		encodeWorkers: []*encodeWorker{
			{InsertValues: &InsertValues{lastInsertID: 1}},
			{InsertValues: &InsertValues{lastInsertID: 2}},
		},
	}
	require.Equal(t, uint64(1), importer.getLastInsertID())

	importer = &logicalJobImporter{
		encodeWorkers: []*encodeWorker{
			{InsertValues: &InsertValues{lastInsertID: 2}},
			{InsertValues: &InsertValues{lastInsertID: 1}},
		},
	}
	require.Equal(t, uint64(1), importer.getLastInsertID())

	importer = &logicalJobImporter{
		encodeWorkers: []*encodeWorker{
			{InsertValues: &InsertValues{lastInsertID: 44}},
			{InsertValues: &InsertValues{lastInsertID: 0}},
		},
		commitWorkers: []*commitWorker{
			{InsertValues: &InsertValues{lastInsertID: 22}},
			{InsertValues: &InsertValues{lastInsertID: 11}},
		},
	}
	require.Equal(t, uint64(11), importer.getLastInsertID())
}
