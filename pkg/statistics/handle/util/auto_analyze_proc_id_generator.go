// Copyright 2023 PingCAP, Inc.
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

package util

// AutoAnalyzeProcIDGenerator is used to generate auto analyze proc ID.
type AutoAnalyzeProcIDGenerator interface {
	// AutoAnalyzeProcID generates an analyze ID.
	AutoAnalyzeProcID() uint64
}

var _ AutoAnalyzeProcIDGenerator = (*generator)(nil)

type generator struct {
	// autoAnalyzeProcIDGetter is used to generate auto analyze ID.
	autoAnalyzeProcIDGetter func() uint64
}

// NewGenerator creates a new Generator.
func NewGenerator(autoAnalyzeProcIDGetter func() uint64) AutoAnalyzeProcIDGenerator {
	return &generator{
		autoAnalyzeProcIDGetter: autoAnalyzeProcIDGetter,
	}
}

// AutoAnalyzeProcID implements AutoAnalyzeProcIDGenerator.
func (g *generator) AutoAnalyzeProcID() uint64 {
	return g.autoAnalyzeProcIDGetter()
}
