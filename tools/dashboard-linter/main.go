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

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
)

type basicDashboard struct {
	Panels []panel `json:"panels"`
}

type panel struct {
	ID     int     `json:"id"`
	Panels []panel `json:"panels"`
}

// a small linter to check if there are duplicate panel IDs in a dashboard json file.
// grafana do have one linter https://github.com/grafana/dashboard-linter, but seems
// it does not have rules to check duplicate panel IDs.
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: dashboard-linter <path-to-dashboard-json>")
		os.Exit(1)
	}
	fileName := os.Args[1]
	content, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	board := basicDashboard{}
	if err = json.Unmarshal(content, &board); err != nil {
		panic(err)
	}
	allIDs := make(map[int]int, 1024)
	for _, p := range board.Panels {
		allIDs[p.ID]++
		// panel can be nested once(as a row panel)
		for _, sub := range p.Panels {
			allIDs[sub.ID]++
		}
	}
	duplicateIDs := make(map[int]int, 8)
	usedIDs := make([]int, 0, len(allIDs))
	for id, count := range allIDs {
		if count > 1 {
			duplicateIDs[id] = count
		}
		usedIDs = append(usedIDs, id)
	}
	slices.Sort(usedIDs)
	availableRange := make([]string, 0, 100)
	if len(usedIDs) > 0 {
		nextID := usedIDs[0] + 1
		for i := 1; i < len(usedIDs); i++ {
			if usedIDs[i]-nextID > 1 {
				availableRange = append(availableRange, fmt.Sprintf("[%d, %d)", nextID, usedIDs[i]))
			} else if usedIDs[i]-nextID == 1 {
				availableRange = append(availableRange, fmt.Sprintf("%d", nextID))
			}
			nextID = usedIDs[i] + 1
		}
		availableRange = append(availableRange, fmt.Sprintf("[%d, ∞)", nextID))
	}
	if len(duplicateIDs) > 0 {
		fmt.Printf("Duplicate panel IDs found(id:count map) in file %s: %v\navailable panel ID range: %v\n",
			fileName, duplicateIDs, availableRange)
		os.Exit(1)
	}
}
