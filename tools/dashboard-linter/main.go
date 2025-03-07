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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"unicode"
)

type basicDashboard struct {
	Panels []panel `json:"panels"`
}

type panel struct {
	ID         int     `json:"id"`
	Panels     []panel `json:"panels"`
	Type       string  `json:"type"`
	Title      string  `json:"title"`
	Collapsed  bool    `json:"collapsed"`
	Datasource string  `json:"datasource"`
	GridPos    struct {
		H int `json:"h"`
	} `json:"gridPos"`
}

const rowType = "row"

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
	checkDashboardPanelFields(board)
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

	if idx := indexOfAny(content, []string{".*$tidb_cluster", "$tidb_cluster.*"}); idx != -1 {
		text := string(content[max(idx-150, 0):min(idx+50, len(content))])
		fmt.Printf("It is unnecessary to use pattern match for $tidb_cluster.\n"+
			"See https://github.com/pingcap/tidb/pull/54135 for details.\n Around: %s\n", text)
		os.Exit(1)
	}
}

func indexOfAny(content []byte, substrs []string) int {
	for _, sub := range substrs {
		idx := bytes.Index(content, []byte(sub))
		if idx != -1 {
			return idx
		}
	}
	return -1
}

func checkDashboardPanelFields(board basicDashboard) {
	errors := make([]error, 0, 8)
	for _, p := range board.Panels {
		if errs := checkPanel(p); len(errs) > 0 {
			errors = append(errors, errs...)
		}
	}
	if len(errors) == 0 {
		return
	}

	for _, e := range errors {
		fmt.Println(e)
	}
	os.Exit(1)
}

func checkPanel(p panel) []error {
	errors := make([]error, 0, 8)
	if p.Type == rowType {
		if !p.Collapsed {
			errors = append(errors, fmt.Errorf("row panel %d should be collapsed", p.ID))
		}
		// panel can be nested once(as a row panel)
		for _, sub := range p.Panels {
			if errs := checkPanel(sub); len(errs) > 0 {
				errors = append(errors, errs...)
			}
		}
		return errors
	}
	// tools like TiUP use our grafana dashboard json files as templates.
	if p.Datasource != "${DS_TEST-CLUSTER}" {
		errors = append(errors, fmt.Errorf("panel %d has datasource %s, which is not ${DS_TEST-CLUSTER}", p.ID, p.Datasource))
	}
	if p.GridPos.H != 7 {
		errors = append(errors, fmt.Errorf("we uses 7 as panel height to uniform UI appearance, panel %d has height %d", p.ID, p.GridPos.H))
	}
	if p.Title == "" {
		errors = append(errors, fmt.Errorf("panel %d has empty title", p.ID))
	}
	// we capitalize every word in title, it doesn't follow english grammar, but
	// we already use it in many places, so we follow the existing style, and
	// check it here.
	splits := strings.Split(p.Title, " ")
	for _, w := range splits {
		// ignore some punctuations, like '-'
		if len(w) <= 1 {
			continue
		}
		for _, c := range w {
			if !unicode.IsUpper(c) && !unicode.IsDigit(c) {
				errors = append(errors, fmt.Errorf("panel %d first char of words in title %s should be all be upper case or digit", p.ID, p.Title))
			}
			break
		}
	}
	return errors
}
