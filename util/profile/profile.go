// Copyright 2019 PingCAP, Inc.
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

package profile

import (
	"bytes"
	"fmt"
	"io"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/google/pprof/graph"
	"github.com/google/pprof/measurement"
	"github.com/google/pprof/profile"
	"github.com/google/pprof/report"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/texttree"
)

// CPUProfileInterval represents the duration of sampling CPU
var CPUProfileInterval = 30 * time.Second

// Collector is used to collect the profile results
type Collector struct {
	Rows [][]types.Datum
}

type perfNode struct {
	Name      string
	Location  string
	Cum       int64
	CumFormat string
	Percent   string
	Children  []*perfNode
}

func (c *Collector) collect(node *perfNode, depth int64, indent string, rootChild int, parentCur int64, isLastChild bool) {
	row := types.MakeDatums(
		texttree.PrettyIdentifier(node.Name, indent, isLastChild),
		node.Percent,
		strings.TrimSpace(measurement.Percentage(node.Cum, parentCur)),
		rootChild,
		depth,
		node.Location,
	)
	c.Rows = append(c.Rows, row)

	indent4Child := texttree.Indent4Child(indent, isLastChild)
	for i, child := range node.Children {
		rc := rootChild
		if depth == 0 {
			rc = i + 1
		}
		c.collect(child, depth+1, indent4Child, rc, node.Cum, i == len(node.Children)-1)
	}
}

func (c *Collector) profileReaderToDatums(f io.Reader) ([][]types.Datum, error) {
	p, err := profile.Parse(f)
	if err != nil {
		return nil, err
	}
	return c.profileToDatums(p)
}

func (c *Collector) profileToDatums(p *profile.Profile) ([][]types.Datum, error) {
	err := p.Aggregate(true, true, true, true, true)
	if err != nil {
		return nil, err
	}
	rpt := report.NewDefault(p, report.Options{
		OutputFormat: report.Dot,
		CallTree:     true,
	})
	g, config := report.GetDOT(rpt)
	var nodes []*perfNode
	nroots := 0
	rootValue := int64(0)
	nodeArr := []string{}
	nodeMap := map[*graph.Node]*perfNode{}
	// Make all nodes and the map, collect the roots.
	for _, n := range g.Nodes {
		v := n.CumValue()
		node := &perfNode{
			Name:      n.Info.Name,
			Location:  fmt.Sprintf("%s:%d", n.Info.File, n.Info.Lineno),
			Cum:       v,
			CumFormat: config.FormatValue(v),
			Percent:   strings.TrimSpace(measurement.Percentage(v, config.Total)),
		}
		nodes = append(nodes, node)
		if len(n.In) == 0 {
			nodes[nroots], nodes[len(nodes)-1] = nodes[len(nodes)-1], nodes[nroots]
			nroots++
			rootValue += v
		}
		nodeMap[n] = node
		// Get all node names into an array.
		nodeArr = append(nodeArr, n.Info.Name)
	}
	// Populate the child links.
	for _, n := range g.Nodes {
		node := nodeMap[n]
		for child := range n.Out {
			node.Children = append(node.Children, nodeMap[child])
		}
	}

	rootNode := &perfNode{
		Name:      "root",
		Location:  "root",
		Cum:       rootValue,
		CumFormat: config.FormatValue(rootValue),
		Percent:   strings.TrimSpace(measurement.Percentage(rootValue, config.Total)),
		Children:  nodes[0:nroots],
	}

	c.collect(rootNode, 0, "", 0, config.Total, len(rootNode.Children) == 0)
	return c.Rows, nil
}

// cpuProfileGraph returns the CPU profile flamegraph which is organized by tree form
func (c *Collector) cpuProfileGraph() ([][]types.Datum, error) {
	buffer := &bytes.Buffer{}
	if err := pprof.StartCPUProfile(buffer); err != nil {
		panic(err)
	}
	time.Sleep(CPUProfileInterval)
	pprof.StopCPUProfile()
	return c.profileReaderToDatums(buffer)
}

// ProfileGraph returns the CPU/memory/mutex/allocs/block profile flamegraph which is organized by tree form
func (c *Collector) ProfileGraph(name string) ([][]types.Datum, error) {
	if strings.ToLower(strings.TrimSpace(name)) == "cpu" {
		return c.cpuProfileGraph()
	}

	p := pprof.Lookup(name)
	if p == nil {
		return nil, errors.Errorf("cannot retrieve %s profile", name)
	}
	buffer := &bytes.Buffer{}
	if err := p.WriteTo(buffer, 0); err != nil {
		return nil, err
	}
	return c.profileReaderToDatums(buffer)
}

// Goroutines returns the groutine list which alive in runtime
func (c *Collector) Goroutines() ([][]types.Datum, error) {
	p := pprof.Lookup("goroutine")
	if p == nil {
		return nil, errors.Errorf("cannot retrieve goroutine profile")
	}

	buffer := bytes.Buffer{}
	err := p.WriteTo(&buffer, 2)
	if err != nil {
		return nil, err
	}

	goroutines := strings.Split(buffer.String(), "\n\n")
	var rows [][]types.Datum
	for _, goroutine := range goroutines {
		colIndex := strings.Index(goroutine, ":")
		if colIndex < 0 {
			return nil, errors.New("goroutine incompatible with current go version")
		}

		headers := strings.SplitN(strings.TrimSpace(goroutine[len("goroutine")+1:colIndex]), " ", 2)
		if len(headers) != 2 {
			return nil, errors.Errorf("incompatible goroutine headers: %s", goroutine[len("goroutine")+1:colIndex])
		}
		id, err := strconv.Atoi(strings.TrimSpace(headers[0]))
		if err != nil {
			return nil, errors.Annotatef(err, "invalid goroutine id: %s", headers[0])
		}
		state := strings.Trim(headers[1], "[]")
		stack := strings.Split(strings.TrimSpace(goroutine[colIndex+1:]), "\n")
		for i := 0; i < len(stack)/2; i++ {
			fn := stack[i*2]
			loc := stack[i*2+1]
			var identifier string
			if i == 0 {
				identifier = fn
			} else if i == len(stack)/2-1 {
				identifier = string(texttree.TreeLastNode) + string(texttree.TreeNodeIdentifier) + fn
			} else {
				identifier = string(texttree.TreeMiddleNode) + string(texttree.TreeNodeIdentifier) + fn
			}
			rows = append(rows, types.MakeDatums(
				identifier,
				id,
				state,
				strings.TrimSpace(loc),
			))
		}
	}
	return rows, nil
}
