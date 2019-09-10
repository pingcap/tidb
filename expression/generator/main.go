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

package main

import (
	"bytes"
	"flag"
	"go/format"
	"io/ioutil"
	"log"
	"path/filepath"
	"text/template"
)

var outputDir = flag.String("outputdir", "expression/", "")

type typeContext struct {
	// Describe the name of "github.com/pingcap/tidb/types".ET{{ .ETName }}
	ETName string
	// Describe the name of "github.com/pingcap/tidb/expression".VecExpr.VecEval{{ .TypeName }}
	// If undefined, it's same as ETName.
	TypeName string
	// Describe the name of "github.com/pingcap/tidb/util/chunk".*Column.Append{{ .TypeNameInColumn }},
	// Resize{{ .TypeNameInColumn }}, Reserve{{ .TypeNameInColumn }}, Get{{ .TypeNameInColumn }} and
	// {{ .TypeNameInColumn }}s.
	// If undefined, it's same as TypeName.
	TypeNameInColumn string
	// Same as "github.com/pingcap/tidb/util/chunk".getFixedLen()
	Fixed bool
}

var typesMap = []typeContext{
	{ETName: "Int", TypeNameInColumn: "Int64", Fixed: true},
	{ETName: "Real", TypeNameInColumn: "Float64", Fixed: true},
	{ETName: "Decimal", Fixed: true},
	{ETName: "String", Fixed: false},
	{ETName: "Datetime", TypeName: "Time", Fixed: true},
	{ETName: "Duration", TypeNameInColumn: "GoDuration", Fixed: true},
	{ETName: "Json", TypeName: "JSON", Fixed: false},
}

func generateDotGo(fileName string, imports string, types []typeContext, tmpls ...*template.Template) error {
	w := new(bytes.Buffer)
	w.WriteString(header)
	w.WriteString(newLine)
	w.WriteString(imports)
	for _, tmpl := range tmpls {
		for _, ctx := range types {
			if ctx.TypeName == "" {
				ctx.TypeName = ctx.ETName
			}
			if ctx.TypeNameInColumn == "" {
				ctx.TypeNameInColumn = ctx.TypeName
			}
			err := tmpl.Execute(w, ctx)
			if err != nil {
				return err
			}
		}
	}
	dst, err := format.Source(w.Bytes())
	if err != nil {
		return err
	}
	return ioutil.WriteFile(fileName, dst, 0644)
}

func generateTestDotGo(fileName string, types []typeContext) error {
	w := new(bytes.Buffer)
	w.WriteString(header)
	err := builtinControlVecTest.Execute(w, types)
	if err != nil {
		return err
	}
	dst, err := format.Source(w.Bytes())
	if err != nil {
		return err
	}
	return ioutil.WriteFile(fileName, dst, 0644)
}

// generateOneFile generate one xxx.go file and the associated xxx_test.go file.
func generateOneFile(fileNamePrefix string, imports string, types []typeContext,
	tmpls ...*template.Template) (err error) {

	err = generateDotGo(fileNamePrefix+".go", imports, types,
		tmpls...,
	)
	if err != nil {
		return
	}
	err = generateTestDotGo(fileNamePrefix+"_test.go", types)
	return
}

func main() {
	flag.Parse()
	var err error
	err = generateOneFile(filepath.Join(*outputDir, "builtin_control_vec"), builtinControlImports, typesMap,
		// Add to the list if the file has more template to execute.
		builtinIfVec,
	)
	if err != nil {
		log.Fatalln(err)
	}
}
