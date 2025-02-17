// Copyright 2022 PingCAP, Inc.
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
	"fmt"
	"os"

	"github.com/google/skylark/syntax"
)

// DownloadableArtifact represents a single URL/SHA256 pair.
type DownloadableArtifact struct {
	URL     []string
	Sha256  string
	Sum     string
	Version string
}

// ListArtifactsInDepsBzl parses the DEPS.bzl file and returns a map of repo name ->
// DownloadableArtifact capturing what's in that repo.
func ListArtifactsInDepsBzl(depsBzl string) (map[string]DownloadableArtifact, error) {
	in, err := os.Open(depsBzl)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	return downloadableArtifactsFromDepsBzl(in)
}

// GetFunctionFromCall returns the name of the function called for the given
// call expression, or the empty string if the function being called is not a
// normal identifier.
func GetFunctionFromCall(call *syntax.CallExpr) string {
	fn, err := ExpectIdent(call.Fn)
	if err != nil {
		return ""
	}
	return fn
}

func downloadableArtifactsFromDepsBzl(in any) (map[string]DownloadableArtifact, error) {
	parsed, err := syntax.Parse("DEPS.bzl", in, 0)
	if err != nil {
		return nil, err
	}
	for _, stmt := range parsed.Stmts {
		switch s := stmt.(type) {
		case *syntax.DefStmt:
			if s.Name.Name == "go_deps" {
				return downloadableArtifactsFromGoDeps(s)
			}
		default:
			continue
		}
	}
	return nil, fmt.Errorf("could not find go_deps function in DEPS.bzl")
}

func downloadableArtifactsFromGoDeps(def *syntax.DefStmt) (map[string]DownloadableArtifact, error) {
	ret := make(map[string]DownloadableArtifact)
	for _, stmt := range def.Function.Body {
		switch s := stmt.(type) {
		case *syntax.ExprStmt:
			switch x := s.X.(type) {
			case *syntax.CallExpr:
				fn := GetFunctionFromCall(x)
				if fn != "go_repository" {
					return nil, fmt.Errorf("expected go_repository, got %s", fn)
				}
				name, mirror, err := maybeGetDownloadableArtifact(x)
				if err != nil {
					return nil, err
				}
				if name != "" {
					ret[name] = mirror
				}
			default:
				return nil, fmt.Errorf("unexpected expression in DEPS.bzl: %v", x)
			}
		}
	}
	return ret, nil
}

func maybeGetDownloadableArtifact(call *syntax.CallExpr) (string, DownloadableArtifact, error) {
	var name, sha256, version, sum string
	var url []string
	for _, arg := range call.Args {
		switch bx := arg.(type) {
		case *syntax.BinaryExpr:
			if bx.Op != syntax.EQ {
				return "", DownloadableArtifact{}, fmt.Errorf("unexpected binary expression Op %d", bx.Op)
			}
			kwarg, err := ExpectIdent(bx.X)
			if err != nil {
				return "", DownloadableArtifact{}, err
			}
			if kwarg == "name" {
				name, err = ExpectLiteralString(bx.Y)
				if err != nil {
					return "", DownloadableArtifact{}, err
				}
			}
			if kwarg == "sha256" {
				sha256, err = ExpectLiteralString(bx.Y)
				if err != nil {
					return "", DownloadableArtifact{}, err
				}
			}
			if kwarg == "version" {
				version, err = ExpectLiteralString(bx.Y)
				if err != nil {
					return "", DownloadableArtifact{}, err
				}
			}
			if kwarg == "sum" {
				sum, err = ExpectLiteralString(bx.Y)
				if err != nil {
					return "", DownloadableArtifact{}, err
				}
			}
		default:
			return "", DownloadableArtifact{}, fmt.Errorf("unexpected expression in DEPS.bzl: %v", bx)
		}
	}
	if len(url) != 0 {
		return name, DownloadableArtifact{URL: url, Sha256: sha256, Sum: sum, Version: version}, nil
	}
	return "", DownloadableArtifact{}, nil
}

// ExpectIdent returns an identifier string or an error if this Expr is not an
// identifier.
func ExpectIdent(x syntax.Expr) (string, error) {
	switch i := x.(type) {
	case *syntax.Ident:
		return i.Name, nil
	default:
		return "", fmt.Errorf("expected identifier, got %v of type %T", i, i)
	}
}

// ExpectLiteralString returns the string represented by this Expr or an error
// if the Expr is not a literal string.
func ExpectLiteralString(x syntax.Expr) (string, error) {
	switch l := x.(type) {
	case *syntax.Literal:
		switch s := l.Value.(type) {
		case string:
			return s, nil
		default:
			return "", fmt.Errorf("expected literal string, got %v of type %T", s, s)
		}
	default:
		return "", fmt.Errorf("expected literal string, got %v of type %T", l, l)
	}
}

// ExpectSingletonStringList returns the string in this list or an error if this
// Expr is not a string list of length 1.
func ExpectSingletonStringList(x syntax.Expr) ([]string, error) {
	switch l := x.(type) {
	case *syntax.ListExpr:
		result := make([]string, 0, len(l.List))
		for _, list := range l.List {
			l, err := ExpectLiteralString(list)
			if err != nil {
				return nil, err
			}
			result = append(result, l)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected list of strings, got %v of type %T", l, l)
	}
}
