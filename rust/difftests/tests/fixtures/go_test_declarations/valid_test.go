// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package fixture

import "testing"

// func TestCommentedLine(t *testing.T) {}
var text = "func TestCommentedString(t *testing.T) {}"
/* func TestCommentedBlock(t *testing.T) {} */

func TestLive(t *testing.T) {}
func BenchmarkLive(b *testing.B) {}
func FuzzLive(f *testing.F) {}
func ExampleLive() {}
func TestMain(m *testing.M) {}

func TestInvalid() {}
func BenchmarkInvalid(t *testing.T) {}
func FuzzInvalid(f testing.F) {}
func ExampleInvalid(value int) {}

type suite struct{}

func (suite) TestMethod(t *testing.T) {}
func (suite) SetUpTest() {}
