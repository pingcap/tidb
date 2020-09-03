package main

import "testing"

const (
	failTestCount = 4
)

func TestFailpoint(t *testing.T) {
	checker := Check("./testdata/illegal.go", true)
	if len(checker.errList) != failTestCount {
		t.Fatalf("check illegal fail, expect %d errors, only get %d errors", failTestCount, len(checker.errList))
	}
	checker = Check("./testdata/legal.go", true)
	if len(checker.errList) != 0 {
		t.Fatalf("check legal fail, expect no error, only get %d errors", len(checker.errList))
	}
}
