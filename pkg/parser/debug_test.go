package parser

import (
	"fmt"
	"testing"
)

func TestCollateBinary(t *testing.T) {
	p := New()
	_, _, err := p.Parse("create table t1 (c1 int) collate=binary;", "", "")
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	} else {
		fmt.Println("SUCCESS")
	}
}
