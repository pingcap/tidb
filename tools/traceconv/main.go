package main

import (
	"os"
	"bufio"
	"encoding/json"

	"github.com/pingcap/tidb/pkg/util/tracing"
)

func main() {
	filePath := os.Args[1]
	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	events, err := tracing.Read(rd)
	if err != nil {
		panic(err)
	}

	out, err := os.OpenFile("trace.json", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	err = enc.Encode(events)
	if err != nil {
		panic(err)
	}
}
