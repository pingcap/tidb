package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strconv"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

var (
	schema     = flag.String("schema", "test", "Test schema name")
	table      = flag.String("table", "parquet", "Test table name")
	chunks     = flag.Int("chunk", 10, "Chunk files count")
	rowNumbers = flag.Int("rows", 1000, "Row number for each test file")
	sourceDir  = flag.String("dir", "", "test directory path")
)

func genParquetFile(dir, name string, count int) error {
	type Test struct {
		I int32  `parquet:"name=iVal, type=INT32"`
		S string `parquet:"name=s, type=UTF8, encoding=PLAIN_DICTIONARY"`
	}

	w, err := local.NewLocalFileWriter(filepath.Join(dir, name))
	if err != nil {
		return err
	}

	test := &Test{}
	dataWriter, err := writer.NewParquetWriter(w, test, 2)
	if err != nil {
		return err
	}
	for i := 0; i < count; i++ {
		test.I = int32(i)
		test.S = strconv.Itoa(i)
		err := dataWriter.Write(test)
		if err != nil {
			return err
		}
	}
	err = dataWriter.WriteStop()
	if err != nil {
		return err
	}
	w.Close()

	return nil
}

func main() {
	flag.Parse()

	for i := 0; i < *chunks; i++ {
		name := fmt.Sprintf("%s.%s.%04d.parquet", *schema, *table, i)
		err := genParquetFile(*sourceDir, name, *rowNumbers)
		if err != nil {
			log.Fatalf("generate test source failed, name: %s, err: %+v", name, err)
		}
	}
}
