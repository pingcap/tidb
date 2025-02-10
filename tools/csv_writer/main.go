package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/pingcap/tidb/br/pkg/storage"
)

func gcs_demo() {
	//js := os.Getenv("/Users/fanzhou/tcms/120T/gke/credential")
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: "/Users/fanzhou/tcms/120T/gke/credential"}}

	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}

	aFile := "testCSVWriter.0.sql"
	//writer, err := store.Create(context.Background(), aFile, nil)
	//if err != nil {
	//	panic(err)
	//}
	//_, err = writer.Write(context.Background(), []byte("Hello, World!"))
	//if err != nil {
	//	panic(err)
	//}
	//writer.Close(context.Background())
	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		log.Printf("path: %s, size: %d", path, size)
		return nil
	})
	store.DeleteFile(context.Background(), aFile)
}

func deleteFile(store storage.ExternalStorage, fileName string) {
	err := store.DeleteFile(context.Background(), fileName)
	if err != nil {
		panic(err)
	}
}

func showFiles(store storage.ExternalStorage) {
	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		log.Printf("Name: %s, Size: %d Size/MiB: %f", path, size, float64(size)/1024/1024)
		return nil
	})
}

func writeToGCS(data [][]string, fileName string) {
	credentialPath := "/Users/fanzhou/tcms/120T/gke/credential"
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}

	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}

	startTime := time.Now()
	writer, err := store.Create(context.Background(), fileName, nil)
	if err != nil {
		panic(err)
	}
	for _, row := range data {
		_, err = writer.Write(context.Background(), []byte(strings.Join(row, ",")))
		if err != nil {
			panic(err)
		}
	}
	writer.Close(context.Background())

	endTime := time.Now()
	log.Printf("GCS write time: %v", endTime.Sub(startTime))

	showFiles(store)
	store.DeleteFile(context.Background(), fileName)
}

// Column 定义表字段
type Column struct {
	Name string
	Type string
	Enum []string // 处理 ENUM 类型
}

// 解析 SQL Schema，提取表结构
func parseSQLSchema(schema string) []Column {
	lines := strings.Split(schema, "\n")
	columns := []Column{}
	enumPattern := regexp.MustCompile(`ENUM$begin:math:text$(.*?)$end:math:text$`)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "CREATE TABLE") || strings.HasPrefix(line, ");") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		colName := strings.Trim(parts[0], "`") // 字段名
		colType := strings.ToUpper(parts[1])   // 数据类型

		// 处理 ENUM 类型
		enumMatches := enumPattern.FindStringSubmatch(line)
		var enumValues []string
		if len(enumMatches) > 1 {
			enumValues = strings.Split(strings.ReplaceAll(enumMatches[1], "'", ""), ",")
		}

		columns = append(columns, Column{Name: colName, Type: colType, Enum: enumValues})
	}
	return columns
}

// 生成符合字段类型的数据
func generateData(columns []Column, rowCount int) [][]string {
	var data [][]string

	for i := 0; i < rowCount; i++ {
		row := []string{}
		for _, col := range columns {
			row = append(row, generateValue(col))
		}
		data = append(data, row)
	}

	return data
}

func extractNumberFromSQLType(sqlType string) int {
	start := strings.Index(sqlType, "(")
	end := strings.Index(sqlType, ")")

	if start != -1 && end != -1 && start < end {
		numStr := sqlType[start+1 : end]
		num, err := strconv.Atoi(numStr)
		if err == nil {
			return num
		}
	}

	return -1 // 未找到
}

// 生成单个字段的随机值
func generateValue(col Column) string {
	switch {
	case strings.HasPrefix(col.Type, "INT"), strings.HasPrefix(col.Type, "BIGINT"):
		return strconv.Itoa(gofakeit.Number(1, 1000000))

	case strings.HasPrefix(col.Type, "DECIMAL"), strings.HasPrefix(col.Type, "FLOAT"), strings.HasPrefix(col.Type, "DOUBLE"):
		return fmt.Sprintf("%.2f", gofakeit.Float64Range(1.0, 10000.0))

	case strings.HasPrefix(col.Type, "VARCHAR"), strings.HasPrefix(col.Type, "TEXT"):
		n := extractNumberFromSQLType(col.Type)
		return gofakeit.LoremIpsumSentence(n)

	case strings.HasPrefix(col.Type, "BOOLEAN"):
		return strconv.FormatBool(gofakeit.Bool())

	case strings.HasPrefix(col.Type, "DATE"):
		return gofakeit.Date().Format("2006-01-02")

	case strings.HasPrefix(col.Type, "DATETIME"), strings.HasPrefix(col.Type, "TIMESTAMP"):
		return gofakeit.Date().Format("2006-01-02 15:04:05")

	case strings.HasPrefix(col.Type, "ENUM") && len(col.Enum) > 0:
		return col.Enum[gofakeit.Number(0, len(col.Enum)-1)]
	}

	// 默认返回字符串
	return gofakeit.Word()
}

// 写入 CSV 文件
func writeCSV(filename string, columns []Column, data [][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入表头
	headers := []string{}
	for _, col := range columns {
		headers = append(headers, col.Name)
	}
	writer.Write(headers)

	// 写入数据
	for _, row := range data {
		writer.Write(row)
	}

	return nil
}

// 读取 SQL Schema 文件
func readSQLFile(filename string) (string, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// 主函数
func main() {
	//gcs_demo()
	//return

	templatePath := "/Users/fanzhou/Documents/GitHub/tidb/tools/csv_writer/template.sql"
	sqlSchema, err := readSQLFile(templatePath)
	if err != nil {
		log.Fatalf("read schema template err: %v", err)
	}

	// 解析 Schema
	columns := parseSQLSchema(sqlSchema)

	// 生成数据
	rowCount := 10000
	data := generateData(columns, rowCount)

	// write to local disk
	//filename := "/Users/fanzhou/Documents/GitHub/tidb/tools/csv_writer/users.csv"
	//err = writeCSV(filename, columns, data)
	//if err != nil {
	//	log.Fatal("Error writing CSV:", err)
	//}

	// write to GCS
	fileName := "testCSVWriter.0.sql"
	//startTime := time.Now()
	writeToGCS(data, fileName)
	//endTime := time.Now()
	//log.Printf("GCS write time: %v", endTime.Sub(startTime))
}
