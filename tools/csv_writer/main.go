//package main
//
//import (
//	"context"
//	"encoding/csv"
//	"fmt"
//	"io/ioutil"
//	"log"
//	"os"
//	"regexp"
//	"strconv"
//	"strings"
//	"sync"
//	"time"
//
//	"github.com/brianvoe/gofakeit/v6"
//	"github.com/pingcap/tidb/br/pkg/storage"
//)
//
//func gcs_demo() {
//	//js := os.Getenv("/Users/fanzhou/tcms/120T/gke/credential")
//	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: "/Users/fanzhou/tcms/120T/gke/credential"}}
//
//	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
//	if err != nil {
//		panic(err)
//	}
//	store, err := storage.NewWithDefaultOpt(context.Background(), s)
//	if err != nil {
//		panic(err)
//	}
//
//	aFile := "testCSVWriter.0.sql"
//	//writer, err := store.Create(context.Background(), aFile, nil)
//	//if err != nil {
//	//	panic(err)
//	//}
//	//_, err = writer.Write(context.Background(), []byte("Hello, World!"))
//	//if err != nil {
//	//	panic(err)
//	//}
//	//writer.Close(context.Background())
//	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
//		log.Printf("path: %s, size: %d", path, size)
//		return nil
//	})
//	store.DeleteFile(context.Background(), aFile)
//}
//
//func deleteFile(store storage.ExternalStorage, fileName string) {
//	err := store.DeleteFile(context.Background(), fileName)
//	if err != nil {
//		panic(err)
//	}
//}
//
//func showFiles(store storage.ExternalStorage) {
//	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
//		log.Printf("Name: %s, Size: %d Size/MiB: %f", path, size, float64(size)/1024/1024)
//		return nil
//	})
//}
//
//func writeToGCS(data [][]string, fileName string) {
//	credentialPath := "/Users/fanzhou/tcms/120T/gke/credential"
//	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}
//
//	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
//	if err != nil {
//		panic(err)
//	}
//	store, err := storage.NewWithDefaultOpt(context.Background(), s)
//	if err != nil {
//		panic(err)
//	}
//
//	startTime := time.Now()
//	writer, err := store.Create(context.Background(), fileName, nil)
//	if err != nil {
//		panic(err)
//	}
//	for _, row := range data {
//		_, err = writer.Write(context.Background(), []byte(strings.Join(row, ",")))
//		if err != nil {
//			panic(err)
//		}
//	}
//	writer.Close(context.Background())
//
//	endTime := time.Now()
//	log.Printf("GCS write time: %v", endTime.Sub(startTime))
//
//	showFiles(store)
//	store.DeleteFile(context.Background(), fileName)
//}
//
// Column 定义表字段
//
//// 解析 SQL Schema，提取表结构
//func parseSQLSchema(schema string) []Column {
//	lines := strings.Split(schema, "\n")
//	columns := []Column{}
//	enumPattern := regexp.MustCompile(`ENUM$begin:math:text$(.*?)$end:math:text$`)
//
//	for _, line := range lines {
//		line = strings.TrimSpace(line)
//		if line == "" || strings.HasPrefix(line, "CREATE TABLE") || strings.HasPrefix(line, ");") {
//			continue
//		}
//
//		parts := strings.Fields(line)
//		if len(parts) < 2 {
//			continue
//		}
//
//		colName := strings.Trim(parts[0], "`") // 字段名
//		colType := strings.ToUpper(parts[1])   // 数据类型
//
//		// 处理 ENUM 类型
//		enumMatches := enumPattern.FindStringSubmatch(line)
//		var enumValues []string
//		if len(enumMatches) > 1 {
//			enumValues = strings.Split(strings.ReplaceAll(enumMatches[1], "'", ""), ",")
//		}
//
//		columns = append(columns, Column{Name: colName, Type: colType, Enum: enumValues})
//	}
//	return columns
//}
//
//// 生成符合字段类型的数据
//func generateData(columns []Column, rowCount int) [][]string {
//	var data [][]string
//
//	for i := 0; i < rowCount; i++ {
//		row := []string{}
//		for _, col := range columns {
//			row = append(row, generateValue(col))
//		}
//		data = append(data, row)
//	}
//
//	return data
//}
//
//// 生成符合字段类型的数据（并发）
//func generateDataConcurrently(columns []Column, rowCount int) [][]string {
//	var wg sync.WaitGroup
//	chunkSize := rowCount / concurrency
//	dataChannel := make(chan [][]string, concurrency) // 并发安全的 channel
//
//	startTime := time.Now()
//
//	// 并发生成数据
//	for i := 0; i < concurrency; i++ {
//		wg.Add(1)
//
//		go func(workerID int) {
//			defer wg.Done()
//
//			start := workerID * chunkSize
//			end := start + chunkSize
//			if workerID == concurrency-1 { // 处理剩余数据
//				end = rowCount
//			}
//
//			log.Printf("Worker %d: 生成数据 %d - %d", workerID, start, end)
//
//			workerData := make([][]string, 0, end-start)
//			for j := start; j < end; j++ {
//				row := []string{}
//				for _, col := range columns {
//					row = append(row, generateValue(col))
//				}
//				workerData = append(workerData, row)
//			}
//
//			dataChannel <- workerData
//			log.Printf("Worker %d: 生成完成 %d 行数据", workerID, len(workerData))
//		}(i)
//	}
//
//	// 等待所有 goroutine 完成
//	wg.Wait()
//	close(dataChannel)
//
//	// 合并所有数据
//	data := make([][]string, 0, rowCount)
//	for chunk := range dataChannel {
//		data = append(data, chunk...)
//	}
//
//	endTime := time.Now()
//	log.Printf("生成随机数据完成，耗时: %v", endTime.Sub(startTime))
//
//	return data
//}
//
//func extractNumberFromSQLType(sqlType string) int {
//	start := strings.Index(sqlType, "(")
//	end := strings.Index(sqlType, ")")
//
//	if start != -1 && end != -1 && start < end {
//		numStr := sqlType[start+1 : end]
//		num, err := strconv.Atoi(numStr)
//		if err == nil {
//			return num
//		}
//	}
//
//	return -1 // 未找到
//}
//

//
//// 写入 CSV 文件
//func writeCSV(filename string, columns []Column, data [][]string) error {
//	file, err := os.Create(filename)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//
//	writer := csv.NewWriter(file)
//	defer writer.Flush()
//
//	// 写入表头
//	headers := []string{}
//	for _, col := range columns {
//		headers = append(headers, col.Name)
//	}
//	writer.Write(headers)
//
//	// 写入数据
//	for _, row := range data {
//		writer.Write(row)
//	}
//
//	return nil
//}
//
//// 读取 SQL Schema 文件
//func readSQLFile(filename string) (string, error) {
//	data, err := ioutil.ReadFile(filename)
//	if err != nil {
//		return "", err
//	}
//	return string(data), nil
//}
//
//// 并发数
//func writeToGCSConcurrently(data [][]string, baseFileName string) {
//	var wg sync.WaitGroup
//	chunkSize := len(data) / concurrency // 每个协程处理 chunkSize 行数据
//
//	// GCS 认证信息
//	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}
//
//	// 初始化 GCS 存储
//	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
//	if err != nil {
//		panic(err)
//	}
//	store, err := storage.NewWithDefaultOpt(context.Background(), s)
//	if err != nil {
//		panic(err)
//	}
//
//	startTime := time.Now()
//
//	// 启动 3 个 goroutine 并发写入
//	for i := 0; i < concurrency; i++ {
//		wg.Add(1)
//
//		go func(workerID int) {
//			defer wg.Done()
//
//			fileName := fmt.Sprintf("%s.%d.sql", baseFileName, workerID) // 生成不同文件
//			writer, err := store.Create(context.Background(), fileName, nil)
//			if err != nil {
//				log.Printf("Worker %d: 创建 GCS 文件失败: %v", workerID, err)
//				return
//			}
//
//			// 计算数据切片范围
//			start := workerID * chunkSize
//			end := start + chunkSize
//			if workerID == concurrency-1 { // 最后一个 worker 可能需要写剩余的数据
//				end = len(data)
//			}
//
//			// 写入数据
//			for _, row := range data[start:end] {
//				_, err = writer.Write(context.Background(), []byte(strings.Join(row, ",")+"\n"))
//				if err != nil {
//					log.Printf("Worker %d: 写入 GCS 失败: %v", workerID, err)
//					return
//				}
//			}
//			writer.Close(context.Background())
//
//			log.Printf("Worker %d: 完成写入 %s (%d 行)", workerID, fileName, end-start)
//		}(i)
//	}
//
//	wg.Wait() // 等待所有协程完成
//	endTime := time.Now()
//	log.Printf("GCS 并发写入完成，耗时: %v", endTime.Sub(startTime))
//	showFiles(store)
//	for i := 0; i < concurrency; i++ {
//		deleteFile(store, fmt.Sprintf("%s.%d.sql", baseFileName, i))
//	}
//}
//
//// 主函数
//const (
//	credentialPath = "/home/admin/credential"
//	templatePath   = "/home/admin/template.sql"
//	concurrency    = 3
//	rowCount       = 10000
//)
//
//func main() {
//	//gcs_demo()
//	//return
//
//	sqlSchema, err := readSQLFile(templatePath)
//	if err != nil {
//		log.Fatalf("read schema template err: %v", err)
//	}
//
//	// 解析 Schema
//	columns := parseSQLSchema(sqlSchema)
//
//	// 生成数据
//	data := generateDataConcurrently(columns, rowCount)
//
//	// write to local disk
//	//filename := "/Users/fanzhou/Documents/GitHub/tidb/tools/csv_writer/users.csv"
//	//err = writeCSV(filename, columns, data)
//	//if err != nil {
//	//	log.Fatal("Error writing CSV:", err)
//	//}
//
//	// write to GCS
//	//fileName := "testCSVWriter.0.sql"
//	//startTime := time.Now()
//	//writeToGCS(data, fileName)
//	//endTime := time.Now()
//	//log.Printf("GCS write time: %v", endTime.Sub(startTime))
//
//	writeToGCSConcurrently(data, "testCSVWriter")
//}

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// 解析命令行参数
var (
	credentialPath = flag.String("credential", "/home/admin/credential", "Path to GCS credential file")
	templatePath   = flag.String("template", "/home/admin/template.sql", "Path to SQL schema template")
	concurrency    = flag.Int("concurrency", 3, "Number of concurrent goroutines for data generation and GCS upload")
	rowCount       = flag.Int("rows", 10000, "Number of rows to generate")
)

type Column struct {
	Name string
	Type string
	Enum []string // 处理 ENUM 类型
}

// 读取 SQL Schema 文件
func readSQLFile(filename string) (string, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// 解析 SQL Schema
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

// 生成符合字段类型的数据（并发）
func generateDataConcurrently(columns []Column, rowCount int, concurrency int) [][]string {
	var wg sync.WaitGroup
	chunkSize := rowCount / concurrency
	dataChannel := make(chan [][]string, concurrency)

	startTime := time.Now()

	// 并发生成数据
	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			start := workerID * chunkSize
			end := start + chunkSize
			if workerID == concurrency-1 {
				end = rowCount
			}

			log.Printf("Worker %d: 生成数据 %d - %d", workerID, start, end)

			workerData := make([][]string, 0, end-start)
			for j := start; j < end; j++ {
				row := []string{}
				for _, col := range columns {
					row = append(row, generateValue(col))
				}
				workerData = append(workerData, row)
			}

			dataChannel <- workerData
			log.Printf("Worker %d: 生成完成 %d 行数据", workerID, len(workerData))
		}(i)
	}

	wg.Wait()
	close(dataChannel)

	// 合并所有数据
	data := make([][]string, 0, rowCount)
	for chunk := range dataChannel {
		data = append(data, chunk...)
	}

	endTime := time.Now()
	log.Printf("✅ 生成随机数据完成，耗时: %v", endTime.Sub(startTime))

	return data
}

// 并发写入 GCS
func writeToGCSConcurrently(data [][]string, baseFileName string, concurrency int, credentialPath string) {
	var wg sync.WaitGroup
	chunkSize := len(data) / concurrency

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

	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			fileName := fmt.Sprintf("%s.%d.sql", baseFileName, workerID)
			writer, err := store.Create(context.Background(), fileName, nil)
			if err != nil {
				log.Printf("Worker %d: 创建 GCS 文件失败: %v", workerID, err)
				return
			}

			start := workerID * chunkSize
			end := start + chunkSize
			if workerID == concurrency-1 {
				end = len(data)
			}

			for _, row := range data[start:end] {
				_, err = writer.Write(context.Background(), []byte(strings.Join(row, ",")+"\n"))
				if err != nil {
					log.Printf("Worker %d: 写入 GCS 失败: %v", workerID, err)
					return
				}
			}
			writer.Close(context.Background())

			log.Printf("Worker %d: 完成写入 %s (%d 行)", workerID, fileName, end-start)
		}(i)
	}

	wg.Wait()
	endTime := time.Now()
	log.Printf("✅ GCS 并发写入完成，耗时: %v", endTime.Sub(startTime))
}

// 主函数
func main() {
	// 解析命令行参数
	flag.Parse()

	log.Printf("配置参数: credential=%s, template=%s, concurrency=%d, rowCount=%d",
		*credentialPath, *templatePath, *concurrency, *rowCount)

	// 读取 SQL Schema
	sqlSchema, err := readSQLFile(*templatePath)
	if err != nil {
		log.Fatalf("读取 SQL 模板失败: %v", err)
	}

	// 解析 Schema
	columns := parseSQLSchema(sqlSchema)

	// 并发生成数据
	data := generateDataConcurrently(columns, *rowCount, *concurrency)

	// 并发写入 GCS
	writeToGCSConcurrently(data, "testCSVWriter", *concurrency, *credentialPath)
}
