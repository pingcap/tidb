package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// 解析命令行参数
var (
	credentialPath = flag.String("credential", "/Users/fanzhou/tcms/120T/credential", "Path to GCS credential file")
	//credentialPath = flag.String("credential", "/home/admin/credential", "Path to GCS credential file")
	//templatePath        = flag.String("template", "/home/admin/template.sql", "Path to SQL schema template")
	templatePath        = flag.String("template", "/Users/fanzhou/Documents/GitHub/tidb/tools/csv_writer/template.sql", "Path to SQL schema template")
	concurrency         = flag.Int("concurrency", 3, "Number of concurrent goroutines for data generation and GCS upload")
	rowCount            = flag.Int("rows", 8000, "Number of rows to generate")
	duplicateWriteTimes = flag.Int("duplicateWriteTimes", 1, "Number of rows to generate")
	showFile            = flag.Bool("showFile", false, "List all files in the GCS directory without generating data")
	deleteFileName      = flag.String("deleteFile", "", "Delete a specific file from GCS")
	deleteAfterWrite    = flag.Bool("deleteAfterWrite", false, "Delete all file from GCS after write, TEST ONLY!")
	//localPath           = flag.String("localPath", "", "Path to write local file")
	localPath         = flag.String("localPath", "/Users/fanzhou/Documents/GitHub/tidb/tools/csv_writer/testCSVWriter.0.csv", "Path to write local file")
	glanceFile        = flag.String("glanceFile", "", "Glance the first 128*1024 byte of a specific file from GCS")
	fileNamePrefix    = flag.String("fileNamePrefix", "testCSVWriter", "Base file name")
	deletePrefixFile  = flag.String("deletePrefixFile", "", "Delete all files with prefix")
	testLongTimeWrite = flag.Bool("testLongTimeWrite", false, "Test long time write")

	batchSize    = flag.Int("batchSize", 500, "Number of rows to generate in each batch")
	generatorNum = flag.Int("generatorNum", 16, "Number of generator goroutines")
	writerNum    = flag.Int("writerNum", 16, "Number of writer goroutines")
)

const (
	maxRetries = 3
	charSet    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // 最大重试次数
)

var faker *gofakeit.Faker
var faker1 *gofakeit.Faker

// 初始化 Faker 实例
func init() {
	rand.Seed(time.Now().UnixNano())
	faker = gofakeit.New(time.Now().Unix())
	faker1 = gofakeit.New(time.Now().Unix())
}

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

	for _, line := range lines {
		line = strings.TrimSpace(line)
		// 过滤掉空行、CREATE TABLE 和 `);`
		if line == "" || strings.HasPrefix(strings.ToUpper(line), "CREATE TABLE") || strings.HasPrefix(line, ");") {
			continue
		}

		// 去掉结尾的 `,`
		line = strings.TrimSuffix(line, ",")

		// 拆分列定义
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		colName := strings.Trim(parts[0], "`") // 获取字段名
		colType := strings.ToUpper(parts[1])   // 获取数据类型

		// 处理 ENUM 类型
		var enumValues []string
		if strings.HasPrefix(strings.ToUpper(colType), "ENUM") {
			start := strings.Index(line, "(")
			end := strings.LastIndex(line, ")")

			if start != -1 && end != -1 && end > start {
				enumStr := line[start+1 : end]
				enumStr = strings.ReplaceAll(enumStr, "'", "") // 去掉单引号
				enumValues = strings.Split(enumStr, ",")       // 按逗号拆分
			}
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

	case strings.HasPrefix(col.Type, "TINYINT"):
		n := extractNumberFromSQLType(col.Type)
		if n == 1 {
			return strconv.Itoa(gofakeit.Number(0, 1))
		}
		return strconv.Itoa(gofakeit.Number(-128, 127))

	//case strings.HasPrefix(col.Type, "DECIMAL"), strings.HasPrefix(col.Type, "FLOAT"), strings.HasPrefix(col.Type, "DOUBLE"):
	//	return fmt.Sprintf("%.2f", gofakeit.Float64Range(1.0, 10000.0))

	//case strings.HasPrefix(col.Type, "VARCHAR"):
	//	n := extractNumberFromSQLType(col.Type)
	//	return gofakeit.Regex(fmt.Sprintf("[a-zA-Z0-9]{%d}", n))
	//
	//case strings.HasPrefix(col.Type, "TEXT"):
	//	n := 64
	//	return gofakeit.Regex(fmt.Sprintf("[a-zA-Z0-9]{%d}", n))
	//
	//case strings.HasPrefix(col.Type, "BOOLEAN"):
	//	return strconv.Itoa(gofakeit.Number(0, 1))

	//case strings.HasPrefix(col.Type, "DATE"):
	//	return gofakeit.Date().Format("2006-01-02")

	case strings.HasPrefix(col.Type, "DATETIME"), strings.HasPrefix(col.Type, "TIMESTAMP"):
		start := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Now() // 取当前时间
		randomTime := gofakeit.DateRange(start, end)
		return randomTime.Format("2006-01-02 15:04:05")

	//case strings.HasPrefix(col.Type, "ENUM") && len(col.Enum) > 0:
	//	return col.Enum[gofakeit.Number(0, len(col.Enum)-1)]

	case strings.HasPrefix(col.Type, "VARBINARY"):
		n := extractNumberFromSQLType(col.Type)
		return generateLetterWithNum(n)

	case strings.HasPrefix(col.Type, "MEDIUMBLOB"):
		return generateLetterWithNum(73312)
	}

	// 默认返回字符串
	return gofakeit.Word()
}

func generateLetterWithNum(len int) string {
	var builder strings.Builder

	// 如果长度小于等于1000，直接生成
	if len <= 1000 {
		builder.WriteString(faker1.Regex(fmt.Sprintf("[a-zA-Z0-9]{%d}", len)))
	} else {
		// 生成1000字符的部分
		builder.WriteString(faker1.Regex("[a-zA-Z0-9]{1000}"))

		// 重复生成
		for i := 1; i < len/1000; i++ {
			builder.WriteString(builder.String()[:1000])
		}

		// 如果有剩余，补充
		remain := len % 1000
		if remain > 0 {
			builder.WriteString(builder.String()[:remain])
		}
	}

	return builder.String()
}

func generateValueByCol(col Column, num int, res []string) {
	switch {
	case strings.HasPrefix(col.Type, "BIGINT"):
		generateBigint(num, res)

	case strings.HasPrefix(col.Type, "TINYINT"):
		generateTinyint1(num, res)

	case strings.HasPrefix(col.Type, "TIMESTAMP"):
		generateTimestamp(num, res)

	case strings.HasPrefix(col.Type, "VARBINARY"):
		generateVarbinary(num, extractNumberFromSQLType(col.Type), res)

	case strings.HasPrefix(col.Type, "MEDIUMBLOB"):
		generateMediumblob(num, res)
	default:
		log.Printf("Unsupported type: %s", col.Type)
	}
	//return nil
}

func generateBigint(num int, res []string) {
	//res := make([]string, num)
	for i := 0; i < num; i++ {
		res[i] = strconv.Itoa(faker.Number(1, 1000000000))
	}
	//return res
}

func generateTinyint1(num int, res []string) {
	//res := make([]string, num)
	for i := 0; i < num; i++ {
		res[i] = strconv.Itoa(faker.Number(0, 1))
	}
	//return res
}

func generateVarbinary(num, len int, res []string) {
	//res := make([]string, num)
	for i := 0; i < num; i++ {
		res[i] = generateLetterWithNum(len)
	}
	//return res
}

//func generateVarbinary(num, len int) []string {
//	// Create a slice to hold the results
//	res := make([]string, num)
//	// Create a channel to receive the results from goroutines
//	resultChan := make(chan string, num)
//
//	// Use WaitGroup to ensure all goroutines finish before returning
//	var wg sync.WaitGroup
//	for i := 0; i < num; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//			// Generate random string for each goroutine
//			resultChan <- generateLetterWithNum(len)
//		}(i)
//	}
//
//	// Wait for all goroutines to finish and collect the results
//	go func() {
//		wg.Wait()
//		close(resultChan)
//	}()
//
//	// Fill the result slice with data from the channel
//	i := 0
//	for resStr := range resultChan {
//		res[i] = resStr
//		i++
//	}
//	return res
//}

func generateMediumblob(num int, res []string) {
	generateVarbinary(num, 73312, res)
}

func generateTimestamp(num int, res []string) {
	//res := make([]string, num)
	start := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Now() // 取当前时间
	for i := 0; i < num; i++ {
		randomTime := faker.DateRange(start, end)
		res[i] = randomTime.Format("2006-01-02 15:04:05")
	}
	//return res
}

// 生成符合字段类型的数据（并发）
func generateDataConcurrentlyByCol(columns []Column, rowCount int, concurrency int) [][][]string {
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
			//for _, col := range columns {
			//t := time.Now()
			//workerData = append(workerData, generateValueByCol(col, end-start))
			//log.Printf("Worker %d: 生成 %s 数据耗时: %v", workerID, col.Type, time.Since(t))
			//}

			dataChannel <- workerData
			log.Printf("Worker %d: 生成完成 %d 行数据", workerID, len(workerData))
		}(i)
	}

	wg.Wait()
	close(dataChannel)

	// 合并所有数据
	data := make([][][]string, 0, rowCount)
	for chunk := range dataChannel {
		data = append(data, chunk)
	}

	endTime := time.Now()
	log.Printf("生成随机数据完成，耗时: %v", endTime.Sub(startTime))

	return data
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
	log.Printf("生成随机数据完成，耗时: %v", endTime.Sub(startTime))

	return data
}

// 长时间并发写入 GCS
func pressureWriteToGCSConcurrently(data [][]string, baseFileName string, concurrency int, credentialPath string) {
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
			start := workerID * chunkSize
			end := start + chunkSize
			if workerID == concurrency-1 {
				end = len(data)
			}
			startT := time.Now()
			cnt := 0
			for ; time.Now().Sub(startT) < 120*time.Minute; cnt++ {
				fileName := fmt.Sprintf("%s.%d.%d.csv", baseFileName, workerID, cnt)
				// 重试机制
				success := false
				for attempt := 1; attempt <= maxRetries; attempt++ {
					err := writeDataToGCS(store, fileName, data[start:end])
					if err == nil {
						success = true
						//log.Printf("Worker %d: 成功写入 %s (%d 行)", workerID, fileName, end-start)
						break
					}

					log.Printf("Worker %d: 第 %d 次写入 GCS 失败: %v", workerID, attempt, err)

					// 指数退避策略：等待 `2^(attempt-1) * 100ms`（最大不超过 5s）
					waitTime := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
					if waitTime > 4*time.Second {
						waitTime = 4 * time.Second
					}
					time.Sleep(waitTime + time.Duration(rand.Intn(500))*time.Millisecond) // 额外加一点随机时间，避免同时重试
				}
				if !success {
					log.Printf("Worker %d: 最终写入失败 %s (%d 行)", workerID, fileName, end-start)
				}
			}
			// clean file after test
			for j := 0; j <= cnt; j++ {
				err = store.DeleteFile(context.Background(), fmt.Sprintf("%s.%d.%d.csv", baseFileName, workerID, j))
				if err != nil {
					log.Printf("Worker %d: 删除文件失败 %s (%d 行)", workerID, fmt.Sprintf("%s.%d.%d.csv", baseFileName, workerID, j), end-start)
				}
			}
		}(i)
	}

	wg.Wait()
	log.Printf("GCS 并发写入完成，耗时: %v", time.Now().Sub(startTime))
}

func pressureWriteToGCSConcurrentlyByCol(data [][][]string, baseFileName string, concurrency int, credentialPath string) {
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
			start := workerID * chunkSize
			end := start + chunkSize
			if workerID == concurrency-1 {
				end = len(data)
			}
			fileName := fmt.Sprintf("%s.%d.csv", baseFileName, workerID)
			// 重试机制
			for attempt := 1; attempt <= maxRetries; attempt++ {
				err := writeDataToGCS(store, fileName, data[start:end])
				if err == nil {
					log.Printf("Worker %d: 成功写入 %s (%d 行)", workerID, fileName, end-start)
					return
				}

				log.Printf("Worker %d: 第 %d 次写入 GCS 失败: %v", workerID, attempt, err)

				// 指数退避策略：等待 `2^(attempt-1) * 100ms`（最大不超过 5s）
				waitTime := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
				if waitTime > 4*time.Second {
					waitTime = 4 * time.Second
				}
				time.Sleep(waitTime + time.Duration(rand.Intn(500))*time.Millisecond) // 额外加一点随机时间，避免同时重试
			}
			log.Printf("Worker %d: 最终写入失败 %s (%d 行)", workerID, fileName, end-start)
		}(i)
	}
	wg.Wait()
	log.Printf("GCS 并发写入完成，耗时: %v", time.Now().Sub(startTime))

	showFiles(credentialPath)
	if *deleteAfterWrite {
		for i := 0; i < concurrency; i++ {
			err = store.DeleteFile(context.Background(), fmt.Sprintf("%s.%d.csv", baseFileName, i))
			if err != nil {
				log.Printf("删除文件失败 %s (%d 行)", fmt.Sprintf("%s.%d.csv", baseFileName, i), chunkSize)
			}
		}
	}
}

// 并发写入 GCS
func writeToGCSConcurrentlyByCol(data [][][]string, baseFileName string, concurrency int, credentialPath string) {
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
			start := workerID * chunkSize
			end := start + chunkSize
			if workerID == concurrency-1 {
				end = len(data)
			}
			fileName := fmt.Sprintf("%s.%d.csv", baseFileName, workerID)
			csvData := data[workerID]
			// 重试机制
			for attempt := 1; attempt <= maxRetries; attempt++ {
				err := writeDataToGCSByCol(store, fileName, csvData)
				if err == nil {
					log.Printf("Worker %d: 成功写入 %s (%d 行)", workerID, fileName, end-start)
					return
				}

				log.Printf("Worker %d: 第 %d 次写入 GCS 失败: %v", workerID, attempt, err)

				// 指数退避策略：等待 `2^(attempt-1) * 100ms`（最大不超过 5s）
				waitTime := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
				if waitTime > 4*time.Second {
					waitTime = 4 * time.Second
				}
				time.Sleep(waitTime + time.Duration(rand.Intn(500))*time.Millisecond) // 额外加一点随机时间，避免同时重试
			}
			log.Printf("Worker %d: 最终写入失败 %s (%d 行)", workerID, fileName, end-start)
		}(i)
	}
	wg.Wait()
	log.Printf("GCS 并发写入完成，耗时: %v", time.Now().Sub(startTime))

	showFiles(credentialPath)
	if *deleteAfterWrite {
		for i := 0; i < concurrency; i++ {
			err = store.DeleteFile(context.Background(), fmt.Sprintf("%s.%d.csv", baseFileName, i))
			if err != nil {
				log.Printf("删除文件失败 %s (%d 行)", fmt.Sprintf("%s.%d.csv", baseFileName, i), chunkSize)
			}
		}
	}
}

// 带重试的 GCS 写入封装
func writeDataToGCS(store storage.ExternalStorage, fileName string, data [][]string) error {
	writer, err := store.Create(context.Background(), fileName, nil)
	if err != nil {
		return fmt.Errorf("创建 GCS 文件失败: %w", err)
	}
	defer writer.Close(context.Background())

	for _, row := range data {
		_, err = writer.Write(context.Background(), []byte(strings.Join(row, ",")+"\n"))
		if err != nil {
			log.Printf("写入 GCS 失败，删除文件: %s", fileName)
			store.DeleteFile(context.Background(), fileName) // 删除已创建的文件
			return fmt.Errorf("写入 GCS 失败: %w", err)
		}
	}
	return nil
}

// 带重试的 GCS 写入封装
func writeDataToGCSByCol(store storage.ExternalStorage, fileName string, data [][]string) error {
	writer, err := store.Create(context.Background(), fileName, nil)
	if err != nil {
		return fmt.Errorf("创建 GCS 文件失败: %w", err)
	}
	defer writer.Close(context.Background())

	for i := 0; i < len(data[0]); i++ {
		row := make([]string, 0, len(data[0]))
		for j := 0; j < len(data); j++ {
			row = append(row, data[j][i])
		}
		_, err = writer.Write(context.Background(), []byte(strings.Join(row, ",")+"\n"))
		if err != nil {
			log.Printf("写入 GCS 失败，删除文件: %s", fileName)
			store.DeleteFile(context.Background(), fileName) // 删除已创建的文件
			return fmt.Errorf("写入 GCS 失败: %w", err)
		}
	}
	return nil
}

func deleteFile(credentialPath, fileName string) {
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}

	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}
	err = store.DeleteFile(context.Background(), fileName)
	if err != nil {
		panic(err)
	}
}

func showFiles(credentialPath string) {
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}

	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}
	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		log.Printf("Name: %s, Size: %d Size/MiB: %f", path, size, float64(size)/1024/1024)
		return nil
	})
}

func glanceFiles(credentialPath, fileName string) {
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}

	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}

	r, _ := store.Open(context.Background(), fileName, nil)
	b := make([]byte, 128*1024)
	r.Read(b)

	fmt.Println(string(b))
}

// 写入 CSV 文件
func writeCSVToLocalDisk(filename string, columns []Column, data [][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入表头
	//headers := []string{}
	//for _, col := range columns {
	//	headers = append(headers, col.Name)
	//}
	//writer.Write(headers)

	// 写入数据
	for _, row := range data {
		writer.Write(row)
	}

	return nil
}

// 写入 CSV 文件
func writeCSVToLocalDiskByCol(filename string, columns []Column, data [][][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入数据
	for _, batch := range data {
		for i := 0; i < len(batch[0]); i++ {
			row := []string{}
			for j := 0; j < len(batch); j++ {
				row = append(row, batch[j][i])
			}
			writer.Write(row)
		}
	}
	return nil
}

func writeCSVToLocalDiskByCol2(filename string, columns []Column, data [][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入数据
	for i := 0; i < len(data[0]); i++ {
		row := []string{}
		for j := 0; j < len(data); j++ {
			row = append(row, data[j][i])
		}
		writer.Write(row)
	}
	return nil
}

func showWriteSpeed(ctx context.Context, wg sync.WaitGroup) {
	defer wg.Done()
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: *credentialPath}}
	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}
	t := time.NewTicker(60 * time.Second)
	lastFileNum := 0
	lastTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			curFileNum := 0
			curSize := 0.0 // MiB
			store.WalkDir(ctx, &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
				curFileNum++
				curSize += float64(size) / 1024 / 1024
				return nil
			})
			if curFileNum > lastFileNum {
				timeDiff := time.Since(lastTime).Seconds()
				writeSpeed := (curSize - float64(lastFileNum)) / timeDiff
				log.Printf("Time: %s, Total files: %d, Files added: %d, WriteSpeed: %.2f MiB/s",
					time.Now().Format("2006-01-02 15:04:05"), curFileNum, curFileNum-lastFileNum, writeSpeed)
			}
			lastFileNum = curFileNum
			lastTime = time.Now()
		}
	}
}

func deleteAllFilesByPrefix(prefix string) {
	var fileNames []string
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: *credentialPath}}

	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}
	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		if strings.HasPrefix(path, prefix) {
			fileNames = append(fileNames, path)
		}
		return nil
	})
	for _, fileName := range fileNames {
		err = store.DeleteFile(context.Background(), fileName)
		if err != nil {
			panic(err)
		}
	}
}

// 主函数
func mainOld() {
	// 解析命令行参数
	flag.Parse()

	// 列出 GCS 目录下的文件
	if *showFile {
		showFiles(*credentialPath)
		return
	}

	// 删除指定文件
	if *deleteFileName != "" {
		deleteFile(*credentialPath, *deleteFileName)
		return
	}

	// 读取指定文件前 1024 字节
	if *glanceFile != "" {
		glanceFiles(*credentialPath, *glanceFile)
		return
	}

	// 删除所有文件
	if *deletePrefixFile != "" {
		deleteAllFilesByPrefix(*deletePrefixFile)
		return
	}

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
	startTime := time.Now()
	data := generateDataConcurrentlyByCol(columns, *rowCount, *concurrency)

	// 并发写入 GCS
	if *localPath != "" {
		err = writeCSVToLocalDiskByCol(*localPath, columns, data)
		if err != nil {
			log.Fatal("Error writing CSV:", err)
		}
		return
	}

	// 打印写入的速度
	//wgS := sync.WaitGroup{}
	//ctx, cancel := context.WithCancel(context.Background())
	//wgS.Add(1)
	//go showWriteSpeed(ctx, wgS)

	if *testLongTimeWrite {
		pressureWriteToGCSConcurrentlyByCol(data, *fileNamePrefix, *concurrency, *credentialPath)
	} else {
		writeToGCSConcurrentlyByCol(data, *fileNamePrefix, *concurrency, *credentialPath)
	}
	log.Printf("写入 GCS 完成，总耗时: %v", time.Since(startTime))
	//cancel()
	//wgS.Wait()
}

func main() {
	//go func() {
	//	// pprof 服务器，将暴露在 6060 端口
	//	if err := http.ListenAndServe(":6060", nil); err != nil {
	//		panic(err)
	//	}
	//}()
	// 解析命令行参数
	flag.Parse()

	// 列出 GCS 目录下的文件
	if *showFile {
		showFiles(*credentialPath)
		return
	}

	// 删除指定文件
	if *deleteFileName != "" {
		deleteFile(*credentialPath, *deleteFileName)
		return
	}

	// 读取指定文件前 1024 字节
	if *glanceFile != "" {
		glanceFiles(*credentialPath, *glanceFile)
		return
	}

	log.Printf("配置参数: credential=%s, template=%s, concurrency=%d, rowCount=%d",
		*credentialPath, *templatePath, *concurrency, *rowCount)

	// 读取 SQL Schema
	sqlSchema, err := readSQLFile(*templatePath)
	if err != nil {
		log.Fatalf("读取 SQL 模板失败: %v", err)
	}

	// 解析 Schema
	columns := parseSQLSchema(sqlSchema)

	if *rowCount <= 0 || *batchSize <= 0 {
		log.Fatal("总数和每个批次的数量必须大于 0")
	}

	// 计算任务数量
	taskCount := (*rowCount + *batchSize - 1) / *batchSize
	log.Printf("总共将生成 %d 个任务，每个任务最多生成 %d 行", taskCount, *batchSize)

	// 创建任务和结果的 channel
	tasksCh := make(chan Task, taskCount)
	resultsCh := make(chan Result, taskCount)

	// 建立一个 sync.Pool 用于复用 []string 切片，初始容量为 batchSize
	pool := &sync.Pool{
		New: func() interface{} {
			return make([][]string, *batchSize)
		},
	}

	var wgGen sync.WaitGroup
	// 启动 generator worker
	for i := 0; i < *generatorNum; i++ {
		wgGen.Add(1)
		go generatorWorkerByCol(tasksCh, resultsCh, i, pool, &wgGen)
	}

	var wgWriter sync.WaitGroup
	// 启动 writer worker
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: *credentialPath}}
	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}
	for i := 0; i < *writerNum; i++ {
		wgWriter.Add(1)
		go writerWorkerByCol(resultsCh, store, i, pool, &wgWriter)
	}

	// 将任务按照 [begin, end) 的范围进行分解，并发送到 tasksCh
	startTime := time.Now()
	taskID := 0
	currentIndex := 0
	var fileNames []string

	for currentIndex < *rowCount {
		begin := currentIndex
		end := currentIndex + *batchSize
		if end > *rowCount {
			end = *rowCount
		}
		csvFileName := fmt.Sprintf("%s.%d.csv", *fileNamePrefix, taskID)
		fileNames = append(fileNames, csvFileName)
		task := Task{
			id:       taskID,
			begin:    begin,
			end:      end,
			cols:     columns,
			fileName: csvFileName,
		}
		tasksCh <- task
		taskID++
		currentIndex = end
	}
	close(tasksCh) // 任务分发完毕后关闭 tasksCh

	// 等待所有 generator 完成后关闭 resultsCh
	wgGen.Wait()
	close(resultsCh)

	// 等待所有 writer 完成写入
	wgWriter.Wait()
	log.Printf("写入 GCS 完成，总耗时: %v", time.Since(startTime))
	showFiles(*credentialPath)

	if *deleteAfterWrite {
		for _, fileName := range fileNames {
			deleteFile(*credentialPath, fileName)
		}
		log.Printf("delete all files after write")
	}

	log.Printf("Done！")
}

// Task 表示一个任务，使用 [begin, end) 表示任务需要生成的随机字符串数量
type Task struct {
	id       int
	begin    int
	end      int
	cols     []Column
	fileName string
}

// Result 表示生成结果，包含任务 id 以及生成的随机字符串集合
type Result struct {
	id       int
	fileName string
	values   [][]string
}

// generatorWorker 从 tasksCh 中获取任务，使用 sync.Pool 复用 []string 切片，生成随机字符串后发送到 resultsCh
func generatorWorker(tasksCh <-chan Task, resultsCh chan<- Result, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range tasksCh {
		count := task.end - task.begin
		// 尝试从池中获取一个 [][]string 切片
		buf := pool.Get().([][]string)
		if cap(buf) < count {
			buf = make([][]string, count)
		}
		// 设定切片长度为 count
		values := buf[:count]

		log.Printf("Generator %d: 处理任务 %d, 范围 [%d, %d)，生成 %d 个随机字符串", workerID, task.id, task.begin, task.end, count)
		for i := 0; i < count; i++ {
			var row []string
			for _, col := range task.cols {
				row = append(row, generateValue(col))
			}
			values[i] = row
		}
		resultsCh <- Result{id: task.id, values: values, fileName: task.fileName}
	}
}

// writerWorker 从 resultsCh 中获取生成结果，并写入 CSV 文件后将使用完的切片放回 pool
func writerWorker(resultsCh <-chan Result, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: *credentialPath}}

	s, err := storage.ParseBackend("gcs://global-sort-dir", &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}

	for result := range resultsCh {
		success := false
		fileName := result.fileName
		// 重试机制
		for attempt := 1; attempt <= maxRetries; attempt++ {
			if *localPath != "" {
				err = writeCSVToLocalDisk(*localPath+fileName, nil, result.values)
				if err != nil {
					log.Fatal("Error writing CSV:", err)
				}
			} else {
				err = writeDataToGCS(store, fileName, result.values)
			}
			if err == nil {
				log.Printf("Worker %d: 成功写入 %s (%d 行)", workerID, fileName, len(result.values))
				success = true
				break
			}

			log.Printf("Worker %d: 第 %d 次写入 GCS 失败: %v", workerID, attempt, err)

			// 指数退避策略：等待 `2^(attempt-1) * 100ms`（最大不超过 5s）
			waitTime := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
			if waitTime > 4*time.Second {
				waitTime = 4 * time.Second
			}
			time.Sleep(waitTime + time.Duration(rand.Intn(500))*time.Millisecond) // 额外加一点随机时间，避免同时重试
		}
		if !success {
			log.Printf("Worker %d: 最终写入失败 %s (%d 行)", workerID, fileName, len(result.values))
		}

		// 将使用完的切片放回 pool 供后续复用
		pool.Put(result.values)
	}
}

// generatorWorker 从 tasksCh 中获取任务，使用 sync.Pool 复用 []string 切片，生成随机字符串后发送到 resultsCh
func generatorWorkerByCol(tasksCh <-chan Task, resultsCh chan<- Result, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range tasksCh {
		startTime := time.Now()
		colNum := len(task.cols)
		count := task.end - task.begin
		// 尝试从池中获取一个 [][]string 切片
		buf := pool.Get().([][]string)
		if cap(buf) != colNum {
			buf = make([][]string, colNum)
		}
		for i := range buf {
			if len(buf[i]) != *batchSize {
				buf[i] = make([]string, *batchSize)
			}
		}
		// 设定切片长度为 count
		values := buf[:colNum]
		for i, col := range task.cols {
			//t := time.Now()
			generateValueByCol(col, count, values[i])
			//log.Printf("Worker %d: 生成 %s 数据耗时: %v", workerID, col.Type, time.Since(t))
		}
		log.Printf("Generator %d: 处理任务 %d, 主键范围 [%d, %d)，生成 %d 个随机字符串, 耗时: %v",
			workerID, task.id, task.begin, task.end, count, time.Since(startTime))
		resultsCh <- Result{id: task.id, values: values, fileName: task.fileName}
	}
}

// writerWorker 从 resultsCh 中获取生成结果，并写入 CSV 文件后将使用完的切片放回 pool
func writerWorkerByCol(resultsCh <-chan Result, store storage.ExternalStorage, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error

	for result := range resultsCh {
		success := false
		fileName := result.fileName
		// 重试机制
		for attempt := 1; attempt <= maxRetries; attempt++ {
			startTime := time.Now()
			if *localPath != "" {
				success = true
				break
				err = writeCSVToLocalDiskByCol2(*localPath+fileName, nil, result.values)
				if err != nil {
					log.Fatal("Error writing CSV:", err)
				}
			} else {
				err = writeDataToGCSByCol(store, fileName, result.values)
			}
			if err == nil {
				log.Printf("Worker %d: 成功写入 %s (%d 行), 耗时: %v", workerID, fileName, len(result.values[0]), time.Since(startTime))
				success = true
				break
			}

			log.Printf("Worker %d: 第 %d 次写入 GCS 失败: %v", workerID, attempt, err)

			// 指数退避策略：等待 `2^(attempt-1) * 100ms`（最大不超过 5s）
			waitTime := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
			if waitTime > 4*time.Second {
				waitTime = 4 * time.Second
			}
			time.Sleep(waitTime + time.Duration(rand.Intn(500))*time.Millisecond) // 额外加一点随机时间，避免同时重试
		}
		if !success {
			log.Printf("Worker %d: 最终写入失败 %s (%d 行)", workerID, fileName, len(result.values))
		}

		// 将使用完的切片放回 pool 供后续复用
		pool.Put(result.values)
	}
}
