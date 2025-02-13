package main

import (
	"bytes"
	"context"
	"encoding/base64"
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
	credentialPath   = flag.String("credential", "/Users/fanzhou/tcms/120T/credential", "Path to GCS credential file")
	templatePath     = flag.String("template", "/Users/fanzhou/Documents/GitHub/tidb/tools/csv_writer/template.sql", "Path to SQL schema template")
	showFile         = flag.Bool("showFile", false, "List all files in the GCS directory without generating data")
	deleteFileName   = flag.String("deleteFile", "", "Delete a specific file from GCS")
	deleteAfterWrite = flag.Bool("deleteAfterWrite", false, "Delete all file from GCS after write, TEST ONLY!")
	localPath        = flag.String("localPath", "/Users/fanzhou/Documents/GitHub/tidb/tools/csv_writer2/", "Path to write local file")
	glanceFile       = flag.String("glanceFile", "", "Glance the first 128*1024 byte of a specific file from GCS")
	fileNamePrefix   = flag.String("fileNamePrefix", "testCSVWriter", "Base file name")
	deletePrefixFile = flag.String("deletePrefixFile", "", "Delete all files with prefix")
	gcsDir           = flag.String("gcsDir", "gcs://global-sort-dir", "GCS directory")

	batchSize           = flag.Int("batchSize", 10, "Number of rows to generate in each batch")
	generatorNum        = flag.Int("generatorNum", 1, "Number of generator goroutines")
	writerNum           = flag.Int("writerNum", 8, "Number of writer goroutines")
	pkBegin             = flag.Int("pkBegin", 0, "Begin of primary key, [begin, end)")
	pkEnd               = flag.Int("pkEnd", 10, "End of primary key[begin, end)")
	fileNameSuffixStart = flag.Int("fileNameSuffixStart", 0, "Start of file name suffix")
	base64Encode        = flag.Bool("base64Encode", false, "Base64 encode the CSV file")
)

const (
	maxRetries  = 3
	uuidLen     = 36
	maxIndexLen = 3072
)

var faker *gofakeit.Faker

// 初始化 Faker 实例
func init() {
	faker = gofakeit.New(time.Now().Unix())
}

type Column struct {
	Name     string
	Type     string
	Enum     []string // 处理 ENUM 类型
	IsPK     bool
	IsUnique bool
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

	hasPk := false
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
		col := Column{Name: colName, Type: colType, Enum: enumValues}
		if strings.Contains(strings.ToUpper(line), "PRIMARY KEY") && !hasPk {
			hasPk = true
			col.IsPK = true
		}
		if strings.Contains(strings.ToUpper(line), "UNIQUE KEY") &&
			strings.HasPrefix(col.Type, "VARBINARY") &&
			extractNumberFromSQLType(colType) > uuidLen &&
			extractNumberFromSQLType(colType) < maxIndexLen {
			col.IsUnique = true
		}
		columns = append(columns, col)

	}
	return columns
}

// 提取类型中的长度
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

func generateValueByCol(col Column, num int, res []string) {
	switch {
	case strings.HasPrefix(col.Type, "BIGINT"):
		generateBigint(num, res)

	case strings.HasPrefix(col.Type, "TINYINT"):
		generateTinyint1(num, res)

	case strings.HasPrefix(col.Type, "TIMESTAMP"):
		generateTimestamp(num, res)

	case strings.HasPrefix(col.Type, "VARBINARY"):
		generateVarbinary(num, extractNumberFromSQLType(col.Type), res, col.IsUnique)

	case strings.HasPrefix(col.Type, "MEDIUMBLOB"):
		generateMediumblob(num, res)

	default:
		log.Printf("Unsupported type: %s", col.Type)
	}
}

func generateLetterWithNum(len int) string {
	var builder strings.Builder

	len = faker.Number(1, len) // 随机长度 varbinary
	// 如果长度小于等于1000，直接生成
	if len <= 1000 {
		builder.WriteString(faker.Regex(fmt.Sprintf("[a-zA-Z0-9]{%d}", len)))
	} else {
		// 生成1000字符的部分
		builder.WriteString(faker.Regex("[a-zA-Z0-9]{1000}"))

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

func generateBigint(num int, res []string) {
	for i := 0; i < num; i++ {
		res[i] = strconv.Itoa(faker.Number(1, 1000000000))
	}
}

func generateTinyint1(num int, res []string) {
	for i := 0; i < num; i++ {
		res[i] = strconv.Itoa(faker.Number(0, 1))
	}
}

func generateVarbinary(num, len int, res []string, unique bool) {
	for i := 0; i < num; i++ {
		if unique {
			uuid := faker.UUID()
			res[i] = uuid + generateLetterWithNum(len-uuidLen)
		} else {
			res[i] = generateLetterWithNum(len)
		}
	}
}

func generateMediumblob(num int, res []string) {
	generateVarbinary(num, 73312, res, false)
}

func generateTimestamp(num int, res []string) {
	start := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Now() // 取当前时间
	for i := 0; i < num; i++ {
		randomTime := faker.DateRange(start, end)
		res[i] = randomTime.Format("2006-01-02 15:04:05")
	}
}

// 左闭右开区间 [begin, end)
func generatePrimaryKey(begin, end int, res []string) {
	idx := 0
	for key := begin; key < end; key++ {
		res[idx] = strconv.Itoa(key)
		//log.Printf("第 %d 个主键的值是 %d", idx, key)
		idx++
	}
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
		// base64 编码
		if *base64Encode {
			base64Str := base64.StdEncoding.EncodeToString([]byte(strings.Join(row, ",") + "\n"))
			_, err = writer.Write(context.Background(), []byte(base64Str))
		} else {
			_, err = writer.Write(context.Background(), []byte(strings.Join(row, ",")+"\n"))
		}
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

	s, err := storage.ParseBackend(*gcsDir, &op)
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

	s, err := storage.ParseBackend(*gcsDir, &op)
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

	s, err := storage.ParseBackend(*gcsDir, &op)
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

func writeCSVToLocalDiskBase64(data [][]string, fileName string) error {
	// 创建一个 bytes.Buffer 用于存放 CSV 数据
	var buf bytes.Buffer

	// 创建 CSV writer，将数据写入内存缓冲区
	writer := csv.NewWriter(&buf)
	for i := 0; i < len(data[0]); i++ {
		var row []string
		for j := 0; j < len(data); j++ {
			row = append(row, data[j][i])
		}
		writer.Write(row)
	}

	// 刷新 CSV writer，确保所有数据写入缓冲区
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatal("刷新 CSV writer 失败:", err)
		return err
	}

	// 将 CSV 数据进行 Base64 编码
	encoded := base64.StdEncoding.EncodeToString(buf.Bytes())

	// 将 Base64 编码后的字符串写入文件
	outFile, err := os.Create(fileName)
	if err != nil {
		log.Fatal("创建文件失败:", err)
		return err
	}
	defer outFile.Close()

	_, err = outFile.WriteString(encoded)
	if err != nil {
		log.Fatal("写入文件失败:", err)
		return err
	}

	log.Println(fmt.Sprintf("CSV 文件已 Base64 编码并写入 %s", fileName))
	return nil
}

// 写入 CSV 文件
func writeCSVToLocalDiskByCol2(filename string, columns []Column, data [][]string) error {
	if *base64Encode {
		return writeCSVToLocalDiskBase64(data, filename)
	}
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
	s, err := storage.ParseBackend(*gcsDir, &op)
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

	s, err := storage.ParseBackend(*gcsDir, &op)
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
			if len(buf[i]) != count {
				buf[i] = make([]string, count)
			}
		}
		// 设定切片长度为 count
		values := buf[:colNum]
		for i, col := range task.cols {
			//t := time.Now()
			if col.IsPK {
				generatePrimaryKey(task.begin, task.end, values[i])
			} else {
				generateValueByCol(col, count, values[i])
			}
			//log.Printf("Worker %d: 生成 %s 数据耗时: %v", workerID, col.Type, time.Since(t))
		}
		log.Printf("Generator %d: 处理 %s, 主键范围 [%d, %d)，生成 %d 行, 耗时: %v",
			workerID, task.fileName, task.begin, task.end, count, time.Since(startTime))
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
				err = writeCSVToLocalDiskByCol2(*localPath+fileName, nil, result.values)
				if err != nil {
					log.Fatal("Error writing CSV:", err)
				}
			} else {
				err = writeDataToGCSByCol(store, fileName, result.values)
			}
			if err == nil {
				log.Printf("Writer %d: 写入 %s (%d 行), 耗时: %v", workerID, fileName, len(result.values[0]), time.Since(startTime))
				success = true
				break
			}

			log.Printf("Writer %d: 第 %d 次写入 GCS 失败: %v", workerID, attempt, err)

			// 指数退避策略：等待 `2^(attempt-1) * 100ms`（最大不超过 5s）
			waitTime := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
			if waitTime > 4*time.Second {
				waitTime = 4 * time.Second
			}
			time.Sleep(waitTime + time.Duration(rand.Intn(500))*time.Millisecond) // 额外加一点随机时间，避免同时重试
		}
		if !success {
			log.Printf("Writer %d: 最终写入失败 %s (%d 行)", workerID, fileName, len(result.values))
		}

		// 将使用完的切片放回 pool 供后续复用
		pool.Put(result.values)
	}
}

func main() {
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

	// 删除所有符合前缀的文件
	if *deletePrefixFile != "" {
		deleteAllFilesByPrefix(*deletePrefixFile)
		return
	}

	// 读取指定文件前 1024 字节
	if *glanceFile != "" {
		glanceFiles(*credentialPath, *glanceFile)
		return
	}

	rowCount := *pkEnd - *pkBegin
	log.Printf("配置参数: credential=%s, template=%s, generatorNum=%d, writerNum=%d, rowCount=%d, batchSize=%d",
		*credentialPath, *templatePath, *generatorNum, *writerNum, rowCount, *batchSize)

	// 读取 SQL Schema
	sqlSchema, err := readSQLFile(*templatePath)
	if err != nil {
		log.Fatalf("读取 SQL 模板失败: %v", err)
	}

	// 解析 Schema
	columns := parseSQLSchema(sqlSchema)

	// 检查 pk 范围
	if rowCount%*batchSize != 0 {
		log.Fatal("pkEnd - pkBegin 必须是 batchSize 的整数倍")
	}

	// 计算任务数量
	if rowCount <= 0 || *batchSize <= 0 {
		log.Fatal("总数和每个批次的数量必须大于 0")
	}
	taskCount := (rowCount + *batchSize - 1) / *batchSize
	log.Printf("总共将生成 %d 个任务，每个任务最多生成 %d 行", taskCount, *batchSize)

	// 创建任务和结果的 channel
	tasksCh := make(chan Task, taskCount)
	resultsCh := make(chan Result, taskCount)

	// 建立一个 sync.Pool 用于复用 []string 切片，初始容量为 batchSize
	pool := &sync.Pool{
		New: func() interface{} {
			buf := make([][]string, len(columns))
			for i := range buf {
				if len(buf[i]) != *batchSize {
					buf[i] = make([]string, *batchSize)
				}
			}
			return buf
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
	s, err := storage.ParseBackend(*gcsDir, &op)
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
	taskID := *fileNameSuffixStart
	var fileNames []string

	for pk := *pkBegin; pk < *pkEnd; pk += *batchSize {
		begin := pk
		end := pk + *batchSize
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
