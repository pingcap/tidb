package main

import (
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// Command-line parameters
var (
	templatePath     = flag.String("template", "/home/admin/template.sql", "Path to SQL schema template")
	credentialPath   = flag.String("credential", "", "Path to GCS credential file")
	showFile         = flag.Bool("showFile", false, "List all files in the GCS directory without generating data")
	deleteFileName   = flag.String("deleteFile", "", "Delete a specific file from GCS")
	deleteAfterWrite = flag.Bool("deleteAfterWrite", false, "Delete all files from GCS after writing (TEST ONLY)")
	localPath        = flag.String("localPath", "", "Path to write local file")
	glanceFile       = flag.String("glanceFile", "", "Glance the first 128*1024 bytes of a specific file from GCS")
	fileNamePrefix   = flag.String("fileNamePrefix", "testCSVWriter", "Base file name")
	deletePrefixFile = flag.String("deletePrefixFile", "", "Delete all files with prefix")
	gcsDir           = flag.String("gcsDir", "gcs://global-sort-dir", "GCS directory")

	batchSize           = flag.Int("batchSize", 10, "Number of rows to generate in each batch")
	generatorNum        = flag.Int("generatorNum", 1, "Number of generator goroutines")
	writerNum           = flag.Int("writerNum", 8, "Number of writer goroutines")
	pkBegin             = flag.Int("pkBegin", 0, "Begin of primary key, [begin, end)")
	pkEnd               = flag.Int("pkEnd", 10, "End of primary key [begin, end)")
	fileNameSuffixStart = flag.Int("fileNameSuffixStart", 0, "Start of file name suffix")
	base64Encode        = flag.Bool("base64Encode", false, "Base64 encode the CSV file")
)

const (
	maxRetries     = 3
	uuidLen        = 36
	maxIndexLen    = 3072
	totalOrdered   = "TOTAL ORDERED"
	totalRandom    = "TOTAL RANDOM"
	partialOrdered = "PARTIAL ORDERED"
)

var faker *gofakeit.Faker

// Initialize Faker instance
func init() {
	faker = gofakeit.New(time.Now().Unix())
}

type Column struct {
	Name     string
	Type     string
	Enum     []string // For ENUM type
	IsPK     bool
	IsUnique bool
	Order    string
}

// Read SQL schema file
func readSQLFile(filename string) (string, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Parse SQL schema and extract columns
func parseSQLSchema(schema string) []Column {
	lines := strings.Split(schema, "\n")
	columns := []Column{}

	hasPk := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Skip empty lines, CREATE TABLE and ");"
		if line == "" || strings.HasPrefix(strings.ToUpper(line), "CREATE TABLE") || strings.HasPrefix(line, ");") {
			continue
		}

		// Remove trailing comma
		line = strings.TrimSuffix(line, ",")

		// Split column definition
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		colName := strings.Trim(parts[0], "`") // Get column name
		colType := strings.ToUpper(parts[1])   // Get data type

		// Handle ENUM type
		var enumValues []string
		if strings.HasPrefix(strings.ToUpper(colType), "ENUM") {
			start := strings.Index(line, "(")
			end := strings.LastIndex(line, ")")
			if start != -1 && end != -1 && end > start {
				enumStr := line[start+1 : end]
				enumStr = strings.ReplaceAll(enumStr, "'", "")
				enumValues = strings.Split(enumStr, ",")
			}
		}
		col := Column{Name: colName, Type: colType, Enum: enumValues}
		if strings.Contains(strings.ToUpper(line), "PRIMARY KEY") && !hasPk {
			hasPk = true
			col.IsPK = true
		}
		if strings.Contains(strings.ToUpper(line), "UNIQUE KEY") &&
			(strings.HasPrefix(col.Type, "VARBINARY") || strings.HasPrefix(col.Type, "VARCHAR")) &&
			extractNumberFromSQLType(colType) > uuidLen &&
			extractNumberFromSQLType(colType) < maxIndexLen {
			col.IsUnique = true
		}
		if strings.Contains(strings.ToUpper(line), totalOrdered) {
			col.Order = totalOrdered
		} else if strings.Contains(strings.ToUpper(line), totalRandom) {
			col.Order = totalRandom
		} else if strings.Contains(strings.ToUpper(line), partialOrdered) {
			col.Order = partialOrdered
		}
		columns = append(columns, col)
	}
	return columns
}

// Extract length from SQL type definition
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
	return -1
}

func generateValueByCol(col Column, num int, res []string) {
	switch {
	case strings.HasPrefix(col.Type, "BIGINT"):
		generateBigint(num, res, col.Order)
	case strings.HasPrefix(col.Type, "TINYINT"):
		generateTinyint1(num, res)
	case strings.HasPrefix(col.Type, "TIMESTAMP"):
		generateTimestamp(num, res)
	case strings.HasPrefix(col.Type, "VARBINARY"):
	case strings.HasPrefix(col.Type, "VARCHAR"):
		generateVarbinary(num, extractNumberFromSQLType(col.Type), res, col.IsUnique)
	case strings.HasPrefix(col.Type, "MEDIUMBLOB"):
		generateMediumblob(num, res)
	default:
		log.Printf("Unsupported type: %s", col.Type)
	}
}

func generateLetterWithNum(len int) string {
	var builder strings.Builder

	len = faker.Number(1, len) // Random length for varbinary
	// If length is less than or equal to 1000, generate directly
	if len <= 1000 {
		builder.WriteString(faker.Regex(fmt.Sprintf("[a-zA-Z0-9]{%d}", len)))
	} else {
		// Generate the first 1000 characters
		builder.WriteString(faker.Regex("[a-zA-Z0-9]{1000}"))
		// Repeat generation
		for i := 1; i < len/1000; i++ {
			builder.WriteString(builder.String()[:1000])
		}
		// If there is remaining part, append it
		remain := len % 1000
		if remain > 0 {
			builder.WriteString(builder.String()[:remain])
		}
	}
	return builder.String()
}

func generateBigint(num int, res []string, order string) {
	intRes := make([]int, num)
	for i := 0; i < num; i++ {
		intRes[i] = faker.Number(-9223372036854775808, 9223372036854775807) // https://docs.pingcap.com/zh/tidb/stable/data-type-numeric#bigint-%E7%B1%BB%E5%9E%8B
	}

	if order == totalOrdered {
		sort.Ints(intRes)
	} else if order == partialOrdered {
		sort.Ints(intRes[:len(res)-1])
	}
	if len(res) < num {
		res = make([]string, num)
	}
	for i, v := range intRes {
		res[i] = strconv.Itoa(v)
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
	end := time.Now()
	for i := 0; i < num; i++ {
		randomTime := faker.DateRange(start, end)
		res[i] = randomTime.Format("2006-01-02 15:04:05")
	}
}

// Generate primary key values for range [begin, end)
func generatePrimaryKey(begin, end int, res []string) {
	idx := 0
	for key := begin; key < end; key++ {
		res[idx] = strconv.Itoa(key)
		// Uncomment the following line for debugging:
		// log.Printf("Primary key at index %d has value %d", idx, key)
		idx++
	}
}

// Write data to GCS with retry (column-oriented)
func writeDataToGCS(store storage.ExternalStorage, fileName string, data [][]string) error {
	writer, err := store.Create(context.Background(), fileName, nil)
	if err != nil {
		return fmt.Errorf("failed to create GCS file: %w", err)
	}
	defer writer.Close(context.Background())

	for i := 0; i < len(data[0]); i++ {
		row := make([]string, 0, len(data[0]))
		for j := 0; j < len(data); j++ {
			if *base64Encode {
				row = append(row, base64.StdEncoding.EncodeToString([]byte(data[j][i])))
			} else {
				row = append(row, data[j][i])
			}
		}
		_, err = writer.Write(context.Background(), []byte(strings.Join(row, ",")+"\n"))
		if err != nil {
			log.Printf("Write to GCS failed, deleting file: %s", fileName)
			store.DeleteFile(context.Background(), fileName) // Delete the file if write fails
			return fmt.Errorf("failed to write to GCS: %w", err)
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
	dirSize := 0.0
	dirFileNum := 0
	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		fSize := float64(size) / 1024 / 1024
		log.Printf("Name: %s, Size: %d, Size (MiB): %f", path, size, fSize)
		dirSize += fSize
		dirFileNum++
		return nil
	})
	log.Printf("Total file Num: %d  Total size: %.2f MiB, %.2f GiB, %.2f TiB", dirFileNum, dirSize, dirSize/1024, dirSize/1024/1024)
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

// Write CSV to local disk (column-oriented)
func writeCSVToLocalDisk(filename string, columns []Column, data [][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV data
	for i := 0; i < len(data[0]); i++ {
		row := []string{}
		for j := 0; j < len(data); j++ {
			if *base64Encode {
				row = append(row, base64.StdEncoding.EncodeToString([]byte(data[j][i])))
			} else {
				row = append(row, data[j][i])
			}
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

// Task represents a task with a [begin, end) range indicating the number of rows to generate
type Task struct {
	id       int
	begin    int
	end      int
	cols     []Column
	fileName string
}

// Result represents the generated result containing task id and the generated random strings
type Result struct {
	id       int
	fileName string
	values   [][]string
}

// generatorWorker retrieves tasks from tasksCh, reuses [][]string slices via sync.Pool, and sends generated results to resultsCh
func generatorWorker(tasksCh <-chan Task, resultsCh chan<- Result, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range tasksCh {
		startTime := time.Now()
		colNum := len(task.cols)
		count := task.end - task.begin
		// Try to get a [][]string slice from the pool
		buf := pool.Get().([][]string)
		if cap(buf) != colNum {
			buf = make([][]string, colNum)
		}
		for i := range buf {
			if len(buf[i]) != count {
				buf[i] = make([]string, count)
			}
		}
		// Set the length of the slice to count
		values := buf[:colNum]
		for i, col := range task.cols {
			if col.IsPK {
				generatePrimaryKey(task.begin, task.end, values[i])
			} else {
				generateValueByCol(col, count, values[i])
			}
		}
		log.Printf("Generator %d: Processed %s, primary key range [%d, %d), generated %d rows, elapsed time: %v",
			workerID, task.fileName, task.begin, task.end, count, time.Since(startTime))
		resultsCh <- Result{id: task.id, values: values, fileName: task.fileName}
	}
}

// writerWorker retrieves generated results from resultsCh, writes them to CSV (or GCS), and puts used slices back to pool
func writerWorker(resultsCh <-chan Result, store storage.ExternalStorage, workerID int, pool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	for result := range resultsCh {
		success := false
		fileName := result.fileName
		// Retry mechanism
		for attempt := 1; attempt <= maxRetries; attempt++ {
			startTime := time.Now()
			if *localPath != "" {
				err = writeCSVToLocalDisk(*localPath+fileName, nil, result.values)
				if err != nil {
					log.Fatal("Error writing CSV:", err)
				}
			} else {
				err = writeDataToGCS(store, fileName, result.values)
			}
			if err == nil {
				log.Printf("Writer %d: Wrote %s (%d rows), elapsed time: %v", workerID, fileName, len(result.values[0]), time.Since(startTime))
				success = true
				break
			}
			log.Printf("Writer %d: Attempt %d to write to GCS failed: %v", workerID, attempt, err)
			// Exponential backoff: wait for 2^(attempt-1)*100ms (max 4s)
			waitTime := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond
			if waitTime > 4*time.Second {
				waitTime = 4 * time.Second
			}
			time.Sleep(waitTime + time.Duration(rand.Intn(500))*time.Millisecond)
		}
		if !success {
			log.Printf("Writer %d: Final write failed for %s (%d rows)", workerID, fileName, len(result.values))
		}
		// Return the used slice to the pool for reuse
		pool.Put(result.values)
	}
}

func main() {
	// Parse command-line arguments.
	flag.Parse()

	// List files in GCS directory if showFile is true
	if *showFile {
		showFiles(*credentialPath)
		return
	}

	// Delete specified file if deleteFileName is provided
	if *deleteFileName != "" {
		deleteFile(*credentialPath, *deleteFileName)
		return
	}

	// Delete all files with the specified prefix
	if *deletePrefixFile != "" {
		deleteAllFilesByPrefix(*deletePrefixFile)
		return
	}

	// Glance at the first 128*1024 bytes of the specified file if glanceFile is provided
	if *glanceFile != "" {
		glanceFiles(*credentialPath, *glanceFile)
		return
	}

	rowCount := *pkEnd - *pkBegin
	log.Printf("Configuration: credential=%s, template=%s, generatorNum=%d, writerNum=%d, rowCount=%d, batchSize=%d",
		*credentialPath, *templatePath, *generatorNum, *writerNum, rowCount, *batchSize)

	// Read SQL schema
	sqlSchema, err := readSQLFile(*templatePath)
	if err != nil {
		log.Fatalf("Failed to read SQL template: %v", err)
	}

	// Parse schema
	columns := parseSQLSchema(sqlSchema)

	// Check primary key range
	if rowCount%*batchSize != 0 {
		log.Fatal("pkEnd - pkBegin must be a multiple of batchSize")
	}

	if rowCount <= 0 || *batchSize <= 0 {
		log.Fatal("Row count and batchSize must be greater than 0")
	}
	taskCount := (rowCount + *batchSize - 1) / *batchSize
	log.Printf("Total tasks: %d, each task generates at most %d rows", taskCount, *batchSize)

	// Create tasks and results channels
	tasksCh := make(chan Task, taskCount)
	resultsCh := make(chan Result, taskCount)

	// Create a sync.Pool for reusing [][]string slices, initial capacity equals number of columns
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
	// Start generator workers
	for i := 0; i < *generatorNum; i++ {
		wgGen.Add(1)
		go generatorWorker(tasksCh, resultsCh, i, pool, &wgGen)
	}

	var wgWriter sync.WaitGroup
	// Start writer workers
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: *credentialPath}}
	s, err := storage.ParseBackend(*gcsDir, &op)
	if err != nil {
		panic(err)
	}
	var store storage.ExternalStorage
	if *localPath == "" {
		store, err = storage.NewWithDefaultOpt(context.Background(), s)
		if err != nil {
			panic(err)
		}
	}
	for i := 0; i < *writerNum; i++ {
		wgWriter.Add(1)
		go writerWorker(resultsCh, store, i, pool, &wgWriter)
	}

	// Divide tasks according to [begin, end) range and send to tasksCh
	startTime := time.Now()
	taskID := *fileNameSuffixStart
	var fileNames []string

	for pk := *pkBegin; pk < *pkEnd; pk += *batchSize {
		begin := pk
		end := pk + *batchSize
		csvFileName := fmt.Sprintf("%s.%09d.csv", *fileNamePrefix, taskID)
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
	close(tasksCh) // Close tasksCh after distributing tasks

	// Wait for all generators to finish then close resultsCh
	wgGen.Wait()
	close(resultsCh)

	// Wait for all writers to finish writing
	wgWriter.Wait()
	log.Printf("GCS write completed, total time: %v", time.Since(startTime))
	if *localPath == "" {
		showFiles(*credentialPath)
	}

	if *deleteAfterWrite {
		for _, fileName := range fileNames {
			deleteFile(*credentialPath, fileName)
		}
		log.Printf("Deleted all files after write")
	}

	log.Printf("Done!")
}
