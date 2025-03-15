package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"math"
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
	genMaxSizeVal    = flag.Bool("genMaxSizeVal", false, "Generate max size value")

	batchSize           = flag.Int("batchSize", 10, "Number of rows to generate in each batch")
	generatorNum        = flag.Int("generatorNum", 1, "Number of generator goroutines")
	writerNum           = flag.Int("writerNum", 8, "Number of writer goroutines")
	pkBegin             = flag.Int("pkBegin", 0, "Begin of primary key, [begin, end)")
	pkEnd               = flag.Int("pkEnd", 10, "End of primary key [begin, end)")
	fileNameSuffixStart = flag.Int("fileNameSuffixStart", 0, "Start of file name suffix")
	base64Encode        = flag.Bool("base64Encode", false, "Base64 encode the CSV file")
	fetchFile           = flag.String("fetchFile", "", "Fetch a specific file from GCS and write to local disk")
	checkColUniqueness  = flag.Int("checkCol", -1, "Check the uniqueness of a specific column in a CSV file")

	s3Path      = flag.String("s3Path", "gcs://global-sort-dir/testGenerateCSV", "S3 path")
	s3AccessKey = flag.String("s3AccessKey", "", "S3 access key")
	s3SecretKey = flag.String("s3SecretKey", "", "S3 secret key")
	s3Region    = flag.String("s3Region", "", "S3 region")
	s3Provider  = flag.String("s3Provider", "", "S3 provider")
	s3Endpoint  = flag.String("s3Endpoint", "", "S3 endpoint")
)

const (
	maxRetries       = 3
	uuidLen          = 36
	maxIndexLen      = 3072
	totalOrdered     = "TOTAL ORDERED"
	partialOrdered   = "PARTIAL ORDERED"
	totalRandom      = "TOTAL RANDOM"
	defaultNullRatio = 0 // [0, 100], 0 means no null value
	nullVal          = "\\N"
)

var (
	faker        *gofakeit.Faker
	nullRatioMap = map[string]int{}
)

// Initialize Faker instance
func init() {
	seed := time.Now().UnixNano()
	faker = gofakeit.New(seed)
	log.Printf("Faker seed: %d", seed)
	loadColNullRatio()
}

// Column fields is read only
type Column struct {
	Name     string
	Type     string
	Enum     []string // For ENUM type
	IsPK     bool
	IsUnique bool
	Order    string
	Len      int     // varchar(999)
	StdDev   float64 // stdDev=1.0, mean=0.0
	Mean     float64
	NotNull  bool
}

// Read SQL schema file
func readSQLFile(filename string) (string, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func getNullRation(colName string) int {
	if n, ok := nullRatioMap[colName]; ok {
		return n
	}
	return defaultNullRatio
}

func (c *Column) canThisValNull() bool {
	return !c.NotNull && faker.Number(1, 100) <= getNullRation(c.Name)
}

// Parse SQL schema and extract columns
func parseSQLSchema(schema string) []*Column {
	lines := strings.Split(schema, "\n")
	columns := []*Column{}

	//hasPk := false
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
		//if strings.Contains(strings.ToUpper(line), "PRIMARY KEY") && !hasPk {
		if strings.Contains(strings.ToUpper(line), "PRIMARY KEY") {
			//hasPk = true
			col.IsPK = true
		}
		extractLenFromSQL(&col, line)
		extractStdMeanFromSQL(&col, line)
		// check unique key
		if strings.Contains(strings.ToUpper(line), "UNIQUE KEY") &&
			(strings.HasPrefix(col.Type, "VARBINARY") || strings.HasPrefix(col.Type, "VARCHAR")) &&
			col.Len > uuidLen && col.Len < maxIndexLen {
			col.IsUnique = true
		} else if strings.Contains(strings.ToUpper(line), "UNIQUE KEY") && strings.HasPrefix(col.Type, "BIGINT") {
			col.IsUnique = true
		}
		// check unique bigint type ordered or random
		if strings.Contains(strings.ToUpper(line), totalOrdered) {
			col.Order = totalOrdered
		} else if strings.Contains(strings.ToUpper(line), partialOrdered) {
			col.Order = partialOrdered
		} else if strings.Contains(strings.ToUpper(line), totalRandom) {
			col.Order = totalRandom
		}
		// check if not null
		if strings.Contains(strings.ToUpper(line), "NOT NULL") {
			col.NotNull = true
		}
		columns = append(columns, &col)
	}
	return columns
}

// Extract length from SQL type definition
func extractNumberFromStr(sqlType string) int {
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

func extractLenFromSQL(col *Column, s string) {
	if !strings.HasPrefix(col.Type, "VARBINARY") &&
		!strings.HasPrefix(col.Type, "VARCHAR") &&
		!strings.HasPrefix(col.Type, "TEXT") &&
		!strings.HasPrefix(col.Type, "CHAR") {
		return
	}

	// from type
	if l := extractNumberFromStr(col.Type); l != -1 {
		col.Len = l
		return
	}
	// from comment
	strs := strings.Split(s, "--")
	if len(strs) != 2 {
		log.Printf("No comment or Invalid comment: %s", s)
		return
	}
	if l := extractNumberFromStr(strs[1]); l != -1 {
		col.Len = l
	}
}

func extractStdMeanFromSQL(col *Column, s string) {
	if !strings.HasPrefix(col.Type, "INT") {
		return
	}

	// get comment
	strs := strings.Split(s, "--")
	if len(strs) == 1 { // no comment
		return
	}
	if len(strs) != 2 {
		log.Printf("No comment or Invalid comment: %s", s)
		panic("extractStdMeanFromSQL")
	}
	strs = strings.Split(strs[1], ",")
	if len(strs) < 2 {
		log.Printf("Invalid stdDev, mean: %s", s)
		panic("extractStdMeanFromSQL")
	}
	// handle stdDev
	parts := strings.Split(strings.TrimSpace(strs[0]), "=")
	if len(parts) != 2 {
		log.Printf("Invalid stdDev comment: %s", s)
		panic("extractStdMeanFromSQL")
	}
	stdDev, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		log.Printf("Invalid stdDev: %s", s)
		panic(err)
	}
	col.StdDev = stdDev
	// handle mean
	parts = strings.Split(strings.TrimSpace(strs[1]), "=")
	if len(parts) != 2 {
		log.Printf("Invalid mean comment: %s", s)
		panic("extractStdMeanFromSQL")
	}
	mean, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		log.Printf("Invalid mean: %s", s)
		panic(err)
	}
	col.Mean = mean
}

// Generate data for each column
func (t *Task) generateValueByCol(col *Column, num int, res []string) {
	switch {
	case strings.HasPrefix(col.Type, "INT"): // int32
		col.generateInt32(num, res, col.StdDev)
	case strings.HasPrefix(col.Type, "BIGINT"): // int64
		if col.IsUnique {
			col.generateBigint(res, col.Order, t.begin, t.end)
		} else {
			col.generateBigintWithNoLimit(num, res, col.Name)
		}
	case strings.HasPrefix(col.Type, "TINYINT"):
		col.generateTinyint1(num, res)
	case strings.HasPrefix(col.Type, "BOOLEAN"):
		col.generateTinyint1(num, res)
	case strings.HasPrefix(col.Type, "TIMESTAMP"):
		col.generateTimestamp(num, res)
	case strings.HasPrefix(col.Type, "VARBINARY"):
		col.generateVarbinary(num, col.Len, res, col.IsUnique)
	case strings.HasPrefix(col.Type, "VARCHAR"):
		col.generateVarbinary(num, col.Len, res, col.IsUnique)
	case strings.HasPrefix(col.Type, "MEDIUMBLOB"):
		col.generateMediumblob(num, res)
	case strings.HasPrefix(col.Type, "JSON"):
		col.generateJSONObject(num, res)
	case strings.HasPrefix(col.Type, "TEXT"):
		col.generateVarbinary(num, col.Len, res, col.IsUnique)
	case strings.HasPrefix(col.Type, "CHAR"):
		col.generateChar(num, res, col.Len)
	case strings.HasPrefix(col.Type, "DECIMAL"):
		col.generateDecimal(num, res)
	default:
		log.Printf("Unsupported type: %s", col.Type)
	}
}

func generateLetterWithNum(len int, randomLen bool) string {
	var builder strings.Builder

	if randomLen {
		len = faker.Number(0, len) // Random length for varbinary
	}
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

func (c *Column) generateDecimal(num int, res []string) {
	for i := 0; i < num; i++ {
		if c.canThisValNull() {
			res[i] = nullVal
		} else {
			intPart := rand.Int63n(1_000_000_000_000_000_000)
			decimalPart := rand.Intn(1_000_000_000)
			res[i] = fmt.Sprintf("%d.%010d", intPart, decimalPart)
		}
	}
}

func (c *Column) generateBigintWithNoLimit(num int, res []string, colName string) {
	for i := 0; i < num; i++ {
		if c.canThisValNull() {
			res[i] = nullVal
		} else {
			res[i] = strconv.Itoa(faker.Number(math.MinInt64, math.MaxInt64)) // https://docs.pingcap.com/zh/tidb/stable/data-type-numeric#bigint-%E7%B1%BB%E5%9E%8B)
		}
	}
}

func (c *Column) generateBigint(res []string, order string, begin, end int) {
	// WARN: total row num should less than 12 digits, 1000 Billion!
	switch order {
	case totalOrdered:
		generateTotalOrderBigint(res, begin, end)
	case partialOrdered:
		generatePartialOrderBigint(res, begin, end)
	case totalRandom:
		generateTotalRandomBigint(res, begin, end)
	default:
		log.Printf("Unsupported order: %s", order)
	}
}

func generateTotalOrderBigint(res []string, begin, end int) {
	for uk := begin; uk < end; uk++ {
		res[uk-begin] = strconv.Itoa(uk)
	}
}

func generatePartialOrderBigint(res []string, begin, end int) {
	for uk := begin; uk < end; uk++ {
		if uk%1000 == 0 {
			res[uk-begin] = generateUniqueRandomBigint(uk)
		} else {
			res[uk-begin] = strconv.Itoa(uk)
		}
	}
}

func generateTotalRandomBigint(res []string, begin, end int) {
	for uk := begin; uk < end; uk++ {
		res[uk-begin] = generateUniqueRandomBigint(uk)
	}
}

// Total data num should less than 12 digits
// Bigint has almost 19 digits, use 18 digits. The first 6 are random, The last 12 are ordered.
func generateUniqueRandomBigint(uk int) string {
	random := faker.Number(-999_999, 999_999)
	r := fmt.Sprintf("%d%012d", random, uk)
	_, err := strconv.ParseInt(r, 10, 64)
	if err != nil {
		panic(err)
	}
	return r
}

func generateNormalFloat(num int, res []string) {
	intRes := make([]float64, num)
	for i := 0; i < num; i++ {
		intRes[i] = rand.NormFloat64()
	}
}

func (c *Column) generateInt32(num int, res []string, stdDev float64) {
	if stdDev == 0 {
		generateInt32WithNoLimit(num, res)
	} else {
		generateNormalDistributeInt32(num, res, stdDev, 0.0)
	}
}

func generateInt32WithNoLimit(num int, res []string) {
	for i := 0; i < num; i++ {
		res[i] = strconv.Itoa(faker.Number(math.MinInt32, math.MaxInt32))
	}
}

func generateNormalDistributeInt32(num int, res []string, stdDev, mean float64) {
	for i := 0; i < num; i++ {
		randomFloat := rand.NormFloat64()*stdDev + mean
		randomInt := int(math.Round(randomFloat))

		// Limit to int32 range, https://docs.pingcap.com/zh/tidb/stable/data-type-numeric#integer-%E7%B1%BB%E5%9E%8B
		if randomInt > math.MaxInt32 {
			randomInt = math.MaxInt32
		} else if randomInt < math.MinInt32 {
			randomInt = math.MinInt32
		}
		res[i] = strconv.Itoa(randomInt)
	}
}

func (c *Column) generateTinyint1(num int, res []string) {
	for i := 0; i < num; i++ {
		res[i] = strconv.Itoa(faker.Number(0, 1))
	}
}

func (c *Column) generateVarbinary(num, len int, res []string, unique bool) {
	if len <= 0 {
		log.Printf("Invalid length: %d", len)
		return
	}

	for i := 0; i < num; i++ {
		if unique {
			uuid := faker.UUID()
			res[i] = uuid + generateLetterWithNum(len-uuidLen, true)
		} else {
			if c.canThisValNull() {
				res[i] = nullVal
			} else {
				// todo: remove 1024
				res[i] = generateLetterWithNum(len, true)
			}
		}
	}
}

func (c *Column) generateMediumblob(num int, res []string) {
	c.generateVarbinary(num, 73312, res, false)
}

func (c *Column) generateChar(num int, res []string, charLen int) {
	for i := 0; i < num; i++ {
		res[i] = generateLetterWithNum(charLen, false)
	}
}

func (c *Column) generateTimestamp(num int, res []string) {
	start := time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC)
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
		idx++
	}
}

// escapeJSONString escapes special characters in a JSON string for CSV
func escapeJSONString(jsonStr string) string {
	// CSV requires the string to be wrapped with double quotes, and any internal double quotes need to be escaped
	// CSV will automatically escape other characters like newlines and commas inside quoted strings
	return `"` + strings.ReplaceAll(jsonStr, `"`, `""`) + `"`
}

func (c *Column) generateJSONObject(num int, res []string) {
	for i := 0; i < num; i++ {
		r := generateJSON()
		if *localPath == "" {
			res[i] = escapeJSONString(r)
		} else {
			res[i] = r
		}
	}
}

type UserInfo struct {
	User     string   `json:"user"`
	UserID   int      `json:"user_id"`
	Zipcode  []int    `json:"zipcode"`    // more than 1 non-unique int
	UniqueID []string `json:"unique_ids"` // more than 1 unique uuid string
}

func generateJSON() string {
	zipCodeNum := faker.Number(1, 10)
	uniqueIDNum := faker.Number(1, 5)
	user := UserInfo{
		User:     faker.Name(),
		UserID:   faker.Number(1, 100000),
		Zipcode:  make([]int, zipCodeNum),
		UniqueID: make([]string, uniqueIDNum), // unique
	}
	for i := 0; i < zipCodeNum; i++ {
		user.Zipcode[i] = faker.Number(100000, 999999)
	}
	for i := 0; i < uniqueIDNum; i++ {
		user.UniqueID[i] = faker.UUID()
	}

	jsonData, err := json.Marshal(user)
	if err != nil {
		panic(err)
	}
	return string(jsonData)
}

func createExternalStorage() storage.ExternalStorage {
	op := storage.BackendOptions{S3: storage.S3BackendOptions{
		Region:          *s3Region,
		AccessKey:       *s3AccessKey,
		SecretAccessKey: *s3SecretKey,
		Provider:        *s3Provider,
		Endpoint:        *s3Endpoint,
	}}
	s, err := storage.ParseBackend(*s3Path, &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}
	return store
}

// Write data to GCS with retry (column-oriented)
func writeDataToGCS(store storage.ExternalStorage, fileName string, data [][]string) error {
	writer, err := store.Create(context.Background(), fileName, nil)
	if err != nil {
		return fmt.Errorf("failed to create GCS file: %w", err)
	}
	defer writer.Close(context.Background())

	rowCnt := len(data[0])
	colCnt := len(data)
	row := make([]string, colCnt)
	for i := 0; i < rowCnt; i++ {
		for j := 0; j < colCnt; j++ {
			if *base64Encode {
				row[j] = base64.StdEncoding.EncodeToString([]byte(data[j][i]))
			} else {
				row[j] = data[j][i]
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
	// gcs
	//op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}
	// aws
	op := storage.BackendOptions{S3: storage.S3BackendOptions{
		Region:          *s3Region,
		AccessKey:       *s3AccessKey,
		SecretAccessKey: *s3SecretKey,
		Provider:        *s3Provider,
		Endpoint:        *s3Endpoint,
	}}
	s, err := storage.ParseBackend(*s3Path, &op)
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
	// gcs
	//op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}
	// aws
	op := storage.BackendOptions{S3: storage.S3BackendOptions{
		Region:          *s3Region,
		AccessKey:       *s3AccessKey,
		SecretAccessKey: *s3SecretKey,
		Provider:        *s3Provider,
		Endpoint:        *s3Endpoint,
	}}
	s, err := storage.ParseBackend(*s3Path, &op)
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
	s, err := storage.ParseBackend(*s3Path, &op)
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

func fetchFileFromGCS(credentialPath, fileName string) {
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}
	s, err := storage.ParseBackend(*s3Path, &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}
	if exist, _ := store.FileExists(context.Background(), fileName); !exist {
		panic(fmt.Errorf("file %s does not exist", fileName))
	}
	res, err := store.ReadFile(context.Background(), fileName)
	if err != nil {
		panic(err)
	}

	// Open the local file where the content will be written
	file, err := os.Create(*localPath + fileName)
	if err != nil {
		panic(fmt.Errorf("failed to create file %s: %v", localPath, err))
	}
	defer file.Close() // Ensure the file is closed after writing

	// Assuming res contains CSV data as []byte, convert it to string and split by newlines
	// (In case the file is already in CSV format, or you need to write CSV data)
	reader := csv.NewReader(bytes.NewReader(res)) // Read the []byte as CSV
	writer := csv.NewWriter(file)                 // Prepare to write to file
	defer writer.Flush()                          // Ensure data is written to file

	// Read the CSV records from the []byte data
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() != "EOF" {
				panic(fmt.Errorf("failed to read CSV from file: %v", err))
			}
			break
		}
		// Write the CSV record to the file
		if err := writer.Write(record); err != nil {
			panic(fmt.Errorf("failed to write CSV row: %v", err))
		}
	}
	fmt.Printf("File %s successfully fetched and written to %s\n", fileName, *localPath)
}

func checkCSVUniqueness(credentialPath, f string) {
	m := map[uint32]string{}
	op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: credentialPath}}
	s, err := storage.ParseBackend(*s3Path, &op)
	if err != nil {
		panic(err)
	}
	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		panic(err)
	}

	var fileNames []string
	store.WalkDir(context.Background(), &storage.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		fileNames = append(fileNames, path)
		return nil
	})

	for _, fileName := range fileNames {
		fmt.Println("Checking file: ", fileName)
		res, err := store.ReadFile(context.Background(), fileName)
		if err != nil {
			panic(err)
		}
		reader := csv.NewReader(bytes.NewReader(res)) // Read the []byte as CSV
		idx := *checkColUniqueness
		for {
			record, err := reader.Read()
			if err != nil {
				if err.Error() != "EOF" {
					panic(fmt.Errorf("failed to read CSV from file: %v", err))
				}
				break
			}
			//fmt.Printf("Record value: %s\n", record[idx])
			hash := crc32.ChecksumIEEE([]byte(record[idx]))
			if _, ok := m[hash]; !ok {
				m[hash] = fileName
			} else {
				log.Fatal("duplicate value: ", record[idx], " in file: ", fileName, " and file: ", m[hash])
				return
			}
		}
	}
	log.Printf("Check success, no duplicate value")
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
	s, err := storage.ParseBackend(*s3Path, &op)
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
	s, err := storage.ParseBackend(*s3Path, &op)
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
	cols     []*Column
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
		if len(buf) != colNum {
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
				task.generateValueByCol(col, count, values[i])
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

func generateTotalRandomBigintForPk(num int, path string) {
	res := make([]string, num)
	intMap := make(map[int]struct{})
	// generate uniform distribute bigint
	for len(intMap) < num {
		intMap[faker.Number(math.MinInt64, math.MaxInt64)] = struct{}{}
	}
	i := 0
	for v := range intMap {
		res[i] = strconv.Itoa(v)
		i++
	}
	writeCSVToLocalDisk(path+"/uniform_bigint_pk.csv", nil, [][]string{res})
	// generate normal distribute bigint
	res = make([]string, num)
	intMap = make(map[int]struct{})
	for len(intMap) < num {
		randomFloat := rand.NormFloat64()
		randomInt := int(math.Round(randomFloat))
		if randomInt > math.MaxInt64 {
			randomInt = math.MaxInt64
		} else if randomInt < math.MinInt64 {
			randomInt = math.MinInt64
		}
		intMap[randomInt] = struct{}{}
	}
	i = 0
	for v := range intMap {
		res[i] = strconv.Itoa(v)
		i++
	}
	writeCSVToLocalDisk(path+"/normal_bigint_pk.csv", nil, [][]string{res})
}

func loadColNullRatio() {
	path := "/col_null_ratio.csv"
	_, err := os.Stat(path)
	if err != nil {
		if !os.IsExist(err) {
			log.Printf("File %s does not exist", path)
			return
		}
	}

	nr := loadCSVFile(path)
	for _, row := range nr {
		n, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			log.Fatal(err)
		}
		nullRatioMap[row[0]] = int(n)
	}
}

func loadCSVFile(path string) [][]string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	return records
}

func generateMaxSizeValues(cols []*Column) {
	// load max size csv and convert to map
	ms := loadCSVFile("/Users/fanzhou/tcms/pinterest/gcs/richpins/col_max_len.csv")
	maxSizeMap := make(map[string]int)
	for _, row := range ms {
		maxL, err := strconv.ParseInt(row[1], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		maxSizeMap[row[0]] = int(maxL)
	}

	colNum := len(cols)
	repeatNumEveryCol := 10
	colVals := make([][]string, colNum)
	for i, col := range cols {
		colVal := make([]string, colNum*repeatNumEveryCol)
		for j := 0; j < colNum*repeatNumEveryCol; j++ {
			if col.IsPK { // pk
				colVal[j] = strconv.Itoa(*pkBegin + j)
			} else if col.IsUnique { // uk
				colVal[j] = faker.UUID()
			} else {
				colVal[j] = nullVal
			}
		}

		if !col.IsUnique && (strings.Contains(col.Type, "BINARY") || strings.Contains(col.Type, "TEXT")) {
			maxL, ok := maxSizeMap[col.Name]
			if !ok {
				maxL = 100 // average length
				log.Printf("Column %s not found in max size map, using average length", col.Name)
			}
			for j := i * repeatNumEveryCol; j < (i+1)*repeatNumEveryCol; j++ {
				colVal[j] = generateLetterWithNum(maxL, false)
			}
		}
		colVals[i] = colVal
	}
	if *localPath != "" {
		err := writeCSVToLocalDisk(*localPath+"max_size_values.csv", nil, colVals)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		store := createExternalStorage()
		filePath := ""
		err := writeDataToGCS(store, filePath, colVals)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	// Parse command-line arguments.
	flag.Parse()
	//checkCSVUniqueness(*credentialPath, "")
	//return

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

	// Fetch file from GCS if fetchFile is provided
	if *fetchFile != "" {
		if *localPath == "" {
			log.Fatal("localPath must be provided when fetching a file")
		}
		fetchFileFromGCS(*credentialPath, *fetchFile)
		return
	}

	if *checkColUniqueness != -1 {
		checkCSVUniqueness(*credentialPath, "")
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

	// generate max size values and write into csv
	if *genMaxSizeVal {
		generateMaxSizeValues(columns)
		return
	}

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
	// gcs
	//op := storage.BackendOptions{GCS: storage.GCSBackendOptions{CredentialsFile: *credentialPath}}
	// aws
	op := storage.BackendOptions{S3: storage.S3BackendOptions{
		Region:          *s3Region,
		AccessKey:       *s3AccessKey,
		SecretAccessKey: *s3SecretKey,
		Provider:        *s3Provider,
		Endpoint:        *s3Endpoint,
	}}
	s, err := storage.ParseBackend(*s3Path, &op)
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
