// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/dumpling/log"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
	"github.com/xitongsys/parquet-go/writer"

	"go.uber.org/zap"
)

const (
	TagTemplate            = "name=%s, type=%s, repetitiontype=%s"
	DefaultCompressionType = parquet.CompressionCodec_ZSTD
	parquetMagicNumber     = "PAR1"
	parquetParallelNumber  = 4
)

func WriteInsertInParquet(
	pCtx *tcontext.Context,
	cfg *Config,
	meta TableMeta,
	tblIR TableDataIR,
	w storage.ExternalFileWriter,
	metrics *metrics,
) (n uint64, err error) {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return 0, fileRowIter.Error()
	}

	parquetLengthLimit := int(cfg.ParquetRowGroupSize)

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < parquetLengthLimit {
		bf.Grow(parquetLengthLimit - bfCap)
	}

	// parquet need to get more information from tableMeta
	writer, err := NewParquetWriter(bf, meta.ColumnInfos(), cfg)
	if err != nil {
		return 0, errors.Trace(err)
	}
	wp := newWriterPipe(w, cfg.FileSize, UnspecifiedSize, metrics, cfg.Labels)
	// use context.Background here to make sure writerPipe can deplete all the chunks in pipeline
	ctx, cancel := tcontext.Background().WithLogger(pCtx.L()).WithCancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wp.Run(ctx)
		wg.Done()
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	var (
		row            = MakeRowReceiver(meta.ColumnTypes())
		counter        uint64
		lastCounter    uint64
		selectedFields = meta.SelectedField()
	)

	defer func() {
		if err != nil {
			pCtx.L().Warn("fail to dumping table(chunk), will revert some metrics and start a retry if possible",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", lastCounter),
				zap.Uint64("finished size", wp.finishedFileSize),
				log.ShortError(err))
			SubGauge(metrics.finishedRowsGauge, float64(lastCounter))
			SubGauge(metrics.finishedSizeGauge, float64(wp.finishedFileSize))
		} else {
			pCtx.L().Debug("finish dumping table(chunk)",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", counter),
				zap.Uint64("finished size", wp.finishedFileSize))
			summary.CollectSuccessUnit(summary.TotalBytes, 1, wp.finishedFileSize)
			summary.CollectSuccessUnit("total rows", 1, counter)
		}
	}()

	// add magic number length
	wp.currentFileSize += uint64(bf.Len())
	// add row to parquet writer, it will flush to buffer when reach row group size
	for fileRowIter.HasNext() {
		lastBfSize := bf.Len()
		if selectedFields != "" {
			if err = fileRowIter.Decode(row); err != nil {
				return counter, errors.Trace(err)
			}
			err = writer.WriteRow(*row)
			if err != nil {
				return counter, errors.Trace(err)
			}
		}
		counter++
		// buffer size only increase when parquet writer flush occurs
		wp.currentFileSize += uint64(bf.Len() - lastBfSize)
		if bf.Len() >= parquetLengthLimit {
			select {
			case <-pCtx.Done():
				return counter, pCtx.Err()
			case err = <-wp.errCh:
				return counter, err
			case wp.input <- bf:
				bf = pool.Get().(*bytes.Buffer)
				if bfCap := bf.Cap(); bfCap < parquetLengthLimit {
					bf.Grow(parquetLengthLimit - bfCap)
				}
				writer.PFile = buffer.BufferFile{
					Writer: bf,
				}
				AddGauge(metrics.finishedRowsGauge, float64(counter-lastCounter))
				lastCounter = counter
			}
		}
		fileRowIter.Next()
		if wp.ShouldSwitchFile() {
			break
		}
	}

	// write remain data and meta file
	if err = writer.WriteStop(); err != nil {
		return counter, errors.Trace(err)
	}
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	AddGauge(metrics.finishedRowsGauge, float64(counter-lastCounter))
	lastCounter = counter
	if err = fileRowIter.Error(); err != nil {
		return counter, errors.Trace(err)
	}
	return counter, wp.Error()
}

func getParquetCompress(compress ParquetCompressType) parquet.CompressionCodec {
	switch compress {
	case NoCompression:
		return parquet.CompressionCodec_UNCOMPRESSED
	case Gzip:
		return parquet.CompressionCodec_GZIP
	case Snappy:
		return parquet.CompressionCodec_SNAPPY
	case Zstd:
		return parquet.CompressionCodec_ZSTD
	default:
		return DefaultCompressionType
	}
}

type SQLWriter struct {
	writer.ParquetWriter
}

func NewParquetWriter(bf *bytes.Buffer, columns []*ColumnInfo, cfg *Config) (*SQLWriter, error) {
	compress := getParquetCompress(cfg.ParquetCompressType)
	pageSize := cfg.ParquetPageSize
	rowGroupSize := cfg.ParquetRowGroupSize

	res := new(SQLWriter)
	res.PFile = buffer.BufferFile{
		Writer: bf,
	}
	schemaHandler, err := NewSchemaHandlerFromSQL(columns)
	if err != nil {
		return nil, err
	}
	res.SchemaHandler = schemaHandler
	res.NP = parquetParallelNumber
	res.PageSize = pageSize
	res.RowGroupSize = rowGroupSize
	res.CompressionType = compress
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.DictRecs = make(map[string]*layout.DictRecType)
	res.ColumnIndexes = make([]*parquet.ColumnIndex, 0)
	res.OffsetIndexes = make([]*parquet.OffsetIndex, 0)
	res.MarshalFunc = marshal.MarshalCSV
	// footer
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	// add magic number
	magicNumber := []byte(parquetMagicNumber)
	bf.Write([]byte(parquetMagicNumber))
	res.Offset = int64(len(magicNumber))
	return res, err
}

// WriteRow writes a row to the parquet format
func (w *SQLWriter) WriteRow(r RowReceiverArr) error {
	var err error
	sqlRaws := r.GetRawBytes()
	rec := make([]any, len(sqlRaws))
	for i := 0; i < len(sqlRaws); i++ {
		rec[i] = nil
		if sqlRaws[i] != nil {
			rec[i], err = convertDataToParquet(sqlRaws[i],
				w.SchemaHandler.SchemaElements[i+1].Type,
				w.SchemaHandler.SchemaElements[i+1].LogicalType)
			if err != nil {
				return err
			}
		}
	}
	return w.Write(rec)
}

// convertDataToParquet some codes references parquet-go types.StrToParquetType()
func convertDataToParquet(data []byte, pT *parquet.Type, lT *parquet.LogicalType) (any, error) {
	s := string(data)
	// convert with logical type
	if lT != nil && lT.DECIMAL != nil {
		numSca := big.NewFloat(1.0)
		for i := 0; i < int(lT.DECIMAL.Scale); i++ {
			numSca.Mul(numSca, big.NewFloat(10))
		}
		num := new(big.Float)
		num.SetString(s)
		num.Mul(num, numSca)

		if *pT == parquet.Type_INT32 {
			tmp, _ := num.Int64()
			return int32(tmp), nil
		} else if *pT == parquet.Type_INT64 {
			tmp, _ := num.Int64()
			return tmp, nil
		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			s = num.Text('f', 0)
			// only used by unsigned big int
			res := types.StrIntToBinary(s, "BigEndian", 9, true)
			return res, nil
		}
	}
	// TiDB may return invalid timestamp/datetime such as "0000-00-00 00:00:00" or "0001-00-00 00:00:00"
	// In this case, we will return nil directly
	if lT != nil && lT.TIMESTAMP != nil {
		s, err := time.Parse(time.DateTime, s)
		if err != nil {
			return nil, nil
		}
		return s.UnixMicro(), nil
	}
	// convert with primitive type
	if *pT == parquet.Type_BOOLEAN {
		var v bool
		_, err := fmt.Sscanf(s, "%t", &v)
		return v, err
	} else if *pT == parquet.Type_INT32 {
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	} else if *pT == parquet.Type_INT64 {
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	} else if *pT == parquet.Type_FLOAT {
		var v float32
		_, err := fmt.Sscanf(s, "%f", &v)
		return v, err
	} else if *pT == parquet.Type_DOUBLE {
		var v float64
		_, err := fmt.Sscanf(s, "%f", &v)
		return v, err
	} else if *pT == parquet.Type_BYTE_ARRAY {
		return s, nil

	} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		return s, nil
	}
	return nil, fmt.Errorf("unsupported type %v", pT)

}

// NewSchemaHandlerFromSQL build tag and reuse the NewSchemaHandlerFromMetadata to create the schema handler
func NewSchemaHandlerFromSQL(columns []*ColumnInfo) (*schema.SchemaHandler, error) {
	tags := make([]string, 0, len(columns))
	for _, column := range columns {
		primitive, logical := ToParquetType(column)
		repetitionType := parquet.FieldRepetitionType_REQUIRED
		// set TIMESTAMP and DATETIME to optional because we will set null for those invalid values
		// such as: 0000-00-00 00:00:00 or 0001-00-00 00:00:00
		if column.Nullable || column.Type == "TIMESTAMP" || column.Type == "DATETIME" {
			repetitionType = parquet.FieldRepetitionType_OPTIONAL
		}
		tag := fmt.Sprintf(TagTemplate,
			column.Name,
			primitive.String(),
			repetitionType.String())
		if logical != "" {
			tag = fmt.Sprintf("%s, %s", tag, logical)
		}
		tags = append(tags, tag)
	}
	// use tag to define the schema
	schemaHandler, err := schema.NewSchemaHandlerFromMetadata(tags)
	if err != nil {
		return nil, err
	}
	return schemaHandler, nil
}

// ToParquetType converts database type to parquet type
func ToParquetType(columnInfo *ColumnInfo) (parquet.Type, string) {
	// TiDB Type
	columnType := columnInfo.Type
	switch columnType {
	case "CHAR", "VARCHAR", "DATE", "TIME", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "SET", "JSON", "VECTOR":
		return parquet.Type_BYTE_ARRAY, "logicaltype=STRING"
	case "ENUM":
		return parquet.Type_BYTE_ARRAY, "logicaltype=STRING"
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY", "BIT":
		return parquet.Type_BYTE_ARRAY, ""
	case "TIMESTAMP", "DATETIME":
		return parquet.Type_INT64, "logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"
	case "YEAR", "TINYINT", "SMALLINT", "MEDIUMINT", "UNSIGNED TINYINT", "UNSIGNED SMALLINT", "UNSIGNED MEDIUMINT", "INT":
		return parquet.Type_INT32, ""
	case "BIGINT", "UNSIGNED INT":
		return parquet.Type_INT64, ""
	case "DECIMAL":
		// add converted type for backward compatibility
		logicalType := fmt.Sprintf("logicaltype=DECIMAL, logicaltype.precision=%d, logicaltype.scale=%d, convertedtype=DECIMAL, precision=%d, scale=%d",
			columnInfo.Precision, columnInfo.Scale, columnInfo.Precision, columnInfo.Scale)
		if columnInfo.Precision <= 9 {
			return parquet.Type_INT32, logicalType
		}
		// int64 has 19 digits, so it can store 18 digits decimal
		if columnInfo.Precision <= 18 {
			return parquet.Type_INT64, logicalType
		}
		return parquet.Type_BYTE_ARRAY, "logicaltype=STRING"
	case "UNSIGNED BIGINT":
		// add converted type for backward compatibility
		return parquet.Type_FIXED_LEN_BYTE_ARRAY, "length=9, logicaltype=DECIMAL, logicaltype.precision=20, logicaltype.scale=0, convertedtype=DECIMAL, precision=20, scale=0"
	case "FLOAT":
		return parquet.Type_FLOAT, ""
	case "DOUBLE":
		return parquet.Type_DOUBLE, ""
	}

	// Other database, like MariaDB
	switch columnType {
	case "NCHAR", "NVARCHAR", "CHARACTER", "VARCHARACTER", "SQL_TSI_YEAR", "NULL", "VAR_STRING", "GEOMETRY", "LONG":
		return parquet.Type_BYTE_ARRAY, ""
	case "INTEGER", "INT1", "INT2", "INT3":
		return parquet.Type_INT32, ""
	case "INT8":
		return parquet.Type_INT64, ""
	case "BOOL", "BOOLEAN":
		return parquet.Type_BOOLEAN, ""
	case "REAL", "DOUBLE", "DOUBLE PRECISION", "NUMERIC", "FIXED":
		return parquet.Type_DOUBLE, ""
	}
	return parquet.Type_BYTE_ARRAY, ""
}
