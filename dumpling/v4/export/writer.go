package export

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/pingcap/errors"

	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

type Writer interface {
	WriteDatabaseMeta(ctx context.Context, db, createSQL string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
	WriteTableData(ctx context.Context, ir TableDataIR) error
}

type SimpleWriter struct {
	cfg *Config
}

func NewSimpleWriter(config *Config) (Writer, error) {
	if config.Sql != "" {
		log.Error("unsupported dump data in sql format", zap.String("sql", config.Sql))
		return nil, errors.New("unsupported dump data in sql format when specific sql")
	}
	sw := &SimpleWriter{cfg: config}
	return sw, os.MkdirAll(config.OutputDirPath, 0755)
}

func (f *SimpleWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	fileName := fmt.Sprintf("%s-schema-create.sql", db)
	filePath := path.Join(f.cfg.OutputDirPath, fileName)
	return writeMetaToFile(db, createSQL, filePath)
}

func (f *SimpleWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fileName := fmt.Sprintf("%s.%s-schema.sql", db, table)
	filePath := path.Join(f.cfg.OutputDirPath, fileName)
	return writeMetaToFile(db, createSQL, filePath)
}

func (f *SimpleWriter) WriteTableData(ctx context.Context, ir TableDataIR) error {
	log.Debug("start dumping table...", zap.String("table", ir.TableName()))

	chunkIndex := ir.ChunkIndex()
	fileName := fmt.Sprintf("%s.%s.%d.sql", ir.DatabaseName(), ir.TableName(), ir.ChunkIndex())
	// just let `database.table.sql` be `database.table.0.sql`
	/*if fileName == "" {
		// set initial file name
		fileName = fmt.Sprintf("%s.%s.sql", ir.DatabaseName(), ir.TableName())
		if f.cfg.FileSize != UnspecifiedSize {
			fileName = fmt.Sprintf("%s.%s.%d.sql", ir.DatabaseName(), ir.TableName(), 0)
		}
	}*/
	chunksIter := buildChunksIter(ir, f.cfg.FileSize, f.cfg.StatementSize)
	defer chunksIter.Rows().Close()

	for {
		filePath := path.Join(f.cfg.OutputDirPath, fileName)
		fileWriter, tearDown := buildInterceptFileWriter(filePath)
		err := WriteInsert(ctx, chunksIter, fileWriter)
		tearDown()
		if err != nil {
			return err
		}

		if w, ok := fileWriter.(*InterceptFileWriter); ok && !w.SomethingIsWritten {
			break
		}

		if f.cfg.FileSize == UnspecifiedSize {
			break
		}
		chunkIndex += 1
		fileName = fmt.Sprintf("%s.%s.%d.sql", ir.DatabaseName(), ir.TableName(), chunkIndex)
	}
	log.Debug("dumping table successfully",
		zap.String("table", ir.TableName()))
	return nil
}

func writeMetaToFile(target, metaSQL, path string) error {
	fileWriter, tearDown, err := buildFileWriter(path)
	if err != nil {
		return err
	}
	defer tearDown()

	return WriteMeta(&metaData{
		target:  target,
		metaSQL: metaSQL,
	}, fileWriter)
}

type CsvWriter struct {
	cfg *Config
}

func NewCsvWriter(config *Config) (Writer, error) {
	sw := &CsvWriter{cfg: config}
	return sw, os.MkdirAll(config.OutputDirPath, 0755)
}

func (f *CsvWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	fileName := fmt.Sprintf("%s-schema-create.sql", db)
	filePath := path.Join(f.cfg.OutputDirPath, fileName)
	return writeMetaToFile(db, createSQL, filePath)
}

func (f *CsvWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fileName := fmt.Sprintf("%s.%s-schema.sql", db, table)
	filePath := path.Join(f.cfg.OutputDirPath, fileName)
	return writeMetaToFile(db, createSQL, filePath)
}

type outputFileNamer struct {
	chunkIndex int
	dbName     string
	tableName  string
}

func newOutputFileNamer(ir TableDataIR) *outputFileNamer {
	return &outputFileNamer{
		chunkIndex: ir.ChunkIndex(),
		dbName:     ir.DatabaseName(),
		tableName:  ir.TableName(),
	}
}

func (namer *outputFileNamer) NextName() string {
	defer func() { namer.chunkIndex++ }()
	if namer.dbName == "" || namer.tableName == "" {
		return fmt.Sprintf("result.%d", namer.chunkIndex)
	}
	return fmt.Sprintf("%s.%s.%d", namer.dbName, namer.tableName, namer.chunkIndex)
}

func (f *CsvWriter) WriteTableData(ctx context.Context, ir TableDataIR) error {
	log.Debug("start dumping table in csv format...", zap.String("table", ir.TableName()))

	namer := newOutputFileNamer(ir)
	fileName := fmt.Sprintf("%s.csv", namer.NextName())
	chunksIter := buildChunksIter(ir, f.cfg.FileSize, f.cfg.StatementSize)
	defer chunksIter.Rows().Close()

	for {
		filePath := path.Join(f.cfg.OutputDirPath, fileName)
		fileWriter, tearDown := buildInterceptFileWriter(filePath)
		err := WriteInsertInCsv(ctx, chunksIter, fileWriter, f.cfg.NoHeader, f.cfg.CsvNullValue)
		tearDown()
		if err != nil {
			return err
		}

		if w, ok := fileWriter.(*InterceptFileWriter); ok && !w.SomethingIsWritten {
			break
		}

		if f.cfg.FileSize == UnspecifiedSize {
			break
		}
		fileName = fmt.Sprintf("%s.csv", namer.NextName())
	}
	log.Debug("dumping table in csv format successfully",
		zap.String("table", ir.TableName()))
	return nil
}
