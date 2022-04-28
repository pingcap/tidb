// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"text/template"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	tcontext "github.com/pingcap/tidb/dumpling/context"
)

// Writer is the abstraction that keep pulling data from database and write to files.
// Every writer owns a snapshot connection, and will try to get a task from task stream chan and work on it.
type Writer struct {
	id         int64
	tctx       *tcontext.Context
	conf       *Config
	conn       *sql.Conn
	extStorage storage.ExternalStorage
	fileFmt    FileFormat

	receivedTaskCount int

	rebuildConnFn       func(*sql.Conn, bool) (*sql.Conn, error)
	finishTaskCallBack  func(Task)
	finishTableCallBack func(Task)
}

// NewWriter returns a new Writer with given configurations
func NewWriter(tctx *tcontext.Context, id int64, config *Config, conn *sql.Conn, externalStore storage.ExternalStorage) *Writer {
	sw := &Writer{
		id:                  id,
		tctx:                tctx,
		conf:                config,
		conn:                conn,
		extStorage:          externalStore,
		finishTaskCallBack:  func(Task) {},
		finishTableCallBack: func(Task) {},
	}
	switch strings.ToLower(config.FileType) {
	case FileFormatSQLTextString:
		sw.fileFmt = FileFormatSQLText
	case FileFormatCSVString:
		sw.fileFmt = FileFormatCSV
	}
	return sw
}

func (w *Writer) setFinishTaskCallBack(fn func(Task)) {
	w.finishTaskCallBack = fn
}

func (w *Writer) setFinishTableCallBack(fn func(Task)) {
	w.finishTableCallBack = fn
}

func countTotalTask(writers []*Writer) int {
	sum := 0
	for _, w := range writers {
		sum += w.receivedTaskCount
	}
	return sum
}

func (w *Writer) run(taskStream <-chan Task) error {
	for {
		select {
		case <-w.tctx.Done():
			w.tctx.L().Info("context has been done, the writer will exit",
				zap.Int64("writer ID", w.id))
			return nil
		case task, ok := <-taskStream:
			if !ok {
				return nil
			}
			w.receivedTaskCount++
			err := w.handleTask(task)
			if err != nil {
				return err
			}
			w.finishTaskCallBack(task)
		}
	}
}

func (w *Writer) handleTask(task Task) error {
	switch t := task.(type) {
	case *TaskDatabaseMeta:
		return w.WriteDatabaseMeta(t.DatabaseName, t.CreateDatabaseSQL)
	case *TaskTableMeta:
		return w.WriteTableMeta(t.DatabaseName, t.TableName, t.CreateTableSQL)
	case *TaskViewMeta:
		return w.WriteViewMeta(t.DatabaseName, t.ViewName, t.CreateTableSQL, t.CreateViewSQL)
	case *TaskSequenceMeta:
		return w.WriteSequenceMeta(t.DatabaseName, t.SequenceName, t.CreateSequenceSQL)
	case *TaskPolicyMeta:
		return w.WritePolicyMeta(t.PolicyName, t.CreatePolicySQL)
	case *TaskTableData:
		err := w.WriteTableData(t.Meta, t.Data, t.ChunkIndex)
		if err != nil {
			return err
		}
		if t.ChunkIndex+1 == t.TotalChunks {
			w.finishTableCallBack(task)
		}
		return nil
	default:
		w.tctx.L().Warn("unsupported writer task type", zap.String("type", fmt.Sprintf("%T", t)))
		return nil
	}
}

// WritePolicyMeta writes database meta to a file
func (w *Writer) WritePolicyMeta(policy, createSQL string) error {
	tctx, conf := w.tctx, w.conf
	fileName, err := (&outputFileNamer{Policy: policy}).render(conf.OutputFileTemplate, outputFileTemplatePolicy)
	if err != nil {
		return err
	}
	return writeMetaToFile(tctx, "placement-policy", createSQL, w.extStorage, fileName+".sql", conf.CompressType)
}

// WriteDatabaseMeta writes database meta to a file
func (w *Writer) WriteDatabaseMeta(db, createSQL string) error {
	tctx, conf := w.tctx, w.conf
	fileName, err := (&outputFileNamer{DB: db}).render(conf.OutputFileTemplate, outputFileTemplateSchema)
	if err != nil {
		return err
	}
	return writeMetaToFile(tctx, db, createSQL, w.extStorage, fileName+".sql", conf.CompressType)
}

// WriteTableMeta writes table meta to a file
func (w *Writer) WriteTableMeta(db, table, createSQL string) error {
	tctx, conf := w.tctx, w.conf
	fileName, err := (&outputFileNamer{DB: db, Table: table}).render(conf.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	return writeMetaToFile(tctx, db, createSQL, w.extStorage, fileName+".sql", conf.CompressType)
}

// WriteViewMeta writes view meta to a file
func (w *Writer) WriteViewMeta(db, view, createTableSQL, createViewSQL string) error {
	tctx, conf := w.tctx, w.conf
	fileNameTable, err := (&outputFileNamer{DB: db, Table: view}).render(conf.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	fileNameView, err := (&outputFileNamer{DB: db, Table: view}).render(conf.OutputFileTemplate, outputFileTemplateView)
	if err != nil {
		return err
	}
	err = writeMetaToFile(tctx, db, createTableSQL, w.extStorage, fileNameTable+".sql", conf.CompressType)
	if err != nil {
		return err
	}
	return writeMetaToFile(tctx, db, createViewSQL, w.extStorage, fileNameView+".sql", conf.CompressType)
}

// WriteSequenceMeta writes sequence meta to a file
func (w *Writer) WriteSequenceMeta(db, sequence, createSQL string) error {
	tctx, conf := w.tctx, w.conf
	fileName, err := (&outputFileNamer{DB: db, Table: sequence}).render(conf.OutputFileTemplate, outputFileTemplateSequence)
	if err != nil {
		return err
	}
	return writeMetaToFile(tctx, db, createSQL, w.extStorage, fileName+".sql", conf.CompressType)
}

// WriteTableData writes table data to a file with retry
func (w *Writer) WriteTableData(meta TableMeta, ir TableDataIR, currentChunk int) error {
	tctx, conf, conn := w.tctx, w.conf, w.conn
	retryTime := 0
	var lastErr error
	return utils.WithRetry(tctx, func() (err error) {
		defer func() {
			lastErr = err
			if err != nil {
				IncCounter(errorCount, conf.Labels)
			}
		}()
		retryTime++
		tctx.L().Debug("trying to dump table chunk", zap.Int("retryTime", retryTime), zap.String("db", meta.DatabaseName()),
			zap.String("table", meta.TableName()), zap.Int("chunkIndex", currentChunk), zap.NamedError("lastError", lastErr))
		// don't rebuild connection when dump for the first time
		if retryTime > 1 {
			conn, err = w.rebuildConnFn(conn, true)
			w.conn = conn
			if err != nil {
				return
			}
		}
		err = ir.Start(tctx, conn)
		if err != nil {
			return
		}
		if conf.SQL != "" {
			meta, err = setTableMetaFromRows(ir.RawRows())
			if err != nil {
				return err
			}
		}
		defer ir.Close()
		return w.tryToWriteTableData(tctx, meta, ir, currentChunk)
	}, newRebuildConnBackOffer(canRebuildConn(conf.Consistency, conf.TransactionalConsistency)))
}

func (w *Writer) tryToWriteTableData(tctx *tcontext.Context, meta TableMeta, ir TableDataIR, curChkIdx int) error {
	conf, format := w.conf, w.fileFmt
	namer := newOutputFileNamer(meta, curChkIdx, conf.Rows != UnspecifiedSize, conf.FileSize != UnspecifiedSize)
	fileName, err := namer.NextName(conf.OutputFileTemplate, w.fileFmt.Extension())
	if err != nil {
		return err
	}

	somethingIsWritten := false
	for {
		fileWriter, tearDown := buildInterceptFileWriter(tctx, w.extStorage, fileName, conf.CompressType)
		n, err := format.WriteInsert(tctx, conf, meta, ir, fileWriter)
		tearDown(tctx)
		if err != nil {
			return err
		}

		if w, ok := fileWriter.(*InterceptFileWriter); ok && !w.SomethingIsWritten {
			break
		}

		tctx.L().Debug("finish dumping table(chunk)",
			zap.String("database", meta.DatabaseName()),
			zap.String("table", meta.TableName()),
			zap.Int("chunkIdx", curChkIdx),
			zap.Uint64("total rows", n))
		somethingIsWritten = true

		if conf.FileSize == UnspecifiedSize {
			break
		}
		fileName, err = namer.NextName(conf.OutputFileTemplate, w.fileFmt.Extension())
		if err != nil {
			return err
		}
	}
	if !somethingIsWritten {
		tctx.L().Info("no data written in table chunk",
			zap.String("database", meta.DatabaseName()),
			zap.String("table", meta.TableName()),
			zap.Int("chunkIdx", curChkIdx))
	}
	return nil
}

func writeMetaToFile(tctx *tcontext.Context, target, metaSQL string, s storage.ExternalStorage, path string, compressType storage.CompressType) error {
	fileWriter, tearDown, err := buildFileWriter(tctx, s, path, compressType)
	if err != nil {
		return errors.Trace(err)
	}
	defer tearDown(tctx)

	return WriteMeta(tctx, &metaData{
		target:  target,
		metaSQL: metaSQL,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}, fileWriter)
}

type outputFileNamer struct {
	ChunkIndex int
	FileIndex  int
	Policy     string
	DB         string
	Table      string
	format     string
}

type csvOption struct {
	nullValue string
	separator []byte
	delimiter []byte
}

func newOutputFileNamer(meta TableMeta, chunkIdx int, rows, fileSize bool) *outputFileNamer {
	o := &outputFileNamer{
		DB:    meta.DatabaseName(),
		Table: meta.TableName(),
	}
	o.ChunkIndex = chunkIdx
	o.FileIndex = 0
	switch {
	case rows && fileSize:
		o.format = "%09d%04d"
	case fileSize:
		o.format = "%09[2]d"
	default:
		o.format = "%09[1]d"
	}
	return o
}

func (namer *outputFileNamer) render(tmpl *template.Template, subName string) (string, error) {
	var bf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&bf, subName, namer); err != nil {
		return "", errors.Trace(err)
	}
	return bf.String(), nil
}

func (namer *outputFileNamer) Index() string {
	return fmt.Sprintf(namer.format, namer.ChunkIndex, namer.FileIndex)
}

func (namer *outputFileNamer) NextName(tmpl *template.Template, fileType string) (string, error) {
	res, err := namer.render(tmpl, outputFileTemplateData)
	namer.FileIndex++
	return res + "." + fileType, err
}
