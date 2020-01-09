package export

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
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
	sw := &SimpleWriter{cfg: config}
	return sw, os.MkdirAll(config.OutputDirPath, 0755)
}

func (f *SimpleWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s-schema-create.sql", db))
	fsStringWriter := NewFileSystemStringWriter(fileName, false)
	meta := &metaData{
		target:  db,
		metaSQL: createSQL,
	}
	return WriteMeta(meta, fsStringWriter)
}

func (f *SimpleWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s.%s-schema.sql", db, table))
	fsStringWriter := NewFileSystemStringWriter(fileName, false)
	meta := &metaData{
		target:  table,
		metaSQL: createSQL,
	}
	return WriteMeta(meta, fsStringWriter)
}

func (f *SimpleWriter) WriteTableData(ctx context.Context, ir TableDataIR) error {
	if f.cfg.FileSize == UnspecifiedSize {
		fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s.%s.sql", ir.DatabaseName(), ir.TableName()))
		fsStringWriter := NewFileSystemStringWriter(fileName, true)
		return WriteInsert(ir, fsStringWriter)
	}

	ir = splitTableDataIntoChunks(ir, f.cfg.FileSize)
	for chunkCount := 0; ; /* loop */ chunkCount += 1 {
		fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s.%s.%3d.sql", ir.DatabaseName(), ir.TableName(), chunkCount))
		fsStringWriter := newInterceptStringWriter(NewFileSystemStringWriter(fileName, true))
		err := WriteInsert(ir, fsStringWriter)
		if err != nil {
			return err
		}
		if fsStringWriter.writeNothingYet {
			break
		}
	}
	return nil
}

type FileSystemStringWriter struct {
	path string

	file *os.File
	once sync.Once
	err  error
}

func (w *FileSystemStringWriter) initFileHandle() {
	w.file, w.err = os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY, 0755)
}

func (w *FileSystemStringWriter) WriteString(str string) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	w.once.Do(w.initFileHandle)
	return w.file.WriteString(str)
}

func NewFileSystemStringWriter(path string, lazyHandleCreation bool) *FileSystemStringWriter {
	w := &FileSystemStringWriter{path: path}
	if !lazyHandleCreation {
		w.once.Do(w.initFileHandle)
	}
	return w
}

type interceptStringWriter struct {
	sw              io.StringWriter
	writeNothingYet bool
}

func (i *interceptStringWriter) WriteString(str string) (int, error) {
	writtenBytes, err := i.sw.WriteString(str)
	if writtenBytes != 0 {
		i.writeNothingYet = false
	}
	return writtenBytes, err
}

func newInterceptStringWriter(sw io.StringWriter) *interceptStringWriter {
	return &interceptStringWriter{
		sw:              sw,
		writeNothingYet: true,
	}
}
