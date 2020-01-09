package export

import (
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/dumpling/v4/log"
	"go.uber.org/zap"
)

func WriteMeta(meta MetaIR, w io.StringWriter) error {
	log.Zap().Debug("start dumping meta data", zap.String("target", meta.TargetName()))

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	if err := write(w, fmt.Sprintf("%s;\n", meta.MetaSQL())); err != nil {
		return err
	}

	log.Zap().Debug("finish dumping meta data", zap.String("target", meta.TargetName()))
	return nil
}

func WriteInsert(tblIR TableDataIR, w io.StringWriter) error {
	rowIter := tblIR.Rows()
	if !rowIter.HasNext() {
		return nil
	}

	log.Zap().Debug("start dumping for table", zap.String("table", tblIR.TableName()))
	specCmtIter := tblIR.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	tblName := wrapBackTicks(tblIR.TableName())
	if err := write(w, fmt.Sprintf("INSERT INTO %s VALUES\n", tblName)); err != nil {
		return err
	}

	for rowIter.HasNext() {
		row := MakeRowReceiver(tblIR.ColumnTypes())
		if err := rowIter.Next(row); err != nil {
			log.Zap().Error("scanning from sql.Row failed", zap.String("error", err.Error()))
			return err
		}

		if err := write(w, row.ToString()); err != nil {
			return err
		}

		var splitter string
		if rowIter.HasNext() {
			splitter = ","
		} else {
			splitter = ";"
		}
		if err := write(w, fmt.Sprintf("%s\n", splitter)); err != nil {
			return err
		}
	}
	log.Zap().Debug("finish dumping for table", zap.String("table", tblIR.TableName()))
	return nil
}

func write(writer io.StringWriter, str string) error {
	_, err := writer.WriteString(str)
	if err != nil {
		log.Zap().Error("writing failed",
			zap.String("string", str),
			zap.String("error", err.Error()))
	}
	return err
}

func wrapBackTicks(identifier string) string {
	if !strings.HasPrefix(identifier, "`") && !strings.HasSuffix(identifier, "`") {
		return wrapStringWith(identifier, "`")
	}
	return identifier
}

func wrapStringWith(str string, wrapper string) string {
	return fmt.Sprintf("%s%s%s", wrapper, str, wrapper)
}
