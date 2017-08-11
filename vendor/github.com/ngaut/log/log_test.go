package log

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func isFileExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	if f.IsDir() {
		return false
	}

	return true
}

func parseDate(value string, format string) (time.Time, error) {
	tt, err := time.ParseInLocation(format, value, time.Local)
	if err != nil {
		fmt.Println("[Error]" + err.Error())
		return tt, err
	}

	return tt, nil
}

func checkLogData(fileName string, containData string, num int64) error {
	input, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer input.Close()

	var lineNum int64
	br := bufio.NewReader(input)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		realLine := strings.TrimRight(line, "\n")
		if strings.Contains(realLine, containData) {
			lineNum += 1
		}
	}

	// check whether num is equal to lineNum
	if lineNum != num {
		return fmt.Errorf("checkLogData fail - %d vs %d", lineNum, num)
	}

	return nil
}

func TestDayRotateCase(t *testing.T) {
	_log = New()

	logName := "example_day_test.log"
	if isFileExists(logName) {
		err := os.Remove(logName)
		if err != nil {
			t.Errorf("Remove old log file fail - %s, %s\n", err.Error(), logName)
		}
	}

	SetRotateByDay()
	err := SetOutputByName(logName)
	if err != nil {
		t.Errorf("SetOutputByName fail - %s, %s\n", err.Error(), logName)
	}

	if _log.logSuffix == "" {
		t.Errorf("bad log suffix fail - %s\n", _log.logSuffix)
	}

	day, err := parseDate(_log.logSuffix, FORMAT_TIME_DAY)
	if err != nil {
		t.Errorf("parseDate fail - %s, %s\n", err.Error(), _log.logSuffix)
	}

	_log.Info("Test data")
	_log.Infof("Test data - %s", day.String())

	// mock log suffix to check rotate
	lastDay := day.AddDate(0, 0, -1)
	_log.logSuffix = genDayTime(lastDay)
	oldLogSuffix := _log.logSuffix

	_log.Info("Test new data")
	_log.Infof("Test new data - %s", day.String())

	err = _log.fd.Close()
	if err != nil {
		t.Errorf("close log fd fail - %s, %s\n", err.Error(), _log.fileName)
	}

	// check both old and new log file datas
	oldLogName := logName + "." + oldLogSuffix
	err = checkLogData(oldLogName, "Test data", 2)
	if err != nil {
		t.Errorf("old log file checkLogData fail - %s, %s\n", err.Error(), oldLogName)
	}

	err = checkLogData(logName, "Test new data", 2)
	if err != nil {
		t.Errorf("new log file checkLogData fail - %s, %s\n", err.Error(), logName)
	}

	// remove test log files
	err = os.Remove(oldLogName)
	if err != nil {
		t.Errorf("Remove final old log file fail - %s, %s\n", err.Error(), oldLogName)
	}

	err = os.Remove(logName)
	if err != nil {
		t.Errorf("Remove final new log file fail - %s, %s\n", err.Error(), logName)
	}
}

func TestHourRotateCase(t *testing.T) {
	_log = New()

	logName := "example_hour_test.log"
	if isFileExists(logName) {
		err := os.Remove(logName)
		if err != nil {
			t.Errorf("Remove old log file fail - %s, %s\n", err.Error(), logName)
		}
	}

	SetRotateByHour()
	err := SetOutputByName(logName)
	if err != nil {
		t.Errorf("SetOutputByName fail - %s, %s\n", err.Error(), logName)
	}

	if _log.logSuffix == "" {
		t.Errorf("bad log suffix fail - %s\n", _log.logSuffix)
	}

	hour, err := parseDate(_log.logSuffix, FORMAT_TIME_HOUR)
	if err != nil {
		t.Errorf("parseDate fail - %s, %s\n", err.Error(), _log.logSuffix)
	}

	_log.Info("Test data")
	_log.Infof("Test data - %s", hour.String())

	// mock log suffix to check rotate
	lastHour := hour.Add(time.Duration(-1 * time.Hour))
	_log.logSuffix = genHourTime(lastHour)
	oldLogSuffix := _log.logSuffix

	_log.Info("Test new data")
	_log.Infof("Test new data - %s", hour.String())

	err = _log.fd.Close()
	if err != nil {
		t.Errorf("close log fd fail - %s, %s\n", err.Error(), _log.fileName)
	}

	// check both old and new log file datas
	oldLogName := logName + "." + oldLogSuffix
	err = checkLogData(oldLogName, "Test data", 2)
	if err != nil {
		t.Errorf("old log file checkLogData fail - %s, %s\n", err.Error(), oldLogName)
	}

	err = checkLogData(logName, "Test new data", 2)
	if err != nil {
		t.Errorf("new log file checkLogData fail - %s, %s\n", err.Error(), logName)
	}

	// remove test log files
	err = os.Remove(oldLogName)
	if err != nil {
		t.Errorf("Remove final old log file fail - %s, %s\n", err.Error(), oldLogName)
	}

	err = os.Remove(logName)
	if err != nil {
		t.Errorf("Remove final new log file fail - %s, %s\n", err.Error(), logName)
	}
}
