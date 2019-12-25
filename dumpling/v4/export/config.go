package export

import (
	"fmt"
)

type Config struct {
	Database string
	Host     string
	User     string
	Port     int
	Password string
	Threads  int

	Logger        Logger
	FileSize      uint64
	OutputDirPath string
}

func DefaultConfig() *Config {
	return &Config{
		Database:      "",
		Host:          "127.0.0.1",
		User:          "root",
		Port:          3306,
		Password:      "",
		Threads:       4,
		Logger:        &DummyLogger{},
		FileSize:      UnspecifiedSize,
		OutputDirPath: ".",
	}
}

func (conf *Config) getDSN(db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conf.User, conf.Password, conf.Host, conf.Port, db)
}

func extractOutputConfig(conf *Config) *Config {
	return &Config{
		Logger:        conf.Logger,
		FileSize:      conf.FileSize,
		OutputDirPath: conf.OutputDirPath,
	}
}

type WriteConfig struct {
	// Logger is used to log the export routine.
	Logger Logger
	// Output size limit in bytes.
	OutputSize int
	// OutputDirPath is the directory to output.
	OutputDirPath string
}

const UnspecifiedSize = 0
