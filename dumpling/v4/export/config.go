package export

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/coreos/go-semver/semver"
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
	ServerInfo    ServerInfo
	SortByPk      bool
	Tables        DatabaseTables
	Snapshot      string
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
		ServerInfo:    ServerInfoUnknown,
		SortByPk:      false,
		Tables:        nil,
		Snapshot:      "",
	}
}

func (conf *Config) getDSN(db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conf.User, conf.Password, conf.Host, conf.Port, db)
}

const UnspecifiedSize = 0

type ServerInfo struct {
	ServerType    ServerType
	ServerVersion *semver.Version
}

var ServerInfoUnknown = ServerInfo{
	ServerType:    ServerTypeUnknown,
	ServerVersion: nil,
}

var versionRegex = regexp.MustCompile(`^\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
var tidbVersionRegex = regexp.MustCompile(`v\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)

func ParseServerInfo(src string) ServerInfo {
	lowerCase := strings.ToLower(src)
	serverInfo := ServerInfo{}
	if strings.Contains(lowerCase, "tidb") {
		serverInfo.ServerType = ServerTypeTiDB
	} else if strings.Contains(lowerCase, "mariadb") {
		serverInfo.ServerType = ServerTypeMariaDB
	} else if versionRegex.MatchString(lowerCase) {
		serverInfo.ServerType = ServerTypeMySQL
	} else {
		serverInfo.ServerType = ServerTypeUnknown
	}

	var versionStr string
	if serverInfo.ServerType == ServerTypeTiDB {
		versionStr = tidbVersionRegex.FindString(src)[1:]
	} else {
		versionStr = versionRegex.FindString(src)
	}

	var err error
	serverInfo.ServerVersion, err = semver.NewVersion(versionStr)
	if err != nil {
		return serverInfo
	}
	return serverInfo
}

type ServerType int8

const (
	ServerTypeUnknown = iota
	ServerTypeMySQL
	ServerTypeMariaDB
	ServerTypeTiDB

	ServerTypeAll
)

type databaseName = string

type tableName = string

type DatabaseTables map[databaseName][]tableName
