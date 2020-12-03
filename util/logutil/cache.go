package logutil

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	defaultLoLogFileMetaCacheCapacity = 10000
	zeroTime                          = time.Time{}
)

// InvalidLogFile indicates the log file format is invalid.
var InvalidLogFile = errors.New("invalid format of log file")

type LogFileMetaCache struct {
	mu       sync.RWMutex
	cache    map[string]*LogFileMeta
	capacity int
}

func NewLogFileMetaCache() *LogFileMetaCache {
	return &LogFileMetaCache{
		cache:    make(map[string]*LogFileMeta),
		capacity: defaultLoLogFileMetaCacheCapacity,
	}
}

func (c *LogFileMetaCache) GetFileMata(stat os.FileInfo) *LogFileMeta {
	if stat == nil {
		return nil
	}
	c.mu.RLock()
	m := c.cache[stat.Name()]
	c.mu.RUnlock()
	return m
}

func (c *LogFileMetaCache) AddFileMataToCache(stat os.FileInfo, meta *LogFileMeta) {
	if stat == nil || meta == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	name := stat.Name()
	_, ok := c.cache[name]
	if ok {
		c.cache[name] = meta
	} else {
		// TODO: Use LRU ?
		if len(c.cache) < c.capacity {
			c.cache[name] = meta
		}
	}
}

func (c *LogFileMetaCache) Len() int {
	c.mu.RLock()
	l := len(c.cache)
	c.mu.RUnlock()
	return l
}

type LogFileMeta struct {
	inValid   bool
	mu        sync.Mutex
	ModTime   time.Time
	startTime time.Time
	endTime   time.Time
}

func NewLogFileMeta(info os.FileInfo) *LogFileMeta {
	return &LogFileMeta{
		ModTime: info.ModTime(),
	}
}

func (l *LogFileMeta) SetEndTime(end time.Time) {
	l.endTime = end
}

func (l *LogFileMeta) GetStartTime(stat os.FileInfo, getStartTime func() (time.Time, error)) (time.Time, error) {
	if stat == nil {
		return zeroTime, fmt.Errorf("file stat can't be nil")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.startTime
	if l.CheckLogTimeValid(t) && l.CheckFileNotModified(stat) {
		return t, nil
	}
	if getStartTime == nil {
		return t, fmt.Errorf("can't get file '%v' start time", stat.Name())
	}
	t, err := getStartTime()
	if err != nil {
		if err == InvalidLogFile {
			l.inValid = true
		}
		return t, err
	}
	l.inValid = false
	l.ModTime = stat.ModTime()
	l.startTime = t
	return t, nil
}

func (l *LogFileMeta) GetEndTime(stat os.FileInfo, getEndTime func() (time.Time, error)) (time.Time, error) {
	if stat == nil {
		return zeroTime, fmt.Errorf("file stat can't be nil")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.endTime
	if l.CheckLogTimeValid(t) && l.CheckFileNotModified(stat) {
		return t, nil
	}
	if getEndTime == nil {
		return t, fmt.Errorf("can't get file '%v' end time", stat.Name())
	}
	t, err := getEndTime()
	if err != nil {
		if err == InvalidLogFile {
			l.inValid = true
		}
		return t, err
	}
	l.inValid = false
	l.ModTime = stat.ModTime()
	l.endTime = t
	return t, nil
}

func (l *LogFileMeta) IsInValid() bool {
	l.mu.Lock()
	invalid := l.inValid
	l.mu.Unlock()
	return invalid
}

// CheckLogTimeValid returns true if t != zeroTime.
func (l *LogFileMeta) CheckLogTimeValid(t time.Time) bool {
	return !t.Equal(zeroTime)
}

// CheckFileNotModified returns true if the file hasn't been modified.
func (l *LogFileMeta) CheckFileNotModified(info os.FileInfo) bool {
	return l.ModTime.Equal(info.ModTime())
}
