package logutil

import (
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	defaultLoLogFileMetaCacheCapacity = 10000
	zeroTime                          = time.Time{}
)

type LogFileMetaCache struct {
	mu       sync.RWMutex
	cache    map[string]*LogFileMeta
	Capacity int
}

func NewLogFileMetaCache() *LogFileMetaCache {
	return &LogFileMetaCache{
		cache:    make(map[string]*LogFileMeta),
		Capacity: defaultLoLogFileMetaCacheCapacity,
	}
}

func (c *LogFileMetaCache) GetFileMata(stat os.FileInfo) *LogFileMeta {
	if stat == nil {
		return nil
	}
	c.mu.RLock()
	m, ok := c.cache[stat.Name()]
	c.mu.RUnlock()
	if !ok {
		return nil
	}
	if m.checkFileNotModified(stat) {
		return m
	}
	return nil
}

func (c *LogFileMetaCache) AddFileMataToCache(stat os.FileInfo, meta *LogFileMeta) {
	if stat == nil || meta == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// TODO: Use LRU ?
	if len(c.cache) < c.Capacity {
		c.cache[stat.Name()] = meta
	}
}

type FileType = int

const (
	FileTypeUnknown FileType = 0
	FileTypeLog     FileType = 1
	FileTypeSlowLog FileType = 2
)

type LogFileMeta struct {
	mu        sync.Mutex
	Type      FileType
	ModTime   time.Time
	startTime time.Time
	endTime   time.Time
}

func NewLogFileMeta(fileType FileType, info os.FileInfo) *LogFileMeta {
	return &LogFileMeta{
		Type:    fileType,
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
	if l.checkLogTimeValid(t) && l.checkFileNotModified(stat) {
		return t, nil
	}
	if getStartTime == nil {
		return t, fmt.Errorf("can't get file '%v' start time", stat.Name())
	}
	t, err := getStartTime()
	if err != nil {
		return t, err
	}
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
	if l.checkLogTimeValid(t) && l.checkFileNotModified(stat) {
		return t, nil
	}
	if getEndTime == nil {
		return t, fmt.Errorf("can't get file '%v' end time", stat.Name())
	}
	t, err := getEndTime()
	if err != nil {
		return t, err
	}
	l.endTime = t
	return t, nil
}

// checkLogTimeValid returns true if t != zeroTime.
func (l *LogFileMeta) checkLogTimeValid(t time.Time) bool {
	return !t.Equal(zeroTime)
}

// checkFileNotModified returns true if the file hasn't been modified.
func (l *LogFileMeta) checkFileNotModified(info os.FileInfo) bool {
	return l.ModTime.Equal(info.ModTime())
}
