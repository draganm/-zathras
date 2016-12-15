package appender

import (
	"bufio"
	"os"
	"time"

	"github.com/draganm/zathras/event"
)

// LogAppender is writer of events to disk
type LogAppender struct {
	file           *os.File
	bufferedWriter *bufio.Writer
	nextID         uint64
	eventCount     uint64
}

// NewLogAppender creates a new log appender
func NewLogAppender(file *os.File, initialID uint64) *LogAppender {
	return &LogAppender{
		file:           file,
		nextID:         initialID,
		bufferedWriter: bufio.NewWriter(file),
	}
}

// AppendEvent appends a new event
func (l *LogAppender) AppendEvent(data []byte) (uint64, error) {
	id := l.nextID
	evt := event.Event{ID: id, Time: time.Now(), Data: data}
	err := evt.Write(l.bufferedWriter)
	if err != nil {
		return 0, err
	}

	err = l.bufferedWriter.Flush()
	if err != nil {
		return 0, err
	}

	l.nextID++
	l.eventCount++
	return id, nil
}

// Sync flushes written data
func (l *LogAppender) Sync() error {
	return l.file.Sync()
}

// EventCount returns the number of event written with this instane of appender.
func (l *LogAppender) EventCount() uint64 {
	return l.eventCount
}
