package appender

import (
	"os"
	"time"

	"github.com/draganm/zathras/event"
)

// LogAppender is writer of events to disk
type LogAppender struct {
	file         *os.File
	nextID       uint64
	EventCount   uint64
	BytesWritten uint64
}

// NewLogAppender creates a new log appender
func NewLogAppender(file *os.File, initialID uint64) *LogAppender {
	return &LogAppender{
		file:   file,
		nextID: initialID,
	}
}

// AppendEvent appends a new event
func (l *LogAppender) AppendEvent(data []byte) (uint64, error) {
	id := l.nextID
	evt := event.Event{ID: id, Time: time.Now(), Data: data}
	written, err := evt.Write(l.file)
	if err != nil {
		return 0, err
	}

	l.nextID++
	l.EventCount++
	l.BytesWritten += uint64(written)
	return id, nil
}

// Sync flushes written data
func (l *LogAppender) Sync() error {
	return l.file.Sync()
}
