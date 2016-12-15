package appender

import (
	"os"
	"time"

	"github.com/draganm/zathras/event"
)

func NewLogAppender(file *os.File) *LogAppender {
	return &LogAppender{
		file: file,
	}
}

// Appender is writer of events to disk
type LogAppender struct {
	file   *os.File
	nextID uint64
}

func (l *LogAppender) AppendEvent(data []byte) (uint64, error) {
	id := l.nextID
	evt := event.Event{ID: id, Time: time.Now(), Data: data}
	err := evt.Write(l.file)
	if err != nil {
		return 0, err
	}
	l.nextID++
	return id, nil
}
