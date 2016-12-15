package appender

import (
	"os"

	"github.com/draganm/zathras/event"
)

func NewLogAppender(file *os.File) *LogAppender {
	return &LogAppender{
		file: file,
	}
}

// Appender is writer of events to disk
type LogAppender struct {
	file *os.File
}

func (l *LogAppender) Append(evt *event.Event) error {
	return nil
}
