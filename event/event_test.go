package event_test

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/draganm/zathras/event"
)

func BenchmarkWrite(b *testing.B) {
	evt := &event.Event{
		ID:   1,
		Time: time.Now(),
		Data: make([]byte, 1024),
	}
	for n := 0; n < b.N; n++ {
		evt.Write(ioutil.Discard)
	}
}
