package event_test

import (
	"bytes"
	"time"

	"github.com/draganm/zathras/event"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestEvent(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Event Suite")
}

var _ = Describe("Event", func() {
	Describe("Write()", func() {
		var evt event.Event

		BeforeEach(func() {
			evt = event.Event{1, time.Unix(1, 1), []byte("test")}
		})

		It("Should write data prefixed with length", func() {
			buf := &bytes.Buffer{}
			err := evt.Write(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(buf.Bytes()).To(Equal([]byte{0x0, 0x0, 0x0, 0x18, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x3b, 0x9a, 0xca, 0x1, 0x74, 0x65, 0x73, 0x74}))
		})

	})
})
