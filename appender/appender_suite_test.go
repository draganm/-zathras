package appender_test

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/draganm/zathras/appender"
	"github.com/draganm/zathras/event"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestAppender(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Appender Suite")
}

var _ = Describe("LogAppender", func() {
	var logFile *os.File
	var ap *appender.LogAppender
	BeforeEach(func() {
		var err error
		logFile, err = ioutil.TempFile("", "")
		Expect(err).ToNot(HaveOccurred())
		ap = appender.NewLogAppender(logFile)
	})
	AfterEach(func() {
		Expect(logFile.Close()).To(Succeed())
		Expect(os.Remove(logFile.Name())).To(Succeed())
	})

	It("Should append events to log", func() {
		evt := &event.Event{ID: 1, Time: time.Now(), Data: []byte("test")}
		Expect(ap.Append(evt)).To(Succeed())
		Expect(logFile.Sync()).To(Succeed())
		stat, err := logFile.Stat()
		Expect(err).ToNot(HaveOccurred())
		Expect(stat.Size()).To(Equal(33))
	})
})
