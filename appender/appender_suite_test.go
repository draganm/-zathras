package appender_test

import (
	"io/ioutil"
	"os"

	"github.com/draganm/zathras/appender"
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
		ap = appender.NewLogAppender(logFile, 0)
	})
	AfterEach(func() {
		Expect(logFile.Close()).To(Succeed())
		Expect(os.Remove(logFile.Name())).To(Succeed())
	})

	It("Should append events to log", func() {
		id, err := ap.AppendEvent([]byte("test"))
		Expect(err).ToNot(HaveOccurred())

		Expect(ap.Sync()).To(Succeed())

		stat, err := logFile.Stat()
		Expect(err).ToNot(HaveOccurred())
		Expect(stat.Size()).To(Equal(int64(24)))
		Expect(id).To(Equal(uint64(0)))
	})
})
