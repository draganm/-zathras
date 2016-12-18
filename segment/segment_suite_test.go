package segment_test

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/draganm/zathras/segment"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestSegment(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Segment Suite")
}

var _ = Describe("Segment", func() {
	var segmentDir string
	var s *segment.Segment
	BeforeEach(func() {
		var err error
		segmentDir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		s, err = segment.New(segmentDir, 1024, time.Now(), 0)
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		Expect(os.RemoveAll(segmentDir)).To(Succeed())
	})

	Describe("Append()", func() {
		Context("When segment is newly created", func() {

			It("Should append data", func() {
				Expect(s.Append([]byte("test"))).To(Succeed())
			})

		})

	})

	Describe("Sync()", func() {
		It("Should sync the file to the disk", func() {
			s.Sync()
		})
	})

	Describe("ReadAll()", func() {

		Context("When data has been appended", func() {

			BeforeEach(func() {
				Expect(s.Append([]byte("test1"))).To(Succeed())
			})

			It("Should read the appended data", func() {
				read := []string{}
				s.ReadAll(func(data []byte) {
					read = append(read, string(data))
				})

				Expect(read).To(Equal([]string{"test1"}))
			})

			Context("When another value has been appended", func() {
				BeforeEach(func() {
					Expect(s.Append([]byte("test2"))).To(Succeed())
				})

				It("Should read the appended data", func() {
					read := []string{}
					s.ReadAll(func(data []byte) {
						read = append(read, string(data))
					})

					Expect(read).To(Equal([]string{"test1", "test2"}))
				})

			})
		})

	})

})
