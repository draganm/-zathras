package segment_test

import (
	"io/ioutil"
	"os"

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
	var segmentFileName string
	var s *segment.Segment
	BeforeEach(func() {

		segmentFile, err := ioutil.TempFile("", "")
		Expect(err).ToNot(HaveOccurred())
		segmentFileName = segmentFile.Name()
		segmentFile.Close()
		err = os.Remove(segmentFileName)
		Expect(err).ToNot(HaveOccurred())

		s, err = segment.New(segmentFileName, 1024)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		Expect(s.Close()).To(Succeed())
		Expect(os.Remove(segmentFileName)).To(Succeed())
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
