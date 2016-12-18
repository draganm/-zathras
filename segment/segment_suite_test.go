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

		s, err = segment.New(segmentFileName, 1024, 0)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		Expect(s.Close()).To(Succeed())
		Expect(os.Remove(segmentFileName)).To(Succeed())
	})

	Describe("Append()", func() {
		Context("A value is appended", func() {

			var id uint64
			var err error
			BeforeEach(func() {
				id, err = s.Append([]byte("test"))
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 0 segment ID", func() {
				Expect(id).To(Equal(uint64(0)))
			})

		})

	})

	Describe("Sync()", func() {
		It("Should sync the file to the disk", func() {
			s.Sync()
		})
	})

	Describe("Read()", func() {

		Context("When data has been appended", func() {

			BeforeEach(func() {
				_, err := s.Append([]byte("test1"))
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should read the appended data", func() {
				read := map[uint64]string{}
				err := s.Read(func(id uint64, data []byte) error {
					read[id] = string(data)
					return nil
				})
				Expect(err).ToNot(HaveOccurred())

				Expect(read).To(Equal(map[uint64]string{0: "test1"}))
			})

			Context("When another value has been appended", func() {
				BeforeEach(func() {
					id, err := s.Append([]byte("test2"))
					Expect(err).ToNot(HaveOccurred())
					Expect(id).To(Equal(uint64(1)))
				})

				It("Should read the appended data", func() {
					read := map[uint64]string{}
					err := s.Read(func(id uint64, data []byte) error {
						read[id] = string(data)
						return nil
					})
					Expect(err).ToNot(HaveOccurred())

					Expect(read).To(Equal(map[uint64]string{0: "test1", 1: "test2"}))
				})

			})
		})

	})

})
