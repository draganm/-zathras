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
		Context("A value is appended", func() {

			var address uint64
			var nextAddress uint64
			var err error
			BeforeEach(func() {
				address, nextAddress, err = s.Append([]byte("test"))
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return the segment Address", func() {
				Expect(address).To(Equal(uint64(0)))
			})

			It("Should return the next segment Address", func() {
				Expect(nextAddress).To(Equal(uint64(8)))
			})
		})

		Context("When value appended would not fit into the segment", func() {
			var err error
			BeforeEach(func() {
				_, _, err = s.Append(make([]byte, 1025))
			})

			It("Should return ErrDataTooLarge", func() {
				Expect(err).To(Equal(segment.ErrDataTooLarge))
			})

		})

	})

	Describe("Read()", func() {

		Context("When data has been appended", func() {

			BeforeEach(func() {
				var err error
				_, _, err = s.Append([]byte("test1"))
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should read the appended data", func() {
				data, nextAdddress, err := s.Read(0)
				Expect(err).ToNot(HaveOccurred())
				Expect(nextAdddress).To(Equal(uint64(9)))

				Expect(data).To(Equal([]byte("test1")))
			})

		})

	})

})
