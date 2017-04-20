package topic_test

import (
	"errors"
	"io/ioutil"
	"os"

	"github.com/draganm/zathras/topic"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestTopic(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Topic Suite")
}

var _ = Describe("Topic", func() {
	var topicDir string

	BeforeEach(func() {
		var err error
		topicDir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		if topicDir != "" {
			Expect(os.RemoveAll(topicDir)).To(Succeed())
		}
	})

	var t *topic.Topic

	BeforeEach(func() {
		var err error
		t, err = topic.New(topicDir, 1024, 0)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Removing existing segments", func() {
		BeforeEach(func() {
			Expect(t.Close()).To(Succeed())
			var err error
			t, err = topic.New(topicDir, 1024, 1)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When a subscription is open", func() {

			var events <-chan topic.Event
			var cl chan interface{}

			BeforeEach(func(done Done) {
				events, cl = t.Subscribe(0)
				close(done)
			})

			Context("When the first segment should not be retained", func() {
				BeforeEach(func(done Done) {
					_, err := t.WriteEvent(make([]byte, 256))
					Expect(err).ToNot(HaveOccurred())

					_, err = t.WriteEvent(make([]byte, 256))
					Expect(err).ToNot(HaveOccurred())

					_, err = t.WriteEvent(make([]byte, 256))
					Expect(err).ToNot(HaveOccurred())

					_, err = t.WriteEvent(make([]byte, 512))
					Expect(err).ToNot(HaveOccurred())

					close(done)
				})

				AfterEach(func() {
					close(cl)
				})

				It("Should receive first and last event", func(done Done) {
					evt := <-events
					Expect(evt.ID).To(Equal(uint64(0)))
					close(done)
				})
			})

		})

		Context("When the first segment should not be retained", func() {
			BeforeEach(func() {
				_, err := t.WriteEvent(make([]byte, 256))
				Expect(err).ToNot(HaveOccurred())
				_, err = t.WriteEvent(make([]byte, 256))
				Expect(err).ToNot(HaveOccurred())
				_, err = t.WriteEvent(make([]byte, 256))
				Expect(err).ToNot(HaveOccurred())
				_, err = t.WriteEvent(make([]byte, 512))
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should be deleted", func() {
				files, err := ioutil.ReadDir(topicDir)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(files)).To(Equal(1))
			})

			It("The topic should contain the rest of the messages", func() {
				t.ReadEvents(func(seq uint64, data []byte) error {
					Expect(seq).To(Equal(uint64(3)))
					Expect(len(data)).To(Equal(512))
					return nil
				})
			})
		})

	})

	Describe("WriteEvent()", func() {
		Context("when data length with header is lower than segment size", func() {
			var eventID uint64
			var err error
			BeforeEach(func() {
				eventID, err = t.WriteEvent([]byte("test"))
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return segmentID", func() {
				Expect(eventID).To(Equal(uint64(0)))
			})

		})

	})

	Describe("ReadEvents()", func() {
		Context("When there is one event", func() {
			BeforeEach(func() {
				_, err := t.WriteEvent([]byte("test"))
				Expect(err).To(Succeed())
			})
			Context("When event callback returns error", func() {
				var err error
				var callbackErr error
				BeforeEach(func() {
					callbackErr = errors.New("x")
					err = t.ReadEvents(func(id uint64, data []byte) error {
						return callbackErr
					})
				})

				It("Should return the same error", func() {
					Expect(err).To(Equal(callbackErr))
				})
			})
		})

		Context("When there are no events", func() {
			It("Should not return error", func() {
				Expect(t.ReadEvents(func(id uint64, data []byte) error {
					return nil
				})).To(Succeed())
			})
		})

	})

	Describe("Multiple segments", func() {
		Context("When first segment is full", func() {
			BeforeEach(func() {
				_, err := t.WriteEvent(make([]byte, 1024-12))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I write another event", func() {
				var segmentID uint64
				var err error
				BeforeEach(func() {
					segmentID, err = t.WriteEvent([]byte("test"))

				})

				It("Should not fail", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should return correct segmentID", func() {
					Expect(segmentID).To(Equal(uint64(1)))
				})

				It("Should create a new segment file", func() {
					files, err := ioutil.ReadDir(topicDir)
					Expect(err).ToNot(HaveOccurred())
					Expect(len(files)).To(Equal(2))
				})

				Context("When I read all values", func() {
					It("Should read both values", func() {
						count := 0
						t.ReadEvents(func(uint64, []byte) error {
							count++
							return nil
						})

						Expect(count).To(Equal(2))

					})
				})

				Context("When I close existing and create a new topic pointing to the same directory", func() {
					BeforeEach(func() {
						Expect(t.Close()).To(Succeed())
						var err error
						t, err = topic.New(topicDir, 1024, 0)
						Expect(err).ToNot(HaveOccurred())
					})

					It("Should retain existing data", func() {
						count := 0
						t.ReadEvents(func(uint64, []byte) error {
							count++
							return nil
						})

						Expect(count).To(Equal(2))

					})
				})

			})

		})
	})

	Describe("Subscribe()", func() {
		Context("When there is one event in the topic", func() {
			BeforeEach(func() {
				t.WriteEvent([]byte("test"))
			})
			Context("When I subscribe to the topic", func() {
				var s <-chan topic.Event
				var c chan interface{}
				BeforeEach(func() {
					s, c = t.Subscribe(0)
				})
				It("The event channel should contain the first event", func(done Done) {
					Expect(<-s).To(Equal(topic.Event{0, []byte("test")}))
					close(done)
				})
				Context("When another event is written to the topic", func() {
					BeforeEach(func() {
						id, err := t.WriteEvent([]byte("test2"))
						Expect(err).ToNot(HaveOccurred())
						Expect(id).To(Equal(uint64(1)))
					})
					It("The event channel should contain both events", func(done Done) {
						Expect(<-s).To(Equal(topic.Event{0, []byte("test")}))
						Expect(<-s).To(Equal(topic.Event{1, []byte("test2")}))
						close(done)
					})

					Context("When I close the subscription", func() {
						BeforeEach(func() {
							close(c)
						})
						It("Close the channel", func() {
							select {
							case <-s:
								Fail("Channel should be closed")
							default:
							}
						})
					})

				})
			})
		})
	})

})
