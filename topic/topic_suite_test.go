package topic_test

import (
	"io/ioutil"
	"os"
	"time"

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
		t, err = topic.New(topicDir, 1024)
		Expect(err).ToNot(HaveOccurred())
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

	Describe("Read()", func() {
		Context("When there is one event", func() {
			var a uint64
			BeforeEach(func() {
				var err error
				a, err = t.WriteEvent([]byte("test"))
				Expect(err).To(Succeed())
			})

			It("Should return that event's data", func() {
				data, nextAddr, err := t.Read(a)
				Expect(err).ToNot(HaveOccurred())
				Expect(nextAddr).To(Equal(uint64(8)))
				Expect(data).To(Equal([]byte("test")))
			})
		})

	})

	Describe("Multiple segments", func() {
		Context("When first segment is full", func() {
			BeforeEach(func() {
				_, err := t.WriteEvent(make([]byte, 1024-4))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I write another event", func() {
				var eventAddress uint64
				var err error
				BeforeEach(func() {
					eventAddress, err = t.WriteEvent([]byte("test"))

				})

				It("Should not fail", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should return correct event address", func() {
					Expect(eventAddress).To(Equal(uint64(1024)))
				})

				It("Should create a new segment file", func() {
					files, err := ioutil.ReadDir(topicDir)
					Expect(err).ToNot(HaveOccurred())
					Expect(len(files)).To(Equal(2))
				})

				It("Should retain existing data", func() {
					count := 0
					t.ReadEvents(func(a uint64, d []byte) error {
						count++
						return nil
					})

					Expect(count).To(Equal(2))

				})

				Context("When I close existing and create a new topic pointing to the same directory", func() {
					BeforeEach(func() {
						Expect(t.Close()).To(Succeed())
						var err error
						t, err = topic.New(topicDir, 1024)
						Expect(err).ToNot(HaveOccurred())
					})

					It("Should retain existing data", func() {
						count := 0
						t.ReadEvents(func(a uint64, d []byte) error {
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
		var s chan topic.Event
		BeforeEach(func() {
			s = make(chan topic.Event)
		})

		var subscriber topic.SubscriberFunc
		BeforeEach(func() {
			subscriber = topic.SubscriberFunc(func(nextAddress uint64, data []byte) error {
				s <- topic.Event{NextAddress: nextAddress, Data: data}
				return nil
			})

		})
		Context("When I unsubscribe", func() {
			BeforeEach(func() {
				t.Unsubscribe(subscriber)
			})

			It("Should not send any notifications to the subscriber", func(done Done) {
				t.WriteEvent([]byte("test3"))
				timeout := time.NewTimer(100 * time.Millisecond)
				select {
				case <-timeout.C:
				case <-s:
					Fail("Should not receive a message")
				}
				close(done)
			})
		})

		Context("When there is one event in the topic", func() {
			BeforeEach(func() {
				t.WriteEvent([]byte("test"))
			})
			Context("When I subscribe to the topic", func() {
				BeforeEach(func() {
					t.Subscribe(0, subscriber)
				})
				It("The event channel should contain the first event", func(done Done) {
					Expect(<-s).To(Equal(topic.Event{8, []byte("test")}))
					close(done)
				})
				Context("When another event is written to the topic", func() {
					BeforeEach(func() {
						addr, err := t.WriteEvent([]byte("test2"))
						Expect(err).ToNot(HaveOccurred())
						Expect(addr).To(Equal(uint64(8)))
					})
					It("The event channel should contain both events", func(done Done) {
						Expect(<-s).To(Equal(topic.Event{8, []byte("test")}))
						Expect(<-s).To(Equal(topic.Event{17, []byte("test2")}))
						close(done)
					})

				})
			})
		})
	})

})
