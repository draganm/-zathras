package topic

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"github.com/draganm/zathras/limiter"
	"github.com/draganm/zathras/segment"
)

type relativeSegment struct {
	*segment.Segment
	startAddress uint64
}

func (r relativeSegment) nextAddress() uint64 {
	return r.Segment.FileSize() + r.startAddress
}

func (r relativeSegment) containsAddress(a uint64) bool {
	return a >= r.startAddress && a < r.nextAddress()
}

func (r relativeSegment) Append(d []byte) (uint64, uint64, error) {
	a, na, err := r.Segment.Append(d)
	if err != nil {
		return a, na, err
	}
	return a + r.startAddress, na + r.startAddress, nil
}

func (r relativeSegment) Read(address uint64) ([]byte, uint64, error) {
	d, na, err := r.Segment.Read(address - r.startAddress)
	if err != nil {
		return d, na, err
	}
	return d, na + r.startAddress, nil
}

type segmentList []relativeSegment

func (s segmentList) Len() int           { return len(s) }
func (s segmentList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s segmentList) Less(i, j int) bool { return s[i].startAddress < s[j].startAddress }

// Topic represents a Zathras topic
type Topic struct {
	sync.RWMutex
	dir            string
	segmentSize    uint64
	oldSegments    segmentList
	currentSegment relativeSegment
	subscribers    map[uintptr](chan uint64)
	nextAddress    uint64
	limiter        *limiter.Limiter
}

// ErrTooLargeEvent is returned when event size (plus size of header) is larger
// than maximal size of a single segment.
var ErrTooLargeEvent = errors.New("Event can't fit into a signle segment.")

var segmentMatcher = regexp.MustCompile(`^(?P<startAddress>[0-9a-z]{16}).seg$`)

// New creates a new topic that uses specified directory and max segment size
func New(dir string, segmentSize uint64) (*Topic, error) {

	files, err := ioutil.ReadDir(dir)

	if err != nil {
		return nil, err
	}

	segments := segmentList{}

	for _, fi := range files {
		if !fi.IsDir() {
			name := fi.Name()
			groups := segmentMatcher.FindStringSubmatch(name)
			if groups != nil {
				fileName := filepath.Join(dir, name)
				var startAddress uint64
				startAddress, err = strconv.ParseUint(groups[1], 16, 64)
				if err != nil {
					return nil, err
				}
				var s *segment.Segment
				s, err = segment.New(fileName, segmentSize)
				if err != nil {
					return nil, err
				}

				segments = append(segments, relativeSegment{s, startAddress})
			}
		}
	}

	if len(segments) == 0 {
		startAddress := uint64(0)

		fileName := filepath.Join(dir, fmt.Sprintf("%016x.seg", startAddress))

		var s *segment.Segment

		s, err = segment.New(fileName, segmentSize)
		if err != nil {
			return nil, err
		}

		segments = append(segments, relativeSegment{s, startAddress})

	}

	sort.Sort(segments)

	oldSegments := segments[:len(segments)-1]

	currentSegment := segments[len(segments)-1]

	if err != nil {
		return nil, err
	}

	nextAddress := currentSegment.startAddress + currentSegment.FileSize()

	t := &Topic{
		dir:            dir,
		segmentSize:    segmentSize,
		currentSegment: currentSegment,
		oldSegments:    oldSegments,
		nextAddress:    nextAddress,
		subscribers:    map[uintptr](chan uint64){},
		limiter:        limiter.New(nextAddress),
	}

	go t.broadcast()

	return t, nil
}

func (t *Topic) broadcast() {
	current := uint64(0)
	for {
		var err error
		current, err = t.limiter.WaitForCurrentToBeGreaterThan(current)
		if err != nil {
			// limiter closed -> close all subscribers
			for _, c := range t.subscribers {
				close(c)
			}
			return
		}

		for _, c := range t.subscribers {
			select {
			case c <- current:
				// whatever
			}
		}
	}
}

// WriteEvent writes an event to the topic and returns eventID or error
func (t *Topic) WriteEvent(data []byte) (uint64, error) {
	if uint64(len(data)) > t.segmentSize {
		return 0, ErrTooLargeEvent
	}
	t.Lock()
	defer t.Unlock()
	address, nextAddress, err := t.currentSegment.Append(data)

	// if too large then create a new segment
	if err == segment.ErrDataTooLarge {
		nextAddress = t.currentSegment.nextAddress()
		fileName := filepath.Join(t.dir, fmt.Sprintf("%016x.seg", nextAddress))
		var ns *segment.Segment
		ns, err = segment.New(fileName, t.segmentSize)
		if err != nil {
			return 0, err
		}
		t.oldSegments = append(t.oldSegments, t.currentSegment)
		t.currentSegment = relativeSegment{ns, nextAddress}
		address, nextAddress, err = t.currentSegment.Append(data)
		if err != nil {
			return 0, err
		}
	}

	t.limiter.UpdateCurrent(nextAddress)

	return address, nil
}

func (t *Topic) firstAddress() uint64 {
	if len(t.oldSegments) == 0 {
		return t.currentSegment.startAddress
	}
	return t.oldSegments[0].startAddress
}

func (t *Topic) lastAddress() uint64 {
	return t.currentSegment.nextAddress()
}

func (t *Topic) ReadEvents(fn func(uint64, []byte) error) error {
	t.RLock()
	defer t.RUnlock()
	lastAddress := t.lastAddress()
	currentAddres := t.firstAddress()
	var data []byte
	var err error
	for currentAddres < lastAddress {
		data, currentAddres, err = t.Read(currentAddres)
		if err != nil {
			return err
		}
		err = fn(currentAddres, data)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close closes all open segments
func (t *Topic) Close() error {
	t.Lock()
	defer t.Unlock()
	for _, s := range t.oldSegments {
		err := s.Close()
		if err != nil {
			return err
		}
	}
	t.limiter.Close()
	return t.currentSegment.Close()
}

func (t *Topic) Read(address uint64) ([]byte, uint64, error) {
	t.RLock()
	defer t.RUnlock()
	if t.currentSegment.containsAddress(address) {
		return t.currentSegment.Read(address)
	}
	for _, s := range t.oldSegments {
		if s.containsAddress(address) {
			return s.Read(address)
		}
	}
	return nil, 0, segment.ErrWrongAddress
}

// Subscribe returns two channels: First one is used to read events.
// Second channel is used to signal (by closing) that listener is no longer inerested on the events.
// From parameter defines from which event ID the IDs should be sent.
func (t *Topic) Subscribe(from uint64, s Subscriber) {
	t.Lock()
	defer t.Unlock()

	ac := make(chan uint64, 1)

	ac <- t.lastAddress()

	ptr := reflect.ValueOf(s).Pointer()

	_, found := t.subscribers[ptr]
	if found {
		return
	}

	t.subscribers[ptr] = ac

	go func() {
		defer t.Unsubscribe(s)
		// defer unregistering listener
		currentAddress := from
		for lastAddress := range ac {
			for currentAddress < lastAddress {
				data, nextAddress, err := t.Read(currentAddress)
				if err != nil {
					log.Println("Subscriber reading error", err)
					return
				}
				err = s.OnEvent(nextAddress, data)
				if err != nil {
					log.Println("Subscriber error", err)
					return
				}
				currentAddress = nextAddress
			}
		}

	}()

}

func (t *Topic) Unsubscribe(s Subscriber) {
	t.Lock()
	defer t.Unlock()
	ptr := reflect.ValueOf(s).Pointer()
	delete(t.subscribers, ptr)
}
