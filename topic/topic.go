package topic

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"github.com/draganm/zathras/segment"
)

type segmentList []*segment.Segment

func (s segmentList) Len() int      { return len(s) }
func (s segmentList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s segmentList) Less(i, j int) bool { return s[i].FirstID < s[j].FirstID }

// Topic represents a Zathras topic
type Topic struct {
	sync.Mutex
	dir             string
	segmentSize     int
	oldSegments     segmentList
	currentSegment  *segment.Segment
	lastIDListeners []chan uint64
}

// ErrTooLargeEvent is returned when event size (plus size of header) is larger
// than maximal size of a single segment.
var ErrTooLargeEvent = errors.New("Event can't fit into a signle segment.")

var segmentMatcher = regexp.MustCompile(`^(?P<firstID>[0-9a-z]{16}).seg$`)

// New creates a new topic that uses specified directory and max segment size
func New(dir string, segmentSize int) (*Topic, error) {

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
				var startID uint64
				startID, err = strconv.ParseUint(groups[1], 16, 64)
				if err != nil {
					return nil, err
				}
				var s *segment.Segment
				s, err = segment.New(fileName, segmentSize, startID)
				if err != nil {
					return nil, err
				}

				segments = append(segments, s)
			}
		}
	}

	if len(segments) == 0 {
		seq := uint64(0)

		fileName := filepath.Join(dir, fmt.Sprintf("%016x.seg", seq))

		var s *segment.Segment

		s, err = segment.New(fileName, segmentSize, 0)
		if err != nil {
			return nil, err
		}

		segments = append(segments, s)

	}

	sort.Sort(segments)

	for i := 1; i < len(segments); i++ {
		segments[i-1].LastID = segments[i].FirstID - 1
	}

	oldSegments := segments[:len(segments)-1]

	currentSegment := segments[len(segments)-1]

	lastID := uint64(0xffffffffffffffff)
	err = currentSegment.Read(func(id uint64, data []byte) error {
		lastID = id
		return nil
	})

	currentSegment.LastID = lastID

	if err != nil {
		return nil, err
	}

	return &Topic{
		dir:            dir,
		segmentSize:    segmentSize,
		currentSegment: currentSegment,
		oldSegments:    oldSegments,
	}, nil
}

// LastID returns ID of the last written event
func (t *Topic) LastID() uint64 {
	t.Lock()
	defer t.Unlock()
	return t.currentSegment.LastID
}

// WriteEvent writes an event to the topic and returns eventID or error
func (t *Topic) WriteEvent(data []byte) (uint64, error) {
	t.Lock()
	defer t.Unlock()

	eventAndHeaderSize := len(data) + 12

	if eventAndHeaderSize > t.segmentSize {
		return 0, ErrTooLargeEvent
	}

	if t.currentSegment.FileSize+eventAndHeaderSize > t.segmentSize {
		err := t.currentSegment.Sync()
		if err != nil {
			return 0, err
		}
		t.oldSegments = append(t.oldSegments, t.currentSegment)
		lastID := t.currentSegment.LastID
		fileName := filepath.Join(t.dir, fmt.Sprintf("%016x.seg", lastID+1))
		newSegment, err := segment.New(fileName, t.segmentSize, lastID+1)
		if err != nil {
			return 0, err
		}

		t.currentSegment = newSegment
	}

	lastID, err := t.currentSegment.Append(data)

	// notify listeners
	for _, l := range t.lastIDListeners {
		select {
		case l <- lastID:
		default:
			// failed, remove the value from channel
			select {
			case <-l:
			default:
			}
			// try again
			select {
			case l <- lastID:
			default:
			}
		}
	}

	return lastID, err
}

// ReadEvents passes every event from the queue to the callback
func (t *Topic) ReadEvents(fn func(uint64, []byte) error) error {
	return t.readEventsFromTo(0, t.currentSegment.LastID, fn)
}

func (t *Topic) readEventsFromTo(from, to uint64, fn func(uint64, []byte) error) error {

	for _, s := range t.oldSegments {
		if from <= s.LastID && to >= s.FirstID {
			err := s.Read(func(id uint64, data []byte) error {
				if from <= id && id <= to {
					return fn(id, data)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
	}

	if from <= t.currentSegment.LastID && to >= t.currentSegment.FirstID {
		err := t.currentSegment.Read(func(id uint64, data []byte) error {
			if from <= id && id <= to {
				return fn(id, data)
			}
			return nil
		})
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
	return t.currentSegment.Close()
}

// Subscribe returns two channels: First one is used to read events.
// Second channel is used to signal (by closing) that listener is no longer inerested on the events.
// From parameter defines from which event ID the IDs should be sent.
func (t *Topic) Subscribe(from uint64) (<-chan Event, chan interface{}) {
	t.Lock()
	defer t.Unlock()
	evtChan := make(chan Event, 1)
	closeChan := make(chan interface{})

	listenerChan := make(chan uint64, 1)
	listenerChan <- t.currentSegment.LastID

	t.lastIDListeners = append(t.lastIDListeners, listenerChan)
	go func() {

		defer func() {
			t.Lock()
			defer t.Unlock()

			newIDListeners := []chan uint64{}

			for _, ch := range t.lastIDListeners {
				if ch != listenerChan {
					newIDListeners = append(newIDListeners, ch)
				}

			}
			t.lastIDListeners = newIDListeners

		}()

		listenerClosedErr := errors.New("listener closed")

		defer close(listenerChan)

		newFrom := from

		for {

			select {
			case lastID := <-listenerChan:
				err := t.readEventsFromTo(newFrom, lastID, func(id uint64, data []byte) error {
					select {
					case evtChan <- Event{id, data}:
						return nil
					case <-closeChan:
						return listenerClosedErr
					}
				})
				if err == listenerClosedErr {
					return
				}
				newFrom = lastID + 1
			case <-closeChan:
				return
			}
		}

	}()

	return evtChan, closeChan
}
