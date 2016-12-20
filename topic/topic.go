package topic

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"

	"github.com/draganm/zathras/segment"
)

// Topic represents a Zathras topic
type Topic struct {
	sync.Mutex
	dir            string
	segmentSize    int
	lastID         uint64
	oldSegments    []*segment.Segment
	currentSegment *segment.Segment
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

	segments := []*segment.Segment{}

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

	oldSegments := segments[:len(segments)-1]

	currentSegment := segments[len(segments)-1]

	return &Topic{
		dir:            dir,
		segmentSize:    segmentSize,
		currentSegment: currentSegment,
		oldSegments:    oldSegments,
	}, nil
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

		fileName := filepath.Join(t.dir, fmt.Sprintf("%016x.seg", t.lastID+1))
		newSegment, err := segment.New(fileName, t.segmentSize, t.lastID+1)
		if err != nil {
			return 0, err
		}

		t.currentSegment = newSegment
	}

	lastID, err := t.currentSegment.Append(data)
	t.lastID = lastID
	return lastID, err
}

func (t *Topic) ReadEvents(fn func(uint64, []byte) error) error {
	t.Lock()
	segments := append(t.oldSegments, t.currentSegment)
	t.Unlock()
	for _, s := range segments {
		err := s.Read(fn)
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
