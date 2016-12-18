package segment

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Segment represents one segment of events on the disk.
type Segment struct {
	file *os.File
	data []byte

	FileName string
	FileSize uint64
}

// New creates a new Segment file in the provided dir
func New(dir string, maxSize int, t time.Time, startID uint64) (*Segment, error) {

	fileName := filepath.Join(dir, fmt.Sprintf("%016x-%016x.seg", t.UnixNano(), startID))

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0700)
	if err != nil {
		return nil, err
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, maxSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &Segment{
		file:     file,
		FileName: fileName,
		data:     data,
	}, nil
}

// Append appends data to the segment
func (s *Segment) Append(d []byte) error {
	size := 4 + len(d)
	data := make([]byte, size)
	binary.BigEndian.PutUint32(data, uint32(size))
	copy(data[4:], d)
	_, err := s.file.Write(data)
	s.FileSize += uint64(size)
	return err
}

// ReadAll calls callback for each value in the file
func (s *Segment) ReadAll(f func(data []byte)) {

	for current := 0; uint64(current) < s.FileSize; {
		sz := binary.BigEndian.Uint32(s.data[current:])
		end := current + int(sz)
		data := s.data[current+4 : end]
		f(data)
		current = end
	}

}

// Sync syncs file to the disk
func (s *Segment) Sync() error {
	return s.file.Sync()
}
