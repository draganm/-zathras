package segment

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// ErrWrongAddress is returned when the address is outside of the segment
var ErrWrongAddress = errors.New("Wrong data address")

// ErrSegmentCorrupted is returned when the data to be read is not aligned with the segment size
var ErrSegmentCorrupted = errors.New("Segment corrupted!")

// Segment represents one segment of events on the disk.
type Segment struct {
	sync.Mutex
	file     *os.File
	data     []byte
	FileSize uint64
}

// New creates a new Segment file in the provided dir
func New(fileName string, maxSize int, firstID uint64) (*Segment, error) {

	exists := true

	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		exists = false
	} else if err != nil {
		return nil, err
	}

	flags := os.O_RDWR

	if !exists {
		flags = os.O_RDWR | os.O_CREATE
	}

	file, err := os.OpenFile(fileName, flags, 0700)
	if err != nil {
		return nil, err
	}

	pos, err := file.Seek(0, 2)
	if err != nil {
		return nil, err
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, maxSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &Segment{
		file:     file,
		data:     data,
		FileSize: uint64(pos),
	}, nil
}

// Append appends data to the segment
func (s *Segment) Append(d []byte) (uint64, error) {
	s.Lock()
	defer s.Unlock()

	eventAddress := s.FileSize

	size := len(d)
	data := make([]byte, size+4)
	binary.BigEndian.PutUint32(data, uint32(size))

	copy(data[4:], d)

	written, err := s.file.Write(data)

	if err != nil {
		return 0, err
	}

	s.FileSize += uint64(written)

	return eventAddress, nil
}

// ReadAll calls callback for each value in the file
func (s *Segment) Read(address uint64) ([]byte, error) {

	fileSize := atomic.LoadUint64(&s.FileSize)

	if address+4 > fileSize {
		return nil, ErrWrongAddress
	}

	sz := uint64(binary.BigEndian.Uint32(s.data[address:]))

	if address+sz+4 > fileSize {
		return nil, ErrSegmentCorrupted
	}

	return s.data[address+4 : address+4+sz], nil

}

// Sync syncs file to the disk
func (s *Segment) Sync() error {
	return s.file.Sync()
}

// Close unmaps the mmaped file and closes the FD
func (s *Segment) Close() error {
	s.Lock()
	defer s.Unlock()

	err := syscall.Munmap(s.data)
	if err != nil {
		return err
	}
	return s.file.Close()
}
