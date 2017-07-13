package segment

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// ErrDataTooLarge is returned when the appending data would extend segment beyond the maxSize
var ErrDataTooLarge = errors.New("Data too large")

// ErrWrongAddress is returned when the address is outside of the segment
var ErrWrongAddress = errors.New("Wrong data address")

// ErrSegmentCorrupted is returned when the data to be read is not aligned with the segment size
var ErrSegmentCorrupted = errors.New("Segment corrupted!")

// Segment represents one segment of events on the disk.
type Segment struct {
	sync.Mutex
	file     *os.File
	data     []byte
	fileSize uint64
	maxSize  uint64
}

// New creates a new Segment file in the provided dir
func New(fileName string, maxSize uint64) (*Segment, error) {

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

	data, err := syscall.Mmap(int(file.Fd()), 0, int(maxSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &Segment{
		file:     file,
		data:     data,
		fileSize: uint64(pos),
		maxSize:  uint64(maxSize),
	}, nil
}

func (s *Segment) FileSize() uint64 {
	return atomic.LoadUint64(&s.fileSize)
}

// Append appends data to the segment
func (s *Segment) Append(d []byte) (uint64, uint64, error) {
	s.Lock()
	defer s.Unlock()

	eventAddress := s.fileSize

	if s.fileSize+4+uint64(len(d)) > s.maxSize {
		return 0, 0, ErrDataTooLarge
	}

	size := len(d)
	data := make([]byte, size+4)
	binary.BigEndian.PutUint32(data, uint32(size))

	copy(data[4:], d)

	written, err := s.file.Write(data)

	if err != nil {
		return 0, 0, err
	}

	s.fileSize += uint64(written)

	return eventAddress, s.fileSize, nil
}

// ReadAll calls callback for each value in the file
func (s *Segment) Read(address uint64) ([]byte, uint64, error) {

	fileSize := atomic.LoadUint64(&s.fileSize)

	if address+4 > fileSize {
		return nil, 0, ErrWrongAddress
	}

	sz := uint64(binary.BigEndian.Uint32(s.data[address:]))

	if address+sz+4 > fileSize {
		return nil, 0, ErrSegmentCorrupted
	}

	return s.data[address+4 : address+4+sz], address + 4 + sz, nil

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
