package segment

import (
	"encoding/binary"
	"os"
	"syscall"
)

// Segment represents one segment of events on the disk.
type Segment struct {
	file     *os.File
	data     []byte
	LastID   uint64
	FileSize int
	FirstID  uint64
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
		LastID:   firstID - 1,
		FirstID:  firstID,
		FileSize: int(pos),
	}, nil
}

// Append appends data to the segment
func (s *Segment) Append(d []byte) (uint64, error) {
	size := 4 + len(d)
	data := make([]byte, size)
	binary.BigEndian.PutUint32(data, uint32(size))

	copy(data[4:], d)

	_, err := s.file.Write(data)

	if err != nil {
		return 0, err
	}

	s.LastID++

	s.FileSize += size

	return s.LastID, nil
}

// ReadAll calls callback for each value in the file
func (s *Segment) Read(f func(id uint64, data []byte) error) error {

	id := s.FirstID

	for current := 0; current < s.FileSize; id++ {
		sz := binary.BigEndian.Uint32(s.data[current:])
		end := current + int(sz)
		data := s.data[current+4 : end]
		err := f(id, data)
		if err != nil {
			return err
		}
		current = end
	}
	return nil
}

// Sync syncs file to the disk
func (s *Segment) Sync() error {
	return s.file.Sync()
}

// Close unmaps the mmaped file and closes the FD
func (s *Segment) Close() error {
	err := syscall.Munmap(s.data)
	if err != nil {
		return err
	}
	return s.file.Close()
}
