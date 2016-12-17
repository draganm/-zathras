package event

import (
	"encoding/binary"
	"io"
	"time"
)

// Event represends data of one event
type Event struct {
	ID   uint64
	Time time.Time
	Data []byte
}

func (e Event) Write(writer io.Writer) (int, error) {

	size := 4 + 8 + 8 + len(e.Data)
	data := make([]byte, size)
	binary.BigEndian.PutUint32(data, uint32(size))
	binary.BigEndian.PutUint64(data[4:], e.ID)
	binary.BigEndian.PutUint64(data[12:], uint64(e.Time.UnixNano()))
	copy(data[20:], e.Data)

	_, err := writer.Write(data)
	if err != nil {
		return -1, err
	}

	return size, nil
}

func Read(reader io.Reader) (Event, error) {

	var size int32
	err := binary.Read(reader, binary.BigEndian, &size)
	if err != nil {
		return Event{}, err
	}

	var id uint64
	err = binary.Read(reader, binary.BigEndian, &id)
	if err != nil {
		return Event{}, err
	}

	var timeNano int64
	err = binary.Read(reader, binary.BigEndian, &timeNano)
	if err != nil {
		return Event{}, err
	}

	dataLength := size - 20
	data := make([]byte, dataLength)

	_, err = io.ReadFull(reader, data)
	if err != nil {
		return Event{}, err
	}

	t := time.Unix(timeNano/1e9, timeNano%1e9)

	return Event{id, t, data}, nil

}
