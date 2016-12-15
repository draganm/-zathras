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

func (e Event) Write(writer io.Writer) error {

	size := int32(4 + 8 + 8 + len(e.Data))
	err := binary.Write(writer, binary.BigEndian, size)
	if err != nil {
		return err
	}

	err = binary.Write(writer, binary.BigEndian, e.ID)
	if err != nil {
		return err
	}

	err = binary.Write(writer, binary.BigEndian, e.Time.UnixNano())
	if err != nil {
		return err
	}

	_, err = writer.Write(e.Data)
	if err != nil {
		return err
	}

	return nil
}
