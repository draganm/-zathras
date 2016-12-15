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

func (e Event) Write(w io.Writer) error {
	size := 4 + 8 + 15 + len(e.Data)

	err := binary.Write(w, binary.BigEndian, size)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, e.ID)
	if err != nil {
		return err
	}

	e.Time.MarshalBinary()
	err = binary.Write(w, binary.BigEndian, e.Time)
	if err != nil {
		return err
	}
	return nil

}
