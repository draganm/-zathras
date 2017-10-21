package limiter

import (
	"errors"
	"fmt"
	"sync"
)

// ErrClosed is returned when limiter is closed
var ErrClosed = errors.New("closed")

type Limiter struct {
	current uint64
	closed  bool
	m       *sync.Mutex
	cond    *sync.Cond
}

// New creates a new instance of a Limiter
func New(initial uint64) *Limiter {
	m := &sync.Mutex{}
	return &Limiter{
		current: initial,
		closed:  false,
		m:       m,
		cond:    sync.NewCond(m),
	}
}

// UpdateCurrent updates the current state of the limiter.
// It will panic if the new current is lower than the previous current value.
func (l *Limiter) UpdateCurrent(current uint64) {

	l.m.Lock()
	defer l.m.Unlock()

	if l.current > current {
		panic(fmt.Errorf("Trying to lower current (%d) to %d", l.current, current))
	}

	l.current = current

	l.cond.Broadcast()

}

// WaitForCurrentToBeGreaterThan waits for current value to go past the from value.
// It returns an error when the limiter is closed.
func (l *Limiter) WaitForCurrentToBeGreaterThan(from uint64) (uint64, error) {
	l.m.Lock()
	defer l.m.Unlock()

	for {
		if l.closed {
			return 0, ErrClosed
		}
		if l.current > from {
			return l.current, nil
		}
		l.cond.Wait()
	}
}

// Close closes the limiter.
func (l *Limiter) Close() {
	l.m.Lock()
	l.closed = true
	l.m.Unlock()
	l.cond.Broadcast()
}
