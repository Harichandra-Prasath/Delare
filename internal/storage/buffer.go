package storage

import (
	"sync/atomic"
)

type RingBuffer struct {
	_    [56]byte
	head atomic.Uint64

	_    [56]byte
	tail atomic.Uint64

	_     [56]byte
	mask  uint64
	slots []atomic.Pointer[[]byte]
}

var GlobalRingBuffer *RingBuffer

func InitialiseRingBuffer(size uint64) {
	GlobalRingBuffer = &RingBuffer{
		mask:  size - 1,
		slots: make([]atomic.Pointer[[]byte], size),
	}
}

// Push is called by MULTIPLE producer goroutines (socket readers).
func (r *RingBuffer) Push(data *[]byte) bool {
	for {
		head := r.head.Load()
		tail := r.tail.Load()

		// 1. Check if full
		if head-tail >= uint64(len(r.slots)) {
			return false // Backpressure: Buffer is full
		}

		if r.head.CompareAndSwap(head, head+1) {
			idx := head & r.mask
			r.slots[idx].Store(data)
			return true
		}
	}
}

// Pop is called by a SINGLE consumer goroutine (disk writer).
func (r *RingBuffer) Pop() *[]byte {
	head := r.head.Load()
	tail := r.tail.Load()

	// 1. Check if empty
	if tail == head {
		return nil
	}

	idx := tail & r.mask

	// We use Swap(nil) so the slot is cleared for the next cycle.
	data := r.slots[idx].Swap(nil)

	// Edge case: A producer claimed 'head' but hasn't executed Store() yet.
	if data == nil {
		return nil
	}

	r.tail.Add(1)

	return data
}
