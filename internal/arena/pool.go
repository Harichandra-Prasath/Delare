package arena

import "sync"

const POOL_SIZE = 64 * 1024

var BufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, POOL_SIZE)
		return &b
	},
}
