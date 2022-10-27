package bytepool

import (
	"bytes"
	"sync"
)

// ByteBufferPool is a cached pool of reusable byte buffers.
type ByteBufferPool struct {
	sync.Pool
}

// NewBufferPool allocates a new ByteBufferPool.
func NewBufferPool() *ByteBufferPool {
	return &ByteBufferPool{sync.Pool{New: func() any { return new(bytes.Buffer) }}}
}

// Get returns a bytes.Buffer from the pool.
func (bbp *ByteBufferPool) Get() *bytes.Buffer {
	return bbp.Pool.Get().(*bytes.Buffer)
}

// Put returns bytes.Buffer to the pool.
func (bbp *ByteBufferPool) Put(b *bytes.Buffer) {
	b.Reset()
	bbp.Pool.Put(b)
}
