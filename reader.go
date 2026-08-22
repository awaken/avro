package avro

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unsafe"
)

const (
	maxIntBufSize            = 5
	maxLongBufSize           = 10
	defaultReaderBufferSize  = 512
	maxConsecutiveEmptyReads = 100
)

// ReaderFunc is a function used to customize the Reader.
type ReaderFunc func(r *Reader)

// WithReaderConfig specifies the configuration to use with a reader.
func WithReaderConfig(cfg API) ReaderFunc {
	return func(r *Reader) {
		r.cfg = cfg.(*frozenConfig)
	}
}

// Reader is an Avro specific io.Reader.
type Reader struct {
	cfg        *frozenConfig
	reader     io.Reader
	slab       []byte
	buf        []byte
	head       int
	tail       int
	pendingErr error
	Error      error
}

// NewReader creates a new Reader.
func NewReader(r io.Reader, bufSize int, opts ...ReaderFunc) *Reader {
	if bufSize <= 0 {
		bufSize = defaultReaderBufferSize
	}
	reader := &Reader{
		cfg:    DefaultConfig.(*frozenConfig),
		reader: r,
		buf:    make([]byte, bufSize),
		head:   0,
		tail:   0,
	}

	for _, opt := range opts {
		opt(reader)
	}

	return reader
}

// Reset resets a Reader with a new byte array attached.
func (r *Reader) Reset(b []byte) *Reader {
	if r.cfg == nil {
		r.cfg = DefaultConfig.(*frozenConfig)
	}
	r.reader = nil
	r.buf = b
	r.head = 0
	r.tail = len(b)
	r.pendingErr = nil
	r.Error = nil
	return r
}

// ReportError record an error in iterator instance with current position.
func (r *Reader) ReportError(operation, msg string) {
	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		return
	}

	r.Error = fmt.Errorf("avro: %s: %s", operation, msg)
}

func (r *Reader) loadMore() bool {
	if r.pendingErr != nil {
		if r.Error == nil {
			r.Error = r.pendingErr
		}
		r.pendingErr = nil
		return false
	}

	if r.reader == nil {
		if r.Error == nil {
			r.head = r.tail
			r.Error = io.EOF
		}
		return false
	}

	for emptyReads := 0; ; emptyReads++ {
		n, err := r.reader.Read(r.buf)
		if n == 0 {
			if err != nil {
				if r.Error == nil {
					r.Error = err
				}
				return false
			}
			if emptyReads >= maxConsecutiveEmptyReads {
				if r.Error == nil {
					r.Error = io.ErrNoProgress
				}
				return false
			}
			continue
		}

		r.head = 0
		r.tail = n
		r.pendingErr = err
		return true
	}
}

func (r *Reader) reportUnexpectedEOF() {
	if r.Error == nil || errors.Is(r.Error, io.EOF) {
		r.Error = io.ErrUnexpectedEOF
	}
}

func (r *Reader) readByte() byte {
	if r.head != r.tail {
		r.head++
		return r.buf[r.head-1]
	}

	return r.readByteSlow()
}

//go:noinline
func (r *Reader) readByteSlow() byte {
	if !r.loadMore() {
		r.reportUnexpectedEOF()
		return 0
	}
	b := r.buf[r.head]
	r.head++
	return b
}

// Peek returns the next byte in the buffer.
// The Reader Error will be io.EOF if no next byte exists.
func (r *Reader) Peek() byte {
	if r.head == r.tail {
		if !r.loadMore() {
			return 0
		}
	}
	return r.buf[r.head]
}

// Read reads data into the given bytes.
func (r *Reader) Read(b []byte) {
	size := len(b)
	read := 0

	for read < size {
		if r.head == r.tail {
			if !r.loadMore() {
				r.reportUnexpectedEOF()
				return
			}
		}

		n := copy(b[read:], r.buf[r.head:r.tail])
		r.head += n
		read += n
	}
}

// ReadBool reads a Bool from the Reader.
func (r *Reader) ReadBool() bool {
	b := r.readByte()

	if b != 0 && b != 1 {
		r.ReportError("ReadBool", "invalid bool")
	}
	return b == 1
}

// ReadInt reads an Int from the Reader.
//
//nolint:dupl
func (r *Reader) ReadInt() int32 {
	if r.Error != nil {
		return 0
	}

	// Fast path: enough bytes remain for the largest encoded int.
	if r.tail-r.head >= maxIntBufSize {
		var value uint32
		var shift uint8
		for index, currentByte := range r.buf[r.head : r.head+maxIntBufSize] {
			if index == maxIntBufSize-1 && currentByte > 0x0f {
				r.ReportError("ReadInt", "int overflow")
				return 0
			}
			value |= uint32(currentByte&0x7f) << shift
			if currentByte&0x80 == 0 {
				r.head += index + 1
				return int32((value >> 1) ^ -(value & 1))
			}
			shift += 7
		}
		r.ReportError("ReadInt", "int overflow")
		return 0
	}

	var (
		consumed int
		value    uint32
		shift    uint8
	)

	for {
		tail := r.tail
		if r.tail-r.head+consumed > maxIntBufSize {
			tail = r.head + maxIntBufSize - consumed
		}

		// Consume what it is in the buffer.
		var index int
		for _, currentByte := range r.buf[r.head:tail] {
			if consumed+index == maxIntBufSize-1 && currentByte > 0x0f {
				r.ReportError("ReadInt", "int overflow")
				return 0
			}
			value |= uint32(currentByte&0x7f) << shift
			if currentByte&0x80 == 0 {
				r.head += index + 1
				return int32((value >> 1) ^ -(value & 1))
			}
			shift += 7
			index++
		}
		if consumed >= maxIntBufSize {
			r.ReportError("ReadInt", "int overflow")
			return 0
		}
		r.head += index
		consumed += index

		// We ran out of buffer and are not at the end of the int,
		// Read more into the buffer.
		if !r.loadMore() {
			r.Error = fmt.Errorf("reading int: %w", r.Error)
			return 0
		}
	}
}

// ReadLong reads a Long from the Reader.
//
//nolint:dupl
func (r *Reader) ReadLong() int64 {
	if r.Error != nil {
		return 0
	}

	// Fast path: enough bytes remain for the largest encoded long.
	if r.tail-r.head >= maxLongBufSize {
		var value uint64
		var shift uint8
		for index, currentByte := range r.buf[r.head : r.head+maxLongBufSize] {
			if index == maxLongBufSize-1 && currentByte > 0x01 {
				r.ReportError("ReadLong", "int overflow")
				return 0
			}
			value |= uint64(currentByte&0x7f) << shift
			if currentByte&0x80 == 0 {
				r.head += index + 1
				return int64((value >> 1) ^ -(value & 1))
			}
			shift += 7
		}
		r.ReportError("ReadLong", "int overflow")
		return 0
	}

	var (
		consumed int
		value    uint64
		shift    uint8
	)

	for {
		tail := r.tail
		if r.tail-r.head+consumed > maxLongBufSize {
			tail = r.head + maxLongBufSize - consumed
		}

		// Consume what it is in the buffer.
		var index int
		for _, currentByte := range r.buf[r.head:tail] {
			if consumed+index == maxLongBufSize-1 && currentByte > 0x01 {
				r.ReportError("ReadLong", "int overflow")
				return 0
			}
			value |= uint64(currentByte&0x7f) << shift
			if currentByte&0x80 == 0 {
				r.head += index + 1
				return int64((value >> 1) ^ -(value & 1))
			}
			shift += 7
			index++
		}
		if consumed >= maxLongBufSize {
			r.ReportError("ReadLong", "int overflow")
			return 0
		}
		r.head += index
		consumed += index

		// We ran out of buffer and are not at the end of the long,
		// Read more into the buffer.
		if !r.loadMore() {
			r.Error = fmt.Errorf("reading long: %w", r.Error)
			return 0
		}
	}
}

// ReadFloat reads a Float from the Reader.
func (r *Reader) ReadFloat() float32 {
	var buf [4]byte
	r.Read(buf[:])

	return math.Float32frombits(binary.LittleEndian.Uint32(buf[:]))
}

// ReadDouble reads a Double from the Reader.
func (r *Reader) ReadDouble() float64 {
	var buf [8]byte
	r.Read(buf[:])

	return math.Float64frombits(binary.LittleEndian.Uint64(buf[:]))
}

// ReadBytes reads Bytes from the Reader.
func (r *Reader) ReadBytes() []byte {
	return r.readBytes("bytes")
}

// ReadString reads a String from the Reader.
func (r *Reader) ReadString() string {
	b := r.readBytes("string")
	if len(b) == 0 {
		return ""
	}

	return *(*string)(unsafe.Pointer(&b))
}

func (r *Reader) readBytes(op string) []byte {
	size64 := r.ReadLong()
	if size64 < 0 {
		fnName := "Read" + strings.ToTitle(op)
		r.ReportError(fnName, "invalid "+op+" length")
		return nil
	}
	if size64 == 0 {
		return []byte{}
	}
	if maxSize := r.cfg.getMaxByteSliceSize(); maxSize > 0 && size64 > int64(maxSize) {
		fnName := "Read" + strings.ToTitle(op)
		r.ReportError(fnName, "size is greater than `Config.MaxByteSliceSize`")
		return nil
	}
	if size64 > int64(maxAllocSize) {
		fnName := "Read" + strings.ToTitle(op)
		r.ReportError(fnName, op+" length exceeds the platform allocation limit")
		return nil
	}
	size := int(size64)

	// The bytes are entirely in the buffer and of a reasonable size.
	// Use the byte slab.
	if r.head+size <= r.tail && size <= 1024 {
		if cap(r.slab) < size {
			r.slab = make([]byte, 1024)
		}
		dst := r.slab[:size:size]
		r.slab = r.slab[size:]
		copy(dst, r.buf[r.head:r.head+size])
		r.head += size
		return dst
	}

	buf := make([]byte, size)
	r.Read(buf)
	return buf
}

// ReadBlockHeader reads a Block Header from the Reader.
func (r *Reader) ReadBlockHeader() (int64, int64) {
	length := r.ReadLong()
	if r.Error != nil {
		return 0, 0
	}
	if length < 0 {
		if length == math.MinInt64 {
			r.ReportError("ReadBlockHeader", "block count cannot be math.MinInt64")
			return 0, 0
		}
		size := r.ReadLong()
		if r.Error != nil {
			return 0, 0
		}
		if size < 0 {
			r.ReportError("ReadBlockHeader", "invalid negative block size")
			return 0, 0
		}

		return -length, size
	}

	return length, 0
}
