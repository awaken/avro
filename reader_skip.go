package avro

import "math"

// SkipNBytesInt64 skips n bytes without narrowing an untrusted wire length.
func (r *Reader) SkipNBytesInt64(n int64) {
	if n < 0 {
		r.ReportError("SkipNBytesInt64", "negative byte count")
		return
	}
	for n > 0 {
		chunk := min(n, int64(math.MaxInt))
		r.SkipNBytes(int(chunk))
		if r.Error != nil {
			return
		}
		n -= chunk
	}
}

// SkipNBytes skips the given number of bytes in the reader.
func (r *Reader) SkipNBytes(n int) {
	if n < 0 {
		r.ReportError("SkipNBytes", "negative byte count")
		return
	}
	read := 0
	for read < n {
		if r.head == r.tail {
			if !r.loadMore() {
				r.reportUnexpectedEOF()
				return
			}
		}

		if r.tail-r.head < n-read {
			read += r.tail - r.head
			r.head = r.tail
			continue
		}

		r.head += n - read
		read += n - read
	}
}

// SkipBool skips a Bool in the reader.
func (r *Reader) SkipBool() {
	_ = r.ReadBool()
}

// SkipInt skips an Int in the reader.
func (r *Reader) SkipInt() {
	r.skipVarint(maxIntBufSize, 0x0f, "SkipInt")
}

// SkipLong skips a Long in the reader.
func (r *Reader) SkipLong() {
	r.skipVarint(maxLongBufSize, 0x01, "SkipLong")
}

func (r *Reader) skipVarint(maxSize int, maxLastByte byte, op string) {
	for i := 0; r.Error == nil && i < maxSize; i++ {
		b := r.readByte()
		if i == maxSize-1 && b > maxLastByte {
			r.ReportError(op, "int overflow")
			return
		}
		if b&0x80 == 0 {
			return
		}
	}
}

// SkipFloat skips a Float in the reader.
func (r *Reader) SkipFloat() {
	r.SkipNBytes(4)
}

// SkipDouble skips a Double in the reader.
func (r *Reader) SkipDouble() {
	r.SkipNBytes(8)
}

// SkipString skips a String in the reader.
func (r *Reader) SkipString() {
	size := r.ReadLong()
	r.SkipNBytesInt64(size)
}

// SkipBytes skips Bytes in the reader.
func (r *Reader) SkipBytes() {
	size := r.ReadLong()
	r.SkipNBytesInt64(size)
}
