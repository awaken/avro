package avro_test

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReader_SkipNBytes(t *testing.T) {
	data := []byte{0x01, 0x01, 0x01, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 2)

	r.SkipNBytes(3)

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipNBytesEOF(t *testing.T) {
	data := []byte{0x01, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 2)

	r.SkipNBytes(3)

	require.ErrorIs(t, r.Error, io.ErrUnexpectedEOF)
}

var errChunkReaderExhausted = errors.New("chunk reader exhausted")

type chunkReader struct {
	reads int
	limit int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.reads == r.limit {
		return 0, errChunkReaderExhausted
	}
	r.reads++
	if r.reads == r.limit {
		p[len(p)-1] = 0x36
	}
	return len(p), nil
}

func TestReaderSkipNBytesMaxInt32LeavesBufferedRemainder(t *testing.T) {
	const bufSize = 1 << 20
	r := avro.NewReader(&chunkReader{limit: math.MaxInt32/bufSize + 1}, bufSize)

	r.SkipNBytes(math.MaxInt32)

	require.NoError(t, r.Error)
	assert.Equal(t, byte(0x36), r.Peek())
	require.NoError(t, r.Error)
}

func TestReader_SkipBool(t *testing.T) {
	data := []byte{0x01, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipBool()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReaderSkipBoolRejectsInvalidValue(t *testing.T) {
	r := avro.NewReader(bytes.NewReader([]byte{0x02}), 10)

	r.SkipBool()

	require.ErrorContains(t, r.Error, "invalid bool")
}

func TestReader_SkipInt(t *testing.T) {
	data := []byte{0x38, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipInt()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipLong(t *testing.T) {
	data := []byte{0x38, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipLong()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReaderSkipRejectsVarintOverflow(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		skip func(*avro.Reader)
	}{
		{
			name: "int continuation",
			data: []byte{0x80, 0x80, 0x80, 0x80, 0x80},
			skip: (*avro.Reader).SkipInt,
		},
		{
			name: "int final byte",
			data: []byte{0xff, 0xff, 0xff, 0xff, 0x10},
			skip: (*avro.Reader).SkipInt,
		},
		{
			name: "long continuation",
			data: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80},
			skip: (*avro.Reader).SkipLong,
		},
		{
			name: "long final byte",
			data: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02},
			skip: (*avro.Reader).SkipLong,
		},
	}

	for _, test := range tests {
		for _, bufSize := range []int{1, 64} {
			t.Run(test.name, func(t *testing.T) {
				r := avro.NewReader(bytes.NewReader(test.data), bufSize)

				test.skip(r)

				require.ErrorContains(t, r.Error, "overflow")
			})
		}
	}
}

func TestReader_SkipFloat(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipFloat()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipDouble(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipDouble()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipString(t *testing.T) {
	data := []byte{0x06, 0x66, 0x6F, 0x6F, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipString()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipStringEmpty(t *testing.T) {
	data := []byte{0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipString()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipBytes(t *testing.T) {
	data := []byte{0x06, 0x66, 0x6F, 0x6F, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipBytes()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipBytesEmpty(t *testing.T) {
	data := []byte{0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipBytes()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}
