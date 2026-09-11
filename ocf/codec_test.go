package ocf

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countedCodec struct {
	Codec
	closes int
	err    error
}

func (c *countedCodec) Close() error {
	c.closes++
	return errors.Join(c.Codec.(io.Closer).Close(), c.err)
}

type failedHeaderWriter struct {
	err    error
	closed bool
}

func (w *failedHeaderWriter) Write([]byte) (int, error) { return 0, w.err }
func (w *failedHeaderWriter) Close() error              { w.closed = true; return nil }

func TestCodecConstructorOwnership(t *testing.T) {
	for _, mode := range []string{"header-failure", "append-failure", "new-success", "append-success"} {
		for shared := range 4 {
			t.Run(mode+"/shared-"+strconv.Itoa(shared), func(t *testing.T) {
				cfg := computeEncoderConfig([]EncoderFunc{WithCodec(ZStandard)})
				if shared&1 != 0 {
					decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
					require.NoError(t, err)
					t.Cleanup(decoder.Close)
					cfg.CodecOptions.ZStandardOptions.Decoder = decoder
				}
				if shared&2 != 0 {
					encoder, err := zstd.NewWriter(io.Discard, zstd.WithEncoderConcurrency(1))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, encoder.Close()) })
					cfg.CodecOptions.ZStandardOptions.Encoder = encoder
				}
				var observed *countedCodec
				cleanupErr := errors.New("codec cleanup failed")
				cfg.CodecOptions.factory = func(name CodecName, opts codecOptions) (Codec, error) {
					codec, err := resolveCodec(name, opts)
					if err != nil {
						return nil, err
					}
					t.Cleanup(func() { require.NoError(t, codec.(io.Closer).Close()) })
					observed = &countedCodec{Codec: codec, err: cleanupErr}
					return observed, nil
				}

				var output io.Writer = &bytes.Buffer{}
				writeErr := errors.New("header write failed")
				failedWriter := &failedHeaderWriter{err: writeErr}
				if mode == "header-failure" {
					output = failedWriter
				}
				if mode == "append-failure" || mode == "append-success" {
					header, err := avro.Marshal(HeaderSchema, Header{Magic: magicBytes,
						Meta: map[string][]byte{schemaKey: []byte(`"long"`), codecKey: []byte(ZStandard)}, Sync: [16]byte{1}})
					require.NoError(t, err)
					if mode == "append-failure" {
						header = append(header, 1)
					} // Negative block count.
					file, err := os.Create(filepath.Join(t.TempDir(), "owned.avro"))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, file.Close()) })
					_, err = file.Write(header)
					require.NoError(t, err)
					output = file
				}

				encoder, err := newEncoder(avro.MustParse(`"long"`), output, cfg)
				require.NotNil(t, observed)
				if mode == "header-failure" || mode == "append-failure" {
					require.Error(t, err)
					require.Nil(t, encoder)
					assert.Equal(t, 1, observed.closes, "failed construction retained codec ownership")
					assert.ErrorIs(t, err, cleanupErr)
					if mode == "header-failure" {
						assert.ErrorIs(t, err, writeErr)
					}
					if mode == "append-failure" {
						assert.ErrorContains(t, err, "negative record count")
					}
				} else {
					require.NoError(t, err)
					require.Zero(t, observed.closes, "constructor prematurely released its codec")
					require.NoError(t, encoder.Encode(int64(7)))
					require.ErrorIs(t, encoder.Close(), cleanupErr)
					assert.Equal(t, 1, observed.closes)
				}
				assert.False(t, failedWriter.closed, "constructor must not close the caller's writer")
				if file, ok := output.(*os.File); ok {
					_, err = file.Stat()
					require.NoError(t, err)
				}
				codec := observed.Codec.(*ZStandardCodec)
				_, err = codec.decoder.DecodeAll(nil, nil)
				if shared&1 != 0 {
					assert.NoError(t, err)
				} else {
					assert.ErrorIs(t, err, zstd.ErrDecoderClosed)
				}
				if shared&2 != 0 {
					_, err = codec.encoder.Write([]byte{2})
					assert.NoError(t, err)
				}
			})
		}
	}
}

func TestCodecConstructorOptionErrors(t *testing.T) {
	for _, side := range []string{"decoder", "encoder"} {
		t.Run(side, func(t *testing.T) {
			cfg := computeEncoderConfig([]EncoderFunc{WithCodec(ZStandard)})
			sharedDecoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			require.NoError(t, err)
			t.Cleanup(sharedDecoder.Close)
			sharedEncoder, err := zstd.NewWriter(io.Discard, zstd.WithEncoderConcurrency(1))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sharedEncoder.Close()) })
			if side == "decoder" {
				cfg.CodecOptions.ZStandardOptions.DOptions = []zstd.DOption{zstd.WithDecoderConcurrency(-1)}
				cfg.CodecOptions.ZStandardOptions.Encoder = sharedEncoder
			} else {
				cfg.CodecOptions.ZStandardOptions.EOptions = []zstd.EOption{zstd.WithEncoderConcurrency(-1)}
				cfg.CodecOptions.ZStandardOptions.Decoder = sharedDecoder
			}
			var output bytes.Buffer
			encoder, err := newEncoder(avro.MustParse(`"long"`), &output, cfg)
			require.ErrorContains(t, err, "create "+side)
			require.Nil(t, encoder)
			require.Zero(t, output.Len(), "invalid options must not publish a header")
			_, err = sharedDecoder.DecodeAll(nil, nil)
			require.NoError(t, err)
			_, err = sharedEncoder.Write([]byte{2})
			require.NoError(t, err)
		})
	}
}

func TestZstdEncodeDecodeLowEntropyLong(t *testing.T) {
	input := makeTestData(8762, func() byte { return 'a' })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeLowEntropyShort(t *testing.T) {
	input := makeTestData(7, func() byte { return 'a' })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeHighEntropyLong(t *testing.T) {
	input := makeTestData(8762, func() byte { return byte(rand.Uint32()) })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeHighEntropyShort(t *testing.T) {
	input := makeTestData(7, func() byte { return byte(rand.Uint32()) })

	verifyZstdEncodeDecode(t, input)
}

/*
benchmark results always creating a new zstd encoder/decoder

goos: linux
goarch: amd64
pkg: github.com/awaken/avro/v2/ocf
cpu: AMD Ryzen 5 3550H with Radeon Vega Mobile Gfx


BenchmarkZstdEncodeDecodeLowEntropyLong
BenchmarkZstdEncodeDecodeLowEntropyLong-8    	     289	   3523847 ns/op	10891887 B/op	      40 allocs/op
BenchmarkZstdEncodeDecodeHighEntropyLong
BenchmarkZstdEncodeDecodeHighEntropyLong-8   	     298	   3390952 ns/op	10894703 B/op	      40 allocs/op


benchmark results reusing an existing zstd encoder/decoder

BenchmarkZstdEncodeDecodeLowEntropyLong
BenchmarkZstdEncodeDecodeLowEntropyLong-8    	   55628	     22883 ns/op	   19220 B/op	       2 allocs/op
BenchmarkZstdEncodeDecodeHighEntropyLong
BenchmarkZstdEncodeDecodeHighEntropyLong-8   	   47652	     25064 ns/op	   31553 B/op	       3 allocs/op
*/

func BenchmarkZstdEncodeDecodeLowEntropyLong(b *testing.B) {
	input := makeTestData(8762, func() byte { return 'a' })

	codec, err := resolveCodec(ZStandard, codecOptions{})
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed := codec.Encode(input)
		_, decodeErr := codec.Decode(compressed)
		require.NoError(b, decodeErr)
	}
}

func BenchmarkZstdEncodeDecodeHighEntropyLong(b *testing.B) {
	input := makeTestData(8762, func() byte { return byte(rand.Uint32()) })

	codec, err := resolveCodec(ZStandard, codecOptions{})
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed := codec.Encode(input)
		_, decodeErr := codec.Decode(compressed)
		require.NoError(b, decodeErr)
	}
}

func verifyZstdEncodeDecode(t *testing.T, input []byte) {
	codec, err := resolveCodec(ZStandard, codecOptions{})
	require.NoError(t, err)

	compressed := codec.Encode(input)
	actual, decodeErr := codec.Decode(compressed)

	require.NoError(t, decodeErr)
	assert.Equal(t, input, actual)
}

func makeTestData(length int, charMaker func() byte) []byte {
	input := make([]byte, length)
	for i := 0; i < length; i++ {
		input[i] = charMaker()
	}
	return input
}
