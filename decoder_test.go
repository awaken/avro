package avro_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type errorReader struct {
	err error
}

func TestDecoder_Buffered(t *testing.T) {
	for _, size := range []int{2, 1025} {
		input := bytes.NewReader(bytes.Repeat([]byte{2}, size))
		decoder := avro.NewDecoderForSchema(avro.MustParse(`"long"`), input)
		require.Zero(t, decoder.Buffered())
		for i := range size {
			var value int64
			require.NoError(t, decoder.DecodeDatum(&value))
			require.Equal(t, int64(1), value)
			require.Equal(t, size-i-1, decoder.Buffered()+input.Len())
		}
		require.ErrorIs(t, decoder.Decode(new(int64)), io.EOF)
	}
}

func TestDecoder_DecodeDatum(t *testing.T) {
	for _, schema := range []string{`"null"`, `{"type":"record","name":"Empty","fields":[]}`} {
		decoder := avro.NewDecoderForSchema(avro.MustParse(schema), bytes.NewReader(nil))
		for range 3 {
			var value any
			require.NoError(t, decoder.DecodeDatum(&value), "external framing permits zero-width records")
			require.Zero(t, decoder.Buffered())
		}
		require.ErrorIs(t, decoder.Decode(new(any)), io.EOF, "stream decoding must still stop at EOF")
	}

	decoder := avro.NewDecoderForSchema(avro.MustParse(`"long"`), bytes.NewReader([]byte{0x80}))
	err := decoder.DecodeDatum(new(int64))
	require.Error(t, err)
	require.Equal(t, err, decoder.DecodeDatum(new(int64)))
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestNewDecoder_SchemaError(t *testing.T) {
	defer ConfigTeardown()

	schema := "{}"
	_, err := avro.NewDecoder(schema, nil)

	assert.Error(t, err)
}

func TestDecoder_DecodeUnsupportedTypeError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := avro.NewPrimitiveSchema(avro.Type("test"), nil)
	dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

	var b bool
	err := dec.Decode(&b)

	assert.Error(t, err)
}

func TestDecoder_DecodeUnsupportedTypeIntoInterfaceError(t *testing.T) {
	schema := avro.NewPrimitiveSchema(avro.Type("test"), nil)
	dec := avro.NewDecoderForSchema(schema, bytes.NewReader([]byte{0x01}))
	var got any
	var err error

	assert.NotPanics(t, func() {
		err = dec.Decode(&got)
	})
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestDecoder_DecodeEmptyReader(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{}
	schema := "boolean"
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var b bool
	err := dec.Decode(b)

	assert.Error(t, err)
}

func TestDecoder_DecodeReaderError(t *testing.T) {
	defer ConfigTeardown()

	want := errors.New("test")
	dec, _ := avro.NewDecoder("boolean", errorReader{err: want})

	var b bool
	err := dec.Decode(&b)

	assert.ErrorIs(t, err, want)
}

func TestDecoder_DecodeNonPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "boolean"
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var b bool
	err := dec.Decode(b)

	assert.Error(t, err)
}

func TestDecoder_DecodeNilPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "boolean"
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	err := dec.Decode((*bool)(nil))

	assert.Error(t, err)
}

func TestUnmarshal(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("int")

	var i int
	err := avro.Unmarshal(schema, []byte{0x02}, &i)

	assert.NoError(t, err)
	assert.Equal(t, 1, i)
}

func TestUnmarshal_TruncatedValue(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("int")

	var i int
	err := avro.Unmarshal(schema, []byte{0xE2}, &i)

	assert.ErrorIs(t, err, io.EOF)
}

func TestUnmarshal_Ptr(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("boolean")

	var b bool
	err := avro.Unmarshal(schema, []byte{0x01}, b)

	assert.Error(t, err)
}

func TestUnmarshal_NilPtr(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("boolean")

	err := avro.Unmarshal(schema, []byte{0x01}, (*bool)(nil))

	assert.Error(t, err)
}

func TestDecoder_NilSchemaReturnsError(t *testing.T) {
	var typedNil *avro.PrimitiveSchema
	tests := []struct {
		name   string
		schema avro.Schema
	}{
		{name: "nil interface"},
		{name: "typed nil", schema: typedNil},
	}

	for _, test := range tests {
		t.Run(test.name+"/stream", func(t *testing.T) {
			dec := avro.NewDecoderForSchema(test.schema, bytes.NewReader([]byte{0x01}))
			var value bool
			var err error

			assert.NotPanics(t, func() {
				err = dec.Decode(&value)
			})
			assert.Error(t, err)
		})

		t.Run(test.name+"/unmarshal", func(t *testing.T) {
			var value bool
			var err error

			assert.NotPanics(t, func() {
				err = avro.Unmarshal(test.schema, []byte{0x01}, &value)
			})
			assert.Error(t, err)
		})
	}
}

func FuzzDecoder(f *testing.F) {
	schema := avro.MustParse(`{
		"type":"record",
		"name":"FuzzRecord",
		"fields":[
			{"name":"text","type":"string"},
			{"name":"values","type":{"type":"array","items":"long"}},
			{"name":"labels","type":{"type":"map","values":"bytes"}},
			{"name":"choice","type":["null","int"]}
		]
	}`)
	config := avro.Config{
		MaxByteSliceSize:  1 << 20,
		MaxSliceAllocSize: 1 << 16,
		MaxMapAllocSize:   1 << 16,
	}.Freeze()

	type record struct {
		Text   string            `avro:"text"`
		Values []int64           `avro:"values"`
		Labels map[string][]byte `avro:"labels"`
		Choice any               `avro:"choice"`
	}

	seed, err := config.Marshal(schema, record{
		Text:   "seed",
		Values: []int64{1, -2, 3},
		Labels: map[string][]byte{"key": {0, 1, 2}},
		Choice: map[string]any{"int": 7},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Add([]byte{0xff})

	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 4<<20 {
			return
		}
		var value record
		_ = config.Unmarshal(schema, data, &value)
		var generic any
		_ = config.Unmarshal(schema, data, &generic)
	})
}

func TestUnmarshal_Nil(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("boolean")

	err := avro.Unmarshal(schema, []byte{0x01}, nil)

	assert.Error(t, err)
}
