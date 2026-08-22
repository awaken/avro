package avro_test

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_ArrayInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x38, 0x0}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_ArraySlice(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x38, 0x0}
	schema := `{"type":"array", "items": "int"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []int
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, []int{27, 28}, got)
}

func TestDecoder_ArrayEmptyReplacesDestination(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items":"int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader([]byte{0}))
	require.NoError(t, err)

	got := []int{27, 28}
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func TestDecoder_ArraySliceShortRead(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `{"type":"array", "items": "int"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []int
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArraySliceOfStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}
	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []TestRecord
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, []TestRecord{{A: 27, B: "foo"}, {A: 27, B: "foo"}}, got)
}

func TestDecoder_ArrayRecursiveStruct(t *testing.T) {
	defer ConfigTeardown()

	type record struct {
		A int      `avro:"a"`
		B []record `avro:"b"`
	}

	data := []byte{0x2, 0x3, 0x8, 0x4, 0x0, 0x6, 0x0, 0x0}
	schema := `{
	  "type": "record",
	  "name": "test",
	  "fields": [
		{
		  "name": "a",
		  "type": "int"
		},
		{
		  "name": "b",
		  "type": {
			"type": "array",
			"items": "test"
		  }
		}
	  ]
	}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got record
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, record{A: 1, B: []record{{A: 2, B: []record{}}, {A: 3, B: []record{}}}}, got)
}

func TestDecoder_ArraySliceError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []int
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArraySliceItemError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []int
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArrayMaxAllocationError(t *testing.T) {
	defer ConfigTeardown()
	data := []byte{0x2, 0x0, 0xe9, 0xe9, 0xe9, 0xe9, 0xe9, 0xe9, 0xe9, 0xe9, 0x0}
	schema := `{"type":"array", "items": { "type": "boolean" }}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []bool
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArrayExceedMaxSliceAllocationConfig(t *testing.T) {
	defer ConfigTeardown()

	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 5}.Freeze()

	// 10 (long) gets encoded to 0x14
	data := []byte{0x14}
	schema := `{"type":"array", "items": { "type": "boolean" }}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []bool
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArrayLimitIsCumulativeAcrossBlocks(t *testing.T) {
	defer ConfigTeardown()
	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 5}.Freeze()

	var data bytes.Buffer
	writer := avro.NewWriter(&data, 64)
	for range 2 {
		writer.WriteLong(3)
		for value := range 3 {
			writer.WriteLong(int64(value))
		}
	}
	writer.WriteLong(0)
	require.NoError(t, writer.Flush())

	decoder, err := avro.NewDecoder(`{"type":"array","items":"long"}`, bytes.NewReader(data.Bytes()))
	require.NoError(t, err)

	var values []int64
	err = decoder.Decode(&values)

	require.Error(t, err)
	assert.ErrorContains(t, err, "MaxSliceAllocSize")
}

func TestDecoder_ArrayExceedsRuntimeAllocationLimit(t *testing.T) {
	defer ConfigTeardown()
	if strconv.IntSize != 64 {
		t.Skip("the 64-bit allocation boundary is architecture-specific")
	}
	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: -1}.Freeze()

	var encoded bytes.Buffer
	w := avro.NewWriter(&encoded, 16)
	w.WriteLong(1 << 48)
	require.NoError(t, w.Flush())

	schema := avro.MustParse(`{"type":"array","items":"long"}`)
	var got []int64
	err := avro.Unmarshal(schema, encoded.Bytes(), &got)

	assert.ErrorContains(t, err, "size is greater than the runtime allocation limit")
}
