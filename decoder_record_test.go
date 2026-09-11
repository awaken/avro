package avro_test

import (
	"bytes"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_RecordStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordStructPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &TestRecord{}
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, &TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordStructPtrNil(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got *TestRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, &TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordStructWithFieldAlias(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "c", "aliases": ["a"], "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got TestRecord
	err = dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordPartialStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_RecordStructInvalidData(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestRecord
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RecordEmbeddedStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestEmbeddedRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestEmbeddedRecord{TestEmbed: TestEmbed{A: 27, B: "foo"}, C: "bar"}, got)
}

func TestDecoder_RecordEmbeddedPtrStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestEmbeddedPtrRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestEmbeddedPtrRecord{TestEmbed: &TestEmbed{A: 27, B: "foo"}, C: "bar"}, got)
}

func TestDecoder_RecordEmbeddedPtrStructNew(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestEmbeddedPtrRecord
	got.C = "nonzero" // non-zero value here triggers bug in allocating TestEmbed
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestEmbeddedPtrRecord{TestEmbed: &TestEmbed{A: 27, B: "foo"}, C: "bar"}, got)
}

func TestDecoder_RecordEmbeddedIntStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestEmbeddedIntRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestEmbeddedIntRecord{B: "foo"}, got)
}

func TestDecoder_RecordMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x02, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
		{"name": "c", "type": ["null","string"]},
		{"name": "d", "type": "null"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[string]any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a": int64(27), "b": "foo", "c": "foo", "d": nil}, got)
}

func TestDecoder_RecordMapStopsAfterFieldError(t *testing.T) {
	reader := avro.MustParse(`{
		"type":"record","name":"test","fields":[
			{"name":"value","type":"string"},
			{"name":"fallback","type":"int","default":1}
		]
	}`)
	writer := avro.MustParse(`{
		"type":"record","name":"test","fields":[
			{"name":"value","type":"string"}
		]
	}`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	calls := 0
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Int,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			calls++
			return in, nil
		},
	})
	dec := api.NewDecoder(resolved, bytes.NewReader([]byte{0x02}))
	var got map[string]any

	err = dec.Decode(&got)

	require.Error(t, err)
	assert.Zero(t, calls)
	assert.NotContains(t, got, "fallback")
}

func TestDecoder_RecordStructStopsAfterSkippedFieldError(t *testing.T) {
	reader := avro.MustParse(`{
		"type":"record","name":"test","fields":[
			{"name":"value","type":"string"},
			{"name":"fallback","type":"int","default":1}
		]
	}`)
	writer := avro.MustParse(`{
		"type":"record","name":"test","fields":[
			{"name":"value","type":"string"}
		]
	}`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	calls := 0
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Int,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			calls++
			return in, nil
		},
	})
	dec := api.NewDecoder(resolved, bytes.NewReader([]byte{0x02}))
	var got struct {
		Fallback any `avro:"fallback"`
	}

	err = dec.Decode(&got)

	require.Error(t, err)
	assert.Zero(t, calls)
	assert.Nil(t, got.Fallback)
}

func TestDecoder_RecordStructStopsAfterFieldEOF(t *testing.T) {
	reader := avro.MustParse(`{
		"type":"record","name":"test","fields":[
			{"name":"value","type":"string"},
			{"name":"fallback","type":"int","default":1}
		]
	}`)
	writer := avro.MustParse(`{
		"type":"record","name":"test","fields":[
			{"name":"value","type":"string"}
		]
	}`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	calls := 0
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Int,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			calls++
			return in, nil
		},
	})
	dec := api.NewDecoder(resolved, bytes.NewReader([]byte{0x80}))
	var got struct {
		Value    string `avro:"value"`
		Fallback any    `avro:"fallback"`
	}

	err = dec.Decode(&got)

	require.Error(t, err)
	assert.Zero(t, calls)
	assert.Nil(t, got.Fallback)
}

func TestDecoder_RecordMapInvalidKey(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[int]any
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RecordMapInvalidElem(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[string]string
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RecordMapInvalidData(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[string]any
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RecordInterface(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestInterface = &TestRecord{}
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, &TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordEmptyInterface(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestInterface
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RefStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "parent",
	"fields" : [
		{"name": "a", "type": {
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "long"},
	    		{"name": "b", "type": "string"}
			]}
		},
	    {"name": "b", "type": "test"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestNestedRecord
	err = dec.Decode(&got)

	want := TestNestedRecord{
		A: TestRecord{A: 27, B: "foo"},
		B: TestRecord{A: 27, B: "foo"},
	}
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
