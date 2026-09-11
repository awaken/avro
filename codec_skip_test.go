package avro_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime/debug"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const recursiveSkipHelper = "AVRO_RECURSIVE_SKIP_HELPER"

func TestDecoder_SkipRecursiveRecord(t *testing.T) {
	if os.Getenv(recursiveSkipHelper) == "1" {
		debug.SetMaxStack(1 << 20)

		tests := []struct {
			name   string
			schema string
		}{
			{
				name: "array",
				schema: `{
					"type":"record", "name":"node",
					"fields":[{"name":"children", "type":{"type":"array", "items":"node"}}]
				}`,
			},
			{
				name: "map",
				schema: `{
					"type":"record", "name":"node",
					"fields":[{"name":"children", "type":{"type":"map", "values":"node"}}]
				}`,
			},
			{
				name: "mutual",
				schema: `{
					"type":"record", "name":"a",
					"fields":[{"name":"children", "type":{"type":"array", "items":{
						"type":"record", "name":"b",
						"fields":[{"name":"parents", "type":{"type":"array", "items":"a"}}]
					}}}]
				}`,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				dec, err := avro.NewDecoder(test.schema, bytes.NewReader([]byte{0x00}))
				require.NoError(t, err)

				err = dec.Decode(&struct{}{})
				require.NoError(t, err)
			})
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestDecoder_SkipRecursiveRecord$")
	cmd.Env = append(os.Environ(), recursiveSkipHelper+"=1")
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "recursive skip helper failed:\n%s", out)
}

func TestDecoder_SkipBool(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "boolean"},
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

func TestDecoder_SkipNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "null"},
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

func TestDecoder_SkipInt(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "int"},
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

func TestDecoderSkipResolvedPromotedField(t *testing.T) {
	tests := []struct {
		writer string
		reader string
		value  any
	}{
		{writer: "int", reader: "long", value: int32(150)},
		{writer: "int", reader: "float", value: int32(150)},
		{writer: "int", reader: "double", value: int32(150)},
		{writer: "long", reader: "float", value: int64(150)},
		{writer: "long", reader: "double", value: int64(150)},
		{writer: "float", reader: "double", value: float32(1.5)},
		{writer: "string", reader: "bytes", value: "foo"},
		{writer: "bytes", reader: "string", value: []byte("foo")},
	}

	for _, test := range tests {
		t.Run(test.writer+"_to_"+test.reader, func(t *testing.T) {
			writer := avro.MustParse(fmt.Sprintf(`{
				"type":"record", "name":"test",
				"fields":[
					{"name":"a", "type":"%s"},
					{"name":"b", "type":"int"}
				]
			}`, test.writer))
			reader := avro.MustParse(fmt.Sprintf(`{
				"type":"record", "name":"test",
				"fields":[
					{"name":"a", "type":"%s"},
					{"name":"b", "type":"int"}
				]
			}`, test.reader))
			resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
			require.NoError(t, err)

			var data bytes.Buffer
			enc := avro.NewEncoderForSchema(writer, &data)
			err = enc.Encode(map[string]any{"a": test.value, "b": int32(7)})
			require.NoError(t, err)

			var got struct {
				B int32 `avro:"b"`
			}
			err = avro.NewDecoderForSchema(resolved, &data).Decode(&got)

			require.NoError(t, err)
			assert.Equal(t, int32(7), got.B)
		})
	}
}

func TestDecoder_SkipLong(t *testing.T) {
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

func TestDecoder_SkipFloat(t *testing.T) {
	data := []byte{0x0, 0x0, 0x0, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "float"},
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

func TestDecoder_SkipDouble(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "double"},
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

func TestDecoder_SkipBytes(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "bytes"},
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

func TestDecoder_SkipString(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string"},
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

func TestDecoder_SkipRecord(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "record", "name": "test2", "fields": [{"name": "c", "type": "string"}]}},
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

func TestDecoder_SkipRef(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f, 0x02, 0x66}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "record", "name": "test2", "fields": [{"name": "c", "type": "string"}]}},
	    {"name": "b", "type": "string"},
		{"name": "c", "type": "test2"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipEnum(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "enum", "name": "test2", "symbols": ["sym1", "sym2"]}},
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

func TestDecoderSkipEnumRejectsUnknownSymbol(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "enum", "name": "test2", "symbols": ["sym1", "sym2"]}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.ErrorContains(t, err, "unknown enum symbol")
}

func TestDecoder_SkipArray(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "array", "items": "int"}},
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

func TestDecoder_SkipArrayBlocks(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x03, 0x04, 0x36, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "array", "items": "int"}},
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

func TestDecoder_SkipMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x02, 0x66, 0x36, 0x02, 0x6f, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "map", "values": "int"}},
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

func TestDecoder_SkipMapBlocks(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x03, 0x0C, 0x02, 0x66, 0x36, 0x02, 0x6f, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "map", "values": "int"}},
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

func TestDecoder_SkipUnion(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"]},
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

func TestDecoder_SkipUnionNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"]},
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

func TestDecoder_SkipUnionInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x03, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"]},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_SkipFixed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x02, 0x66, 0x36, 0x02, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "fixed", "name": "test2", "size": 7}},
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

func TestDecoderSkipFixedRejectsTruncation(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "fixed", "name": "test2", "size": 2}}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader([]byte{0x01}))
	require.NoError(t, err)

	err = dec.Decode(&struct{}{})

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
