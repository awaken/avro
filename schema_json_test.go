package avro_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchema_JSON(t *testing.T) {
	tests := []struct {
		input string
		json  string
	}{
		{
			input: `"null"`,
			json:  `"null"`,
		},
		{
			input: `{"type":"null"}`,
			json:  `"null"`,
		},
		{
			input: `{"type":"null","other":"foo"}`,
			json:  `{"type":"null","other":"foo"}`,
		},
		{
			input: `"boolean"`,
			json:  `"boolean"`,
		},
		{
			input: `{"type":"boolean"}`,
			json:  `"boolean"`,
		},
		{
			input: `"int"`,
			json:  `"int"`,
		},
		{
			input: `{"type":"int"}`,
			json:  `"int"`,
		},
		{
			input: `{"type":"int","logicalType":"date"}`,
			json:  `{"type":"int","logicalType":"date"}`,
		},
		{
			input: `{"type":"int","logicalType":"time-millis"}`,
			json:  `{"type":"int","logicalType":"time-millis"}`,
		},
		{
			input: `{"type":"int"}`,
			json:  `"int"`,
		},
		{
			input: `"long"`,
			json:  `"long"`,
		},
		{
			input: `{"type":"long"}`,
			json:  `"long"`,
		},
		{
			input: `{"property-b": "value-bar", "type":"long", "property-a": "value-foo"}`,
			json:  `{"type":"long","property-a":"value-foo","property-b":"value-bar"}`,
		},
		{
			input: `{"type":"long","logicalType":"time-micros"}`,
			json:  `{"type":"long","logicalType":"time-micros"}`,
		},
		{
			input: `{"type":"long","logicalType":"timestamp-millis"}`,
			json:  `{"type":"long","logicalType":"timestamp-millis"}`,
		},
		{
			input: `{"type":"long","logicalType":"timestamp-millis"}`,
			json:  `{"type":"long","logicalType":"timestamp-millis"}`,
		},
		{
			input: `"float"`,
			json:  `"float"`,
		},
		{
			input: `{"type":"float"}`,
			json:  `"float"`,
		},
		{
			input: `"double"`,
			json:  `"double"`,
		},
		{
			input: `{"type":"double"}`,
			json:  `"double"`,
		},
		{
			input: `"bytes"`,
			json:  `"bytes"`,
		},
		{
			input: `{"type":"bytes"}`,
			json:  `"bytes"`,
		},
		{
			input: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			json:  `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			input: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}`,
			json:  `{"type":"bytes","logicalType":"decimal","precision":4}`,
		},
		{
			input: `"string"`,
			json:  `"string"`,
		},
		{
			input: `{"type":"string"}`,
			json:  `"string"`,
		},
		{
			input: `{"type":"string","logicalType":"uuid"}`,
			json:  `{"type":"string","logicalType":"uuid"}`,
		},
		{
			input: `[ "int"  ]`,
			json:  `["int"]`,
		},
		{
			input: `[ "int" , {"type":"boolean"} ]`,
			json:  `["int","boolean"]`,
		},
		{
			input: `{"fields":[], "type":"error", "name":"foo"}`,
			json:  `{"name":"foo","type":"error","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo"}`,
			json:  `{"name":"foo","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo", "namespace":"x.y"}`,
			json:  `{"name":"x.y.foo","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"a.b.foo", "namespace":"x.y"}`,
			json:  `{"name":"a.b.foo","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo", "doc":"Useful info"}`,
			json:  `{"name":"foo","doc":"Useful info","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo", "doc":"Useful info\n on multiple \n lines"}`,
			json:  `{"name":"foo","doc":"Useful info\n on multiple \n lines","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo", "aliases":["foo","bar"]}`,
			json:  `{"name":"foo","aliases":["foo","bar"],"type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}`,
			json:  `{"name":"foo","aliases":["foo","bar"],"doc":"foo","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "property-foo": "value-bar", "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}`,
			json:  `{"name":"foo","aliases":["foo","bar"],"doc":"foo","type":"record","fields":[],"property-foo":"value-bar"}`,
		},
		{
			input: `{"fields":[], "property\"foo": "value-bar", "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}`,
			json:  `{"name":"foo","aliases":["foo","bar"],"doc":"foo","type":"record","fields":[],"property\"foo":"value-bar"}`,
		},
		{
			input: `{"fields":[{"type":{"type":"boolean"}, "name":"f1"}], "type":"record", "name":"foo"}`,
			json:  `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
		},
		{
			input: `
{ "fields":[{"type":"boolean", "aliases":["foo"], "name":"f1", "default":true},
           {"order":"descending","name":"f2","doc":"Hello","type":"int"}],
 "type":"record", "name":"foo"
}`,
			json: `{"name":"foo","type":"record","fields":[{"name":"f1","aliases":["foo"],"type":"boolean","default":true},{"name":"f2","doc":"Hello","type":"int","order":"descending"}]}`,
		},
		{
			input: `{"type":"enum", "name":"foo", "symbols":["A1"]}`,
			json:  `{"name":"foo","type":"enum","symbols":["A1"]}`,
		},
		{
			input: `{"namespace":"x.y.z", "type":"enum", "name":"foo", "doc":"foo bar", "symbols":["A1", "A2"]}`,
			json:  `{"name":"x.y.z.foo","doc":"foo bar","type":"enum","symbols":["A1","A2"]}`,
		},
		{
			input: `{"name":"foo","type":"fixed","size":15}`,
			json:  `{"name":"foo","type":"fixed","size":15}`,
		},
		{
			input: `{"name":"foo","type":"fixed","logicalType":"duration","size":12}`,
			json:  `{"name":"foo","type":"fixed","size":12,"logicalType":"duration"}`,
		},
		{
			input: `{"name":"foo","type":"fixed","logicalType":"decimal","size":12,"precision":4,"scale":2}`,
			json:  `{"name":"foo","type":"fixed","size":12,"logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			input: `{"name":"foo","type":"fixed","logicalType":"decimal","size":12,"precision":4,"scale":0}`,
			json:  `{"name":"foo","type":"fixed","size":12,"logicalType":"decimal","precision":4}`,
		},
		{
			input: `{"namespace":"x.y.z", "type":"fixed", "name":"foo", "size":32}`,
			json:  `{"name":"x.y.z.foo","type":"fixed","size":32}`,
		},
		{
			input: `{ "items":{"type":"null"}, "type":"array"}`,
			json:  `{"type":"array","items":"null"}`,
		},
		{
			input: `{ "values":"string", "type":"map"}`,
			json:  `{"type":"map","values":"string"}`,
		},
		{
			input: `

 {"name":"PigValue","type":"record",
  "fields":[{"name":"value", "type":["null", "int", "long", "PigValue"]}]}
`,
			json: `{"name":"PigValue","type":"record","fields":[{"name":"value","type":["null","int","long","PigValue"]}]}`,
		},
		{
			input: `{
				"type":"record",
				"namespace": "org.hamba.avro",
				"name":"X",
  				"fields":[
					{"name":"value", "type":{
						"type":"record",
						"name":"Y",
						"fields":[
							{"name":"value", "type":"string"}
						]
					}}
				]
			}`,
			json: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"value","type":{"name":"org.hamba.avro.Y","type":"record","fields":[{"name":"value","type":"string"}]}}]}`,
		},
		{
			input: `{
				"type":"record",
				"namespace": "org.hamba.avro",
				"name":"X",
  				"fields":[
					{"name":"value", "type":{
						"type":"enum",
						"name":"Y",
						"symbols":["TEST"]
					}}
				]
			}`,
			json: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"value","type":{"name":"org.hamba.avro.Y","type":"enum","symbols":["TEST"]}}]}`,
		},
		{
			input: `{
				"type":"record",
				"namespace": "org.hamba.avro",
				"name":"X",
  				"fields":[
					{"name":"value", "type":{
						"type":"fixed",
						"name":"Y",
						"size":15
					}}
				]
			}`,
			json: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"value","type":{"name":"org.hamba.avro.Y","type":"fixed","size":15}}]}`,
		},
		{
			input: `{
				"type":"record",
				"namespace": "org.hamba.avro",
				"name":"X",
  				"fields":[
					{"name":"union_no_def","type":["null", "int"]},
					{"name":"union_with_def","type":["null", "string"],"default": null}
				]
			}`,
			json: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"union_no_def","type":["null","int"]},{"name":"union_with_def","type":["null","string"],"default":null}]}`,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.input, "", &avro.SchemaCache{})
			require.NoError(t, err)

			b, err := json.Marshal(schema)

			require.NoError(t, err)
			assert.Equal(t, test.json, string(b))
		})
	}
}

func TestSchema_JSONEscapesProgrammaticLogicalType(t *testing.T) {
	logicalType := avro.LogicalType("custom\"\n\\")
	logical := avro.NewPrimitiveLogicalSchema(logicalType)
	fixed, err := avro.NewFixedSchema("test", "", 1, logical)
	require.NoError(t, err)

	tests := []struct {
		name   string
		schema avro.Schema
	}{
		{name: "primitive", schema: avro.NewPrimitiveSchema(avro.String, logical)},
		{name: "fixed", schema: fixed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.schema)
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, json.Unmarshal(data, &got))
			assert.Equal(t, string(logicalType), got["logicalType"])
		})
	}
}

func TestPrimitiveLogicalSchema_StringEscapesCustomType(t *testing.T) {
	logicalType := avro.LogicalType("custom\"\n\\")
	logical := avro.NewPrimitiveLogicalSchema(logicalType)

	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte("{"+logical.String()+"}"), &got))
	assert.Equal(t, string(logicalType), got["logicalType"])
}

func TestSchema_JSONPreservesBytesAndFixedDefaults(t *testing.T) {
	const input = `{
		"type":"record",
		"name":"Defaults",
		"fields":[
			{"name":"bytes","type":"bytes","default":"\u0000\u00ff"},
			{"name":"fixed","type":{"type":"fixed","name":"F","size":2},"default":"\u0000\u00ff"},
			{
				"name":"nested",
				"type":{"type":"record","name":"Nested","fields":[{"name":"value","type":"bytes"}]},
				"default":{"value":"\u00ff"}
			}
		]
	}`

	schema, err := avro.ParseWithCache(input, "", &avro.SchemaCache{})
	require.NoError(t, err)
	data, err := json.Marshal(schema)
	require.NoError(t, err)

	var document struct {
		Fields []struct {
			Default any `json:"default"`
		} `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(data, &document))
	require.Len(t, document.Fields, 3)
	assert.Equal(t, "\x00ÿ", document.Fields[0].Default)
	assert.Equal(t, "\x00ÿ", document.Fields[1].Default)
	assert.Equal(t, map[string]any{"value": "ÿ"}, document.Fields[2].Default)

	reparsed, err := avro.ParseBytesWithCache(data, "", &avro.SchemaCache{})
	require.NoError(t, err)
	fields := reparsed.(*avro.RecordSchema).Fields()
	assert.Equal(t, []byte{0, 255}, fields[0].Default())
	assert.Equal(t, [2]byte{0, 255}, fields[1].Default())
	assert.Equal(t, map[string]any{"value": []byte{255}}, fields[2].Default())
}

func TestSchema_JSONPreservesDefaultAfterResolution(t *testing.T) {
	reader, err := avro.ParseWithCache(
		`{"type":"record","name":"R","fields":[{"name":"value","type":"bytes","default":"\u00ff"}]}`,
		"", &avro.SchemaCache{},
	)
	require.NoError(t, err)
	writer, err := avro.ParseWithCache(`{"type":"record","name":"R","fields":[]}`, "", &avro.SchemaCache{})
	require.NoError(t, err)

	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)
	data, err := json.Marshal(resolved)
	require.NoError(t, err)

	reparsed, err := avro.ParseBytesWithCache(data, "", &avro.SchemaCache{})
	require.NoError(t, err)
	assert.Equal(t, []byte{255}, reparsed.(*avro.RecordSchema).Fields()[0].Default())
}

func TestSchema_JSONEscapesNamesWhenValidationIsSkipped(t *testing.T) {
	old := avro.SkipNameValidation
	avro.SkipNameValidation = true
	t.Cleanup(func() {
		avro.SkipNameValidation = old
	})

	field, err := avro.NewField(`f"`, avro.NewPrimitiveSchema(avro.Int, nil))
	require.NoError(t, err)
	record, err := avro.NewRecordSchema(`R"`, "", []*avro.Field{field})
	require.NoError(t, err)
	enum, err := avro.NewEnumSchema(`E"`, "", []string{`A"`}, avro.WithDefault(`A"`))
	require.NoError(t, err)
	fixed, err := avro.NewFixedSchema(`F"`, "", 1, nil)
	require.NoError(t, err)

	tests := []struct {
		name   string
		schema avro.Schema
		want   string
	}{
		{
			name:   "record and field",
			schema: record,
			want:   `{"name":"R\"","type":"record","fields":[{"name":"f\"","type":"int"}]}`,
		},
		{
			name:   "enum name and default",
			schema: enum,
			want:   `{"name":"E\"","type":"enum","symbols":["A\""],"default":"A\""}`,
		},
		{
			name:   "fixed name",
			schema: fixed,
			want:   `{"name":"F\"","type":"fixed","size":1}`,
		},
		{
			name:   "reference name",
			schema: avro.NewRefSchema(record),
			want:   `"R\""`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.schema)
			require.NoError(t, err)
			assert.Equal(t, test.want, string(data))
			assert.True(t, json.Valid(data))
		})
	}
}
