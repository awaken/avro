package gen_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/awaken/avro/v2/gen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "Update golden files")

func parseSchemaFileWithCache(t testing.TB, cache *avro.SchemaCache, path string) avro.Schema {
	t.Helper()

	schemaJSON, err := os.ReadFile(path)
	require.NoError(t, err)
	schema, err := avro.ParseBytesWithCache(schemaJSON, "", cache)
	require.NoError(t, err)
	return schema
}

func TestStruct_InvalidSchemaYieldsErr(t *testing.T) {
	err := gen.Struct(`asd`, &bytes.Buffer{}, gen.Config{})

	assert.Error(t, err)
}

func TestStruct_NonRecordSchemasAreNotSupported(t *testing.T) {
	err := gen.Struct(`{"type": "string"}`, &bytes.Buffer{}, gen.Config{})

	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "only")
	assert.Contains(t, strings.ToLower(err.Error()), "record schema")
}

func TestStructFromSchema_RejectsTypedNilRecord(t *testing.T) {
	var schema *avro.RecordSchema
	var err error

	assert.NotPanics(t, func() {
		err = gen.StructFromSchema(schema, io.Discard, gen.Config{})
	})
	assert.EqualError(t, err, "can only generate Go code from Record Schemas")
}

func TestStruct_HandlesNullField(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "test",
		"fields": [{"name": "value", "type": "null"}]
	}`

	_, lines := generate(t, schema, gen.Config{PackageName: "Something"})

	assert.Contains(t, lines, "Value any `avro:\"value\"`")
}

func TestStruct_HandlesProgrammaticPrimitiveNull(t *testing.T) {
	field, err := avro.NewField("value", avro.NewPrimitiveSchema(avro.Null, nil))
	require.NoError(t, err)
	schema, err := avro.NewRecordSchema("test", "", []*avro.Field{field})
	require.NoError(t, err)

	var output bytes.Buffer
	require.NoError(t, gen.StructFromSchema(schema, &output, gen.Config{PackageName: "Something"}))

	assert.Contains(t, removeSpaceAndEmptyLines(output.Bytes()), "Value any `avro:\"value\"`")
}

func TestStruct_HandlesProgrammaticPrimitiveNullUnion(t *testing.T) {
	tests := []struct {
		name  string
		types []avro.Schema
	}{
		{
			name: "null first",
			types: []avro.Schema{
				avro.NewPrimitiveSchema(avro.Null, nil),
				avro.NewPrimitiveSchema(avro.String, nil),
			},
		},
		{
			name: "null last",
			types: []avro.Schema{
				avro.NewPrimitiveSchema(avro.String, nil),
				avro.NewPrimitiveSchema(avro.Null, nil),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			union, err := avro.NewUnionSchema(test.types)
			require.NoError(t, err)
			field, err := avro.NewField("value", union)
			require.NoError(t, err)
			schema, err := avro.NewRecordSchema("test", "", []*avro.Field{field})
			require.NoError(t, err)

			var output bytes.Buffer
			require.NoError(t, gen.StructFromSchema(schema, &output, gen.Config{PackageName: "Something"}))

			assert.Contains(t, removeSpaceAndEmptyLines(output.Bytes()), "Value *string `avro:\"value\"`")
		})
	}
}

func TestStruct_AvroStyleCannotBeOverridden(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`
	gc := gen.Config{
		PackageName: "Something",
		Tags: map[string]gen.TagStyle{
			"avro": gen.Kebab,
		},
	}

	_, lines := generate(t, schema, gc)

	for _, expected := range []string{
		"package something",
		"type Test struct {",
		"SomeString string `avro:\"someString\"`",
		"}",
	} {
		assert.Contains(t, lines, expected, "avro tags should not be configurable, they need to match the schema")
	}
}

func TestStruct_HandlesGoInitialisms(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "httpRecord",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`
	gc := gen.Config{
		PackageName: "Something",
	}

	_, lines := generate(t, schema, gc)

	assert.Contains(t, lines, "type HTTPRecord struct {")
}

func TestStruct_MultilineDoc(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Test",
  "doc": "Test record doc\nMultiline record comments",
  "fields": [
    { "name": "someString", "type": "string", "doc": "Test field doc\nMultiline field comments" }
  ]
}`
	gc := gen.Config{
		PackageName: "Something",
	}

	_, lines := generate(t, schema, gc)

	for _, expected := range []string{
		"// Test record doc",
		"// Multiline record comments.",
		"// Test field doc",
		"// Multiline field comments.",
	} {
		assert.Contains(t, lines, expected)
	}
}

func TestStruct_EscapeBacktick(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "Test",
  "doc": "Test record doc with ` + "`" + `backticks` + "`" + `",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`
	gc := gen.Config{
		PackageName: "Something",
		Encoders:    true,
		FullSchema:  true,
	}

	_, lines := generate(t, schema, gc)

	for _, expected := range []string{
		"var schemaTest = avro.MustParse(`{\"name\":\"Test\",\"doc\":\"Test record doc with ` + \"`\" + `backticks` + \"`\" + `\",\"type\":\"record\",\"fields\":[{\"name\":\"someString\",\"type\":\"string\"}]}`)",
	} {
		assert.Contains(t, lines, expected)
	}
}

func TestStruct_HandlesAdditionalInitialisms(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "CidOverHttpRecord",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`
	gc := gen.Config{
		PackageName: "Something",
		Initialisms: []string{"CID"},
	}

	_, lines := generate(t, schema, gc)

	assert.Contains(t, lines, "type CIDOverHTTPRecord struct {")
}

func TestStruct_HandlesStrictTypes(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someString", "type": "int" }
  ]
}`
	gc := gen.Config{
		PackageName: "Something",
		StrictTypes: true,
	}

	_, lines := generate(t, schema, gc)

	assert.Contains(t, lines, "SomeString int32 `avro:\"someString\"`")
}

func TestStruct_CustomLogicalTypeOverridesStrictMapping(t *testing.T) {
	logical := avro.NewPrimitiveLogicalSchema("custom")
	field, err := avro.NewField("value", avro.NewPrimitiveSchema(avro.Int, logical))
	require.NoError(t, err)
	schema, err := avro.NewRecordSchema("test", "", []*avro.Field{field})
	require.NoError(t, err)

	var output bytes.Buffer
	err = gen.StructFromSchema(schema, &output, gen.Config{
		PackageName: "Something",
		StrictTypes: true,
		LogicalTypes: []gen.LogicalType{{
			Name: "custom",
			Typ:  "int",
		}},
	})
	require.NoError(t, err)

	assert.Contains(t, removeSpaceAndEmptyLines(output.Bytes()), "Value int `avro:\"value\"`")
}

func TestStruct_HandlesLocalTimestamps(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "test",
		"fields": [
			{"name": "millis", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
			{"name": "micros", "type": {"type": "long", "logicalType": "local-timestamp-micros"}}
		]
	}`

	_, lines := generate(t, schema, gen.Config{PackageName: "Something"})

	assert.Contains(t, lines, `"time"`)
	assert.Contains(t, lines, "Millis time.Time `avro:\"millis\"`")
	assert.Contains(t, lines, "Micros time.Time `avro:\"micros\"`")
}

func TestStruct_ConfigurableFieldTags(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someSTRING", "type": "string" }
  ]
}`

	tests := []struct {
		tagStyle    gen.TagStyle
		expectedTag string
	}{
		{tagStyle: gen.Camel, expectedTag: "json:\"someString\""},
		{tagStyle: gen.Snake, expectedTag: "json:\"some_string\""},
		{tagStyle: gen.Kebab, expectedTag: "json:\"some-string\""},
		{tagStyle: gen.UpperCamel, expectedTag: "json:\"SomeString\""},
		{tagStyle: gen.Original, expectedTag: "json:\"someSTRING\""},
		{tagStyle: gen.TagStyle(""), expectedTag: "json:\"someSTRING\""},
	}

	for _, test := range tests {
		test := test
		t.Run(string(test.tagStyle), func(t *testing.T) {
			gc := gen.Config{
				PackageName: "Something",
				Tags: map[string]gen.TagStyle{
					"json": test.tagStyle,
				},
			}
			_, lines := generate(t, schema, gc)

			for _, expected := range []string{
				"package something",
				"type Test struct {",
				"SomeString string `avro:\"someSTRING\" " + test.expectedTag + "`",
				"}",
			} {
				assert.Contains(t, lines, expected)
			}
		})
	}
}

func TestStruct_RejectsInvalidTagNames(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "test",
		"fields": [{"name": "value", "type": "string"}]
	}`
	tests := []string{
		"",
		"bad key",
		`bad"key`,
		"bad:key",
		"bad\nkey",
		"bad\x7fkey",
		string([]byte{0xff}),
	}

	for _, tag := range tests {
		t.Run(strconv.Quote(tag), func(t *testing.T) {
			err := gen.Struct(schema, io.Discard, gen.Config{
				PackageName: "something",
				Tags:        map[string]gen.TagStyle{tag: gen.Original},
			})

			assert.EqualError(t, err, "invalid struct tag name "+strconv.Quote(tag))
		})
	}
}

func TestStruct_ConfigurableLogicalTypes(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "id", "type": {"type": "string", "logicalType": "uuid"} }
  ]
}`

	gc := gen.Config{
		PackageName: "Something",
		LogicalTypes: []gen.LogicalType{{
			Name:             "uuid",
			Typ:              "uuid.UUID",
			ThirdPartyImport: "github.com/google/uuid",
		}},
	}
	_, lines := generate(t, schema, gc)

	for _, expected := range []string{
		"package something",
		"import (",
		"\"github.com/google/uuid\"",
		"type Test struct {",
		"ID uuid.UUID `avro:\"id\"`",
		"}",
	} {
		assert.Contains(t, lines, expected)
	}
}

func TestStruct_UnmappedLogicalTypesUseUnderlyingTypes(t *testing.T) {
	logical := avro.NewPrimitiveLogicalSchema("custom")
	fixed, err := avro.NewFixedSchema("token", "", 4, logical)
	require.NoError(t, err)
	fields := make([]*avro.Field, 2)
	fields[0], err = avro.NewField("text", avro.NewPrimitiveSchema(avro.String, logical))
	require.NoError(t, err)
	fields[1], err = avro.NewField("token", fixed)
	require.NoError(t, err)
	schema, err := avro.NewRecordSchema("test", "", fields)
	require.NoError(t, err)

	var output bytes.Buffer
	require.NoError(t, gen.StructFromSchema(schema, &output, gen.Config{PackageName: "Something"}))
	lines := removeSpaceAndEmptyLines(output.Bytes())

	assert.Contains(t, lines, "Text string `avro:\"text\"`")
	assert.Contains(t, lines, "Token [4]byte `avro:\"token\"`")
}

func TestStruct_InvalidStandardLogicalTypesUseUnderlyingTypes(t *testing.T) {
	fields := make([]*avro.Field, 0, 9)
	addField := func(name string, schema avro.Schema) {
		t.Helper()

		field, err := avro.NewField(name, schema)
		require.NoError(t, err)
		fields = append(fields, field)
	}

	addField("bad_date", avro.NewPrimitiveSchema(avro.String, avro.NewPrimitiveLogicalSchema(avro.Date)))
	addField("bad_timestamp", avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.TimestampMillis)))
	addField("bad_uuid", avro.NewPrimitiveSchema(avro.Bytes, avro.NewPrimitiveLogicalSchema(avro.UUID)))
	badDuration, err := avro.NewFixedSchema("bad_duration", "", 4, avro.NewPrimitiveLogicalSchema(avro.Duration))
	require.NoError(t, err)
	addField("bad_duration", badDuration)
	addField("bad_bytes_decimal", avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(2, 3)))
	badFixedDecimal, err := avro.NewFixedSchema("bad_fixed_decimal", "", 1, avro.NewDecimalLogicalSchema(3, 0))
	require.NoError(t, err)
	addField("bad_fixed_decimal", badFixedDecimal)
	validDuration, err := avro.NewFixedSchema("valid_duration", "", 12, avro.NewPrimitiveLogicalSchema(avro.Duration))
	require.NoError(t, err)
	addField("valid_duration", validDuration)
	addField("valid_bytes_decimal", avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(4, 2)))
	validFixedDecimal, err := avro.NewFixedSchema("valid_fixed_decimal", "", 4, avro.NewDecimalLogicalSchema(4, 2))
	require.NoError(t, err)
	addField("valid_fixed_decimal", validFixedDecimal)

	schema, err := avro.NewRecordSchema("test", "", fields)
	require.NoError(t, err)
	var output bytes.Buffer
	require.NoError(t, gen.StructFromSchema(schema, &output, gen.Config{PackageName: "Something"}))
	lines := removeSpaceAndEmptyLines(output.Bytes())

	for _, expected := range []string{
		"BadDate string `avro:\"bad_date\"`",
		"BadTimestamp int `avro:\"bad_timestamp\"`",
		"BadUUID []byte `avro:\"bad_uuid\"`",
		"BadDuration [4]byte `avro:\"bad_duration\"`",
		"BadBytesDecimal []byte `avro:\"bad_bytes_decimal\"`",
		"BadFixedDecimal [1]byte `avro:\"bad_fixed_decimal\"`",
		"ValidDuration avro.LogicalDuration `avro:\"valid_duration\"`",
		"ValidBytesDecimal *big.Rat `avro:\"valid_bytes_decimal\"`",
		"ValidFixedDecimal *big.Rat `avro:\"valid_fixed_decimal\"`",
	} {
		assert.Contains(t, lines, expected)
	}
}

func TestStruct_GenFromRecordSchema(t *testing.T) {
	fileName := "testdata/golden.go"
	gc := gen.Config{PackageName: "Something"}
	schema, err := os.ReadFile("testdata/golden.avsc")
	require.NoError(t, err)

	file, _ := generate(t, string(schema), gc)

	if *update {
		err = os.WriteFile(fileName, file, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile(fileName)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(file))
}

func TestStruct_GenFromRecordSchemaWithCustomLogicalTypes(t *testing.T) {
	fileName := "testdata/golden_logicaltype.go"

	gc := gen.Config{PackageName: "Something", LogicalTypes: []gen.LogicalType{{
		Name:             "uuid",
		Typ:              "uuid.UUID",
		ThirdPartyImport: "github.com/google/uuid",
	}}}
	schema, err := os.ReadFile("testdata/golden.avsc")
	require.NoError(t, err)

	file, _ := generate(t, string(schema), gc)

	if *update {
		err = os.WriteFile(fileName, file, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile(fileName)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(file))
}

func TestStruct_GenFromRecordSchemaWithFullName(t *testing.T) {
	schema, err := os.ReadFile("testdata/golden.avsc")
	require.NoError(t, err)

	gc := gen.Config{PackageName: "Something", FullName: true}
	file, _ := generate(t, string(schema), gc)

	if *update {
		err = os.WriteFile("testdata/golden_fullname.go", file, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_fullname.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(file))
}

func TestStruct_GenFromRecordSchemaWithEncoders(t *testing.T) {
	schema, err := os.ReadFile("testdata/golden.avsc")
	require.NoError(t, err)

	gc := gen.Config{PackageName: "Something", Encoders: true}
	file, _ := generate(t, string(schema), gc)

	if *update {
		err = os.WriteFile("testdata/golden_encoders.go", file, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_encoders.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(file))
}

func TestStruct_GenFromRecordSchemaWithFullSchema(t *testing.T) {
	schema, err := os.ReadFile("testdata/golden.avsc")
	require.NoError(t, err)

	gc := gen.Config{PackageName: "Something", FullSchema: true, Encoders: true}
	file, _ := generate(t, string(schema), gc)

	if *update {
		err = os.WriteFile("testdata/golden_encoders_fullschema.go", file, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_encoders_fullschema.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(file))
}

func TestGenerator_GenEnum(t *testing.T) {
	goldenSchema, err := avro.ParseFiles("testdata/golden.avsc")
	require.NoError(t, err)

	g := gen.NewGenerator("something", map[string]gen.TagStyle{}, gen.WithEnums(true))
	g.Parse(goldenSchema)

	var buf bytes.Buffer
	err = g.Write(&buf)
	require.NoError(t, err)

	formatted, err := format.Source(buf.Bytes())
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_enum.go", formatted, 0600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_enum.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(formatted))
}

func TestGenerator(t *testing.T) {
	cache := &avro.SchemaCache{}
	unionSchema := parseSchemaFileWithCache(t, cache, "testdata/uniontype.avsc")
	mainSchema := parseSchemaFileWithCache(t, cache, "testdata/main.avsc")

	g := gen.NewGenerator("something", map[string]gen.TagStyle{})
	g.Parse(unionSchema)
	g.Parse(mainSchema)

	var buf bytes.Buffer
	err := g.Write(&buf)
	require.NoError(t, err)

	formatted, err := format.Source(buf.Bytes())
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_multiple.go", formatted, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_multiple.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(formatted))
}

func TestGenerator_ResetClearsOutputAndPreservesOptions(t *testing.T) {
	first := avro.MustParse(`{
		"type": "record",
		"name": "First",
		"fields": [{
			"name": "status",
			"type": {"type": "enum", "name": "Status", "symbols": ["READY"]}
		}]
	}`)
	second := avro.MustParse(`{
		"type": "record",
		"name": "Second",
		"fields": [{"name": "value", "type": "string"}]
	}`)
	g := gen.NewGenerator("something", nil, gen.WithEncoders(true), gen.WithEnums(true))
	g.Parse(first)

	g.Reset()
	g.Parse(second)

	var output bytes.Buffer
	require.NoError(t, g.Write(&output))
	assert.NotContains(t, output.String(), "type Status string")
	assert.Contains(t, output.String(), `"github.com/awaken/avro/v2"`)
	assert.NotContains(t, output.String(), "type First struct")
	assert.Contains(t, output.String(), "type Second struct")
}

func TestGenerator_EncoderOptionsCompose(t *testing.T) {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields": [{"name": "value", "type": "string"}]
	}`)
	tests := []struct {
		name         string
		opts         []gen.OptsFunc
		wantImports  int
		wantEncoders bool
	}{
		{
			name:         "repeated enable",
			opts:         []gen.OptsFunc{gen.WithEncoders(true), gen.WithEncoders(true)},
			wantImports:  1,
			wantEncoders: true,
		},
		{
			name:         "later disable",
			opts:         []gen.OptsFunc{gen.WithEncoders(true), gen.WithEncoders(false)},
			wantImports:  0,
			wantEncoders: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := gen.NewGenerator("something", nil, test.opts...)
			g.Parse(schema)

			var output bytes.Buffer
			require.NoError(t, g.Write(&output))
			assert.Equal(t, test.wantImports, strings.Count(output.String(), `"github.com/awaken/avro/v2"`))
			assert.Equal(t, test.wantEncoders, strings.Contains(output.String(), "func (o *Test) Schema() avro.Schema"))
		})
	}
}

func TestGenerator_FullSchemaMarshalError(t *testing.T) {
	bad, err := avro.NewRecordSchema(
		"bad",
		"",
		nil,
		avro.WithProps(map[string]any{"unsupported": make(chan int)}),
	)
	require.NoError(t, err)
	good := avro.MustParse(`{"type":"record","name":"good","fields":[]}`)
	g := gen.NewGenerator("something", nil, gen.WithEncoders(true), gen.WithFullSchema(true))

	assert.NotPanics(t, func() {
		g.Parse(bad)
	})
	err = g.Write(io.Discard)
	assert.ErrorContains(t, err, "failed to marshal raw schema for 'bad'")

	g.Reset()
	g.Parse(good)
	assert.NoError(t, g.Write(io.Discard))
}

func TestGenerator_RejectsNilSchemas(t *testing.T) {
	tests := []struct {
		name   string
		schema avro.Schema
	}{
		{name: "nil"},
		{name: "typed nil", schema: (*avro.ArraySchema)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := gen.NewGenerator("something", nil)

			assert.NotPanics(t, func() {
				g.Parse(test.schema)
			})
			assert.EqualError(t, g.Write(io.Discard), "cannot generate Go code from a nil schema")
		})
	}
}

func TestGenerator_RejectsNilReferencedSchema(t *testing.T) {
	var target *avro.RecordSchema
	field, err := avro.NewField("value", avro.NewRefSchema(target))
	require.NoError(t, err)
	schema, err := avro.NewRecordSchema("test", "", []*avro.Field{field})
	require.NoError(t, err)

	var output bytes.Buffer
	assert.NotPanics(t, func() {
		err = gen.StructFromSchema(schema, &output, gen.Config{PackageName: "something"})
	})
	assert.EqualError(t, err, "cannot generate Go code from a nil schema")
}

func TestGenerator_RejectsUnsupportedSchemaImplementations(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		g := gen.NewGenerator("something", nil)
		g.Parse(unsupportedSchema{})

		assert.ErrorContains(t, g.Write(io.Discard), "unsupported schema implementation")
	})

	t.Run("record field", func(t *testing.T) {
		field, err := avro.NewField("value", unsupportedSchema{})
		require.NoError(t, err)
		schema, err := avro.NewRecordSchema("test", "", []*avro.Field{field})
		require.NoError(t, err)

		err = gen.StructFromSchema(schema, io.Discard, gen.Config{PackageName: "something"})
		assert.ErrorContains(t, err, "unsupported schema implementation")
	})
}

func TestGenerator_CustomTemplateWithMetadata(t *testing.T) {
	cache := &avro.SchemaCache{}
	unionSchema := parseSchemaFileWithCache(t, cache, "testdata/uniontype.avsc")
	mainSchema := parseSchemaFileWithCache(t, cache, "testdata/main.avsc")

	template := `// Code generated by avro/gen. DO NOT EDIT.
package {{ .PackageName }}

{{- range .Typedefs }}
	{{- if .Metadata }}
	// metadata: {{ .Metadata }}
	{{- end }}
	type {{ .Name }} struct {
	// fields ommitted for brevity
	}
{{- end }}`

	g := gen.NewGenerator("something", map[string]gen.TagStyle{}, gen.WithTemplate(template))
	g.ParseWithMetadata(unionSchema, "metadata for union schema")
	g.ParseWithMetadata(mainSchema, map[string]any{"metadata for": "main schema"})

	var buf bytes.Buffer
	err := g.Write(&buf)
	require.NoError(t, err)

	formatted, err := format.Source(buf.Bytes())
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_metadata.go", formatted, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_metadata.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(formatted))
}

func TestStruct_HostileSchemaCannotInjectGoDeclarations(t *testing.T) {
	avro.SkipNameValidation = true
	defer func() {
		avro.SkipNameValidation = false
	}()

	hostile := "x`; func init() { panic(1) }; var injected = `"
	schema := map[string]any{
		"type": "record",
		"name": "Hostile" + hostile,
		"fields": []any{
			map[string]any{
				"name": "field" + hostile,
				"type": map[string]any{
					"type":    "enum",
					"name":    "Enum" + hostile,
					"symbols": []string{"SAFE", hostile},
				},
			},
		},
	}
	encoded, err := json.Marshal(schema)
	require.NoError(t, err)

	var output bytes.Buffer
	err = gen.Struct(string(encoded), &output, gen.Config{
		PackageName: "securitytest",
		Encoders:    true,
	})
	require.NoError(t, err)

	file, err := parser.ParseFile(token.NewFileSet(), "generated.go", output.Bytes(), 0)
	require.NoError(t, err)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "init" {
			t.Fatal("hostile schema injected an init function")
		}
	}
}

// generate is a utility to run the generation and return the result as a tuple
func generate(t *testing.T, schema string, gc gen.Config) ([]byte, []string) {
	t.Helper()

	buf := &bytes.Buffer{}
	err := gen.Struct(schema, buf, gc)
	require.NoError(t, err)

	b := make([]byte, buf.Len())
	copy(b, buf.Bytes())

	return buf.Bytes(), removeSpaceAndEmptyLines(b)
}

func removeSpaceAndEmptyLines(goCode []byte) []string {
	var lines []string
	for _, lineBytes := range bytes.Split(goCode, []byte("\n")) {
		if len(lineBytes) == 0 {
			continue
		}
		trimmed := removeMoreThanOneConsecutiveSpaces(lineBytes)
		lines = append(lines, trimmed)
	}
	return lines
}

// removeMoreThanOneConsecutiveSpaces replaces all sequences of more than one space, with a single one
func removeMoreThanOneConsecutiveSpaces(lineBytes []byte) string {
	lines := strings.TrimSpace(string(lineBytes))
	return strings.Join(regexp.MustCompile(`\s+|\t+`).Split(lines, -1), " ")
}

type unsupportedSchema struct{}

func (unsupportedSchema) Type() avro.Type {
	return avro.String
}

func (unsupportedSchema) String() string {
	return `"string"`
}

func (unsupportedSchema) Fingerprint() [32]byte {
	return [32]byte{}
}

func (unsupportedSchema) FingerprintUsing(avro.FingerprintType) ([]byte, error) {
	return nil, nil
}

func (unsupportedSchema) CacheFingerprint() [32]byte {
	return [32]byte{}
}
