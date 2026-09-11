package gen_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/awaken/avro/v2"
	"github.com/awaken/avro/v2/gen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "Update golden files")

func TestGeneratorNameCollisions(t *testing.T) {
	for _, schema := range []string{
		`{"type":"record","name":"Collision","fields":[{"name":"foo_bar","type":"int"},{"name":"fooBar","type":"int"}]}`,
		`{"type":"record","name":"Collision","fields":[{"name":"schema","type":"int"}]}`,
		`{"type":"record","name":"Collision","fields":[{"name":"e","type":{"type":"enum","name":"State","symbols":["foo_bar","fooBar"]}}]}`,
	} {
		g := gen.NewGenerator("generated", nil, gen.WithEncoders(true), gen.WithEnums(true))
		g.Parse(avro.MustParse(schema))
		var output bytes.Buffer
		assert.Error(t, g.Write(&output), schema)
		assert.Empty(t, output.String(), "failed generation must not write partial source")
	}
}

func TestGeneratorBlankIdentifiers(t *testing.T) {
	for _, schema := range []string{
		`{"type":"record","name":"_","fields":[]}`,
		`{"type":"record","name":"BlankField","fields":[{"name":"_","type":"int"}]}`,
		`{"type":"record","name":"BlankEnum","fields":[{"name":"value","type":{"type":"enum","name":"_","symbols":["A"]}}]}`,
		`{"type":"record","name":"NumericField","fields":[{"name":"_1","type":"int"}]}`,
	} {
		g := gen.NewGenerator("generated", nil, gen.WithEnums(true))
		g.Parse(avro.MustParse(schema))
		var output bytes.Buffer
		assert.Error(t, g.Write(&output), schema)
		assert.Empty(t, output.String())
	}
}

func TestGeneratorReferenceDeclaration(t *testing.T) {
	external := avro.MustParse(`{"type":"record","name":"External","fields":[{"name":"value","type":"int"}]}`).(*avro.RecordSchema)
	field, err := avro.NewField("value", avro.NewRefSchema(external))
	require.NoError(t, err)
	root, err := avro.NewRecordSchema("Root", "", []*avro.Field{field})
	require.NoError(t, err)
	for _, encoders := range []bool{false, true} {
		g := gen.NewGenerator("generated", nil, gen.WithEncoders(encoders))
		g.Parse(root)
		empty, err := avro.NewRecordSchema("Empty", "tree", nil)
		require.NoError(t, err)
		g.Parse(empty)
		var output bytes.Buffer
		require.NoError(t, g.Write(&output))
		testGeneratedPackage(t, output.Bytes(), `package generated
import "testing"
func TestReferencedValue(t *testing.T) {
 value := Root{Value: External{Value: 7}}
 if value.Value.Value != 7 { t.Fatal(value) }
}`)
	}
}

func TestGeneratorRecursiveInitialization(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(strconv.FormatBool(full), func(t *testing.T) {
			g := gen.NewGenerator("generated", nil, gen.WithEncoders(true), gen.WithFullSchema(full))
			g.Parse(avro.MustParse(`{"type":"record","name":"Parent","namespace":"generation","fields":[{"name":"value","type":"int"},{"name":"child","type":{"type":"record","name":"Child","fields":[{"name":"parent","type":["null","Parent"],"default":null}]}}]}`))
			var output bytes.Buffer
			require.NoError(t, g.Write(&output))
			testGeneratedPackage(t, output.Bytes(), `package generated
import (
 "testing"
 "github.com/awaken/avro/v2"
)
func TestRecursiveValue(t *testing.T) {
 value := Parent{Value: 7, Child: Child{Parent: &Parent{Value: 9}}}
 data, err := value.Marshal()
 if err != nil { t.Fatal(err) }
 var got Parent
 if err := got.Unmarshal(data); err != nil { t.Fatal(err) }
 if got.Value != 7 || got.Child.Parent.Value != 9 { t.Fatal(got) }
 if got.Schema().(avro.NamedSchema).FullName() != "generation.Parent" { t.Fatal("parent schema") }
 if got.Child.Schema().(avro.NamedSchema).FullName() != "generation.Child" { t.Fatal("child schema") }
 if avro.DefaultSchemaCache.Get("generation.Parent") != nil { t.Fatal("generated code changed the global schema cache") }
}`)
		})
	}
}

func TestGeneratorNamedTypes(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"NamedRoot","fields":[{"name":"first","type":{"type":"enum","name":"one.State","symbols":["READY"]}},{"name":"second","type":{"type":"enum","name":"two.State","symbols":["WAIT"]}}]}`)
	short := gen.NewGenerator("generated", nil, gen.WithEnums(true))
	short.Parse(schema)
	require.ErrorContains(t, short.Write(io.Discard), "collides")
	full := gen.NewGenerator("generated", nil, gen.WithEnums(true), gen.WithFullName(true))
	full.Parse(schema)
	var output bytes.Buffer
	require.NoError(t, full.Write(&output))
	testGeneratedPackage(t, output.Bytes(), `package generated
import "testing"
func TestEnumNames(t *testing.T) {
 value := NamedRoot{First: OneStateReady, Second: TwoStateWait}
 if value.First != "READY" || value.Second != "WAIT" { t.Fatal(value) }
}`)
	for _, schemas := range [][]string{
		{`{"type":"record","name":"one.State","fields":[]}`, `{"type":"enum","name":"two.State","symbols":["READY"]}`},
		{`{"type":"enum","name":"Status","symbols":["READY"]}`, `{"type":"record","name":"StatusReady","fields":[]}`},
		{`{"type":"record","name":"Same","fields":[]}`, `{"type":"fixed","name":"Same","size":4}`},
		{`{"type":"record","name":"Same","fields":[{"name":"value","type":"int"}]}`, `{"type":"record","name":"Same","fields":[{"name":"value","type":"string"}]}`},
	} {
		g := gen.NewGenerator("generated", nil, gen.WithEnums(true))
		for _, text := range schemas {
			g.Parse(avro.MustParse(text))
		}
		var output bytes.Buffer
		err := g.Write(&output)
		require.Error(t, err)
		require.Empty(t, output.String())
		g.Parse(avro.MustParse(`{"type":"record","name":"Good","fields":[]}`))
		require.Equal(t, err, g.Write(&output), "error remains sticky")
		g.Reset()
		g.Parse(avro.MustParse(`{"type":"record","name":"Good","fields":[]}`))
		require.NoError(t, g.Write(&output))
		require.NotContains(t, output.String(), "State")
	}
}

func TestGeneratorRepeatedGraph(t *testing.T) {
	for _, full := range []bool{false, true} {
		g := gen.NewGenerator("generated", nil, gen.WithEncoders(true), gen.WithFullName(true), gen.WithFullSchema(full))
		schema := avro.MustParse(`{"type":"record","name":"tree.Node","fields":[{"name":"value","type":"int"},{"name":"children","type":{"type":"array","items":"tree.Node"}}]}`)
		g.Parse(schema)
		g.Parse(avro.MustParse(`{"type":"record","name":"tree.Node","fields":[{"name":"value","type":"int"},{"name":"children","type":{"type":"array","items":"tree.Node"}}]}`))
		field, err := avro.NewField("node", avro.NewRefSchema(schema.(*avro.RecordSchema)))
		require.NoError(t, err)
		root, err := avro.NewRecordSchema("Root", "tree", []*avro.Field{field})
		require.NoError(t, err)
		g.Parse(root)
		var output bytes.Buffer
		require.NoError(t, g.Write(&output))
		require.Equal(t, 1, strings.Count(output.String(), "type TreeNode struct"))
		testGeneratedPackage(t, output.Bytes(), `package generated
import "testing"
func TestTree(t *testing.T) {
 value := TreeRoot{Node: TreeNode{Value: 1, Children: []TreeNode{{Value: 2}}}}
 data, err := value.Marshal()
 if err != nil { t.Fatal(err) }
 var got TreeRoot
 if err := got.Unmarshal(data); err != nil { t.Fatal(err) }
 if got.Node.Value != 1 || len(got.Node.Children) != 1 || got.Node.Children[0].Value != 2 { t.Fatal(got) }
}`)
	}
}

func TestGeneratorNamingOptions(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"Options","fields":[{"name":"schema","type":"int"},{"name":"cid","type":"int"},{"name":"kind","type":{"type":"enum","name":"Kind","symbols":["CID"]}}]}`)
	g := gen.NewGenerator("generated", nil, gen.WithInitialisms([]string{"CID"}), gen.WithEnums(true))
	g.Parse(schema)
	var output bytes.Buffer
	require.NoError(t, g.Write(&output))
	testGeneratedPackage(t, output.Bytes(), `package generated
import "testing"
func TestOptions(t *testing.T) {
 value := Options{Schema: 1, CID: 2, Kind: KindCID}
 if value.Schema != 1 || value.CID != 2 || value.Kind != "CID" { t.Fatal(value) }
}`)
}

func TestGeneratorRejectsValueRecursion(t *testing.T) {
	for _, text := range []string{
		`{"type":"record","name":"ValueNode","fields":[{"name":"next","type":"ValueNode"}]}`,
		`{"type":"record","name":"ValueA","fields":[{"name":"b","type":{"type":"record","name":"ValueB","fields":[{"name":"a","type":"ValueA"}]}}]}`,
	} {
		g := gen.NewGenerator("generated", nil)
		g.Parse(avro.MustParse(text))
		var output bytes.Buffer
		require.ErrorContains(t, g.Write(&output), "recursive Go value")
		require.Empty(t, output.String())
	}
}

func TestGeneratorEmptyRecordSchema(t *testing.T) {
	schema, err := avro.NewRecordSchema("Empty", "", nil)
	require.NoError(t, err)
	g := gen.NewGenerator("generated", nil, gen.WithEncoders(true), gen.WithFullSchema(true))
	g.Parse(schema)
	var output bytes.Buffer
	require.NoError(t, g.Write(&output))
	testGeneratedPackage(t, output.Bytes(), `package generated
import "testing"
func TestEmpty(t *testing.T) {
 var value Empty
 data, err := value.Marshal()
 if err != nil || len(data) != 0 { t.Fatal(data, err) }
}`)
}

// Compile and execute generated code in a fresh process and scratch module.
// This prevents an already populated Avro schema cache from masking init bugs.
func testGeneratedPackage(t *testing.T, source []byte, test string) {
	t.Helper()
	root, err := filepath.Abs("../../../../tmp")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(root, 0o700))
	dir, err := os.MkdirTemp(root, "avro-generator-")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, os.RemoveAll(dir)) })
	module, err := filepath.Abs("..")
	require.NoError(t, err)
	mod, err := os.ReadFile(filepath.Join(module, "go.mod"))
	require.NoError(t, err)
	mod = bytes.Replace(mod, []byte("module github.com/awaken/avro/v2"), []byte("module generated"), 1)
	mod = append(mod, []byte("\nrequire github.com/awaken/avro/v2 v2.0.0\nreplace github.com/awaken/avro/v2 => "+strconv.Quote(module)+"\n")...)
	sum, err := os.ReadFile(filepath.Join(module, "go.sum"))
	require.NoError(t, err)
	for name, content := range map[string][]byte{"go.mod": mod, "go.sum": sum, "generated.go": source, "generated_test.go": []byte(test)} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), content, 0o600))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-p=1", "-count=1", "-timeout=20s", ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}

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

	source, _ := generate(t, schema, gc)
	testGeneratedPackage(t, source, `package something
import (
 "testing"
 "github.com/awaken/avro/v2"
)
func TestSchemaDoc(t *testing.T) {
 var value Test
 if got := value.Schema().(*avro.RecordSchema).Doc(); got != `+strconv.Quote("Test record doc with `backticks`")+` { t.Fatal(got) }
}`)
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
