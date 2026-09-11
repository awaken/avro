package avro_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_InvalidType(t *testing.T) {
	schemas := []string{
		`123`,
		`{"type": 123}`,
		`{"type":"record","name":"R","fields":[{"name":"v","type":null}]}`,
		`{"type":"array","items":null}`,
		`[null]`,
	}

	for _, schm := range schemas {
		_, err := avro.Parse(schm)

		assert.Error(t, err)
	}
}

func TestNewField_AliasCollisions(t *testing.T) {
	for _, aliases := range [][]string{{"old", "old"}, {"value"}} {
		_, err := avro.NewField("value", avro.NewPrimitiveSchema(avro.Int, nil), avro.WithAliases(aliases))
		require.Error(t, err, "aliases %v", aliases)
	}

	field, err := avro.NewField("value", avro.NewPrimitiveSchema(avro.Int, nil), avro.WithAliases([]string{"old-name", ""}))
	require.NoError(t, err)
	require.Equal(t, []string{"old-name", ""}, field.Aliases())
}

func TestNewRecordSchema_AliasNamespace(t *testing.T) {
	for _, aliases := range [][]string{{"second"}, {"shared"}} {
		first, err := avro.NewField("first", avro.NewPrimitiveSchema(avro.Int, nil), avro.WithAliases(aliases))
		require.NoError(t, err)
		second, err := avro.NewField("second", avro.NewPrimitiveSchema(avro.Int, nil), avro.WithAliases([]string{"shared"}))
		require.NoError(t, err)
		for _, fields := range [][]*avro.Field{{first, second}, {second, first}} {
			_, err = avro.NewRecordSchema("R", "", fields)
			require.Error(t, err, "aliases %v", aliases)
			_, err = avro.NewErrorRecordSchema("R", "", fields)
			require.Error(t, err, "error record aliases %v", aliases)
		}
	}
}

func TestNewField_RejectsCyclicDefault(t *testing.T) {
	const helper = "AVRO_CYCLIC_DEFAULT_HELPER"
	if kind := os.Getenv(helper); kind != "" {
		debug.SetMaxStack(256 << 10)

		def := map[string]any{}
		switch kind {
		case "map":
			def["extra"] = def
		case "slice":
			value := make([]any, 1)
			value[0] = value
			def["extra"] = value
		default:
			panic("unknown cyclic-default helper case")
		}

		record, err := avro.NewRecordSchema("R", "", nil)
		if err != nil {
			panic(err)
		}
		if _, err = avro.NewField("value", record, avro.WithDefault(def)); err == nil {
			panic("cyclic default was accepted")
		}
		return
	}

	for _, kind := range []string{"map", "slice"} {
		t.Run(kind, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestNewField_RejectsCyclicDefault$")
			cmd.Env = append(os.Environ(), helper+"="+kind)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, string(output))
		})
	}

	shared := map[string]any{"value": float64(1)}
	def := map[string]any{"left": shared, "right": shared}
	mapSchema := avro.NewMapSchema(avro.NewPrimitiveSchema(avro.Double, nil))
	recordField, err := avro.NewField("left", mapSchema)
	require.NoError(t, err)
	recordField2, err := avro.NewField("right", mapSchema)
	require.NoError(t, err)
	record, err := avro.NewRecordSchema("Shared", "", []*avro.Field{recordField, recordField2})
	require.NoError(t, err)

	_, err = avro.NewField("value", record, avro.WithDefault(def))
	require.NoError(t, err)
}

func TestParse_MalformedJSONPreservesSyntaxError(t *testing.T) {
	for _, schema := range []string{
		`{"type":"record"`,
		`["null","int"`,
		`"unterminated`,
	} {
		_, err := avro.Parse(schema)
		require.ErrorContains(t, err, "invalid schema JSON")
	}
}

func TestParse_BareNamedReference(t *testing.T) {
	cache := &avro.SchemaCache{}
	_, err := avro.ParseWithCache(`{"type":"record","name":"Referenced","namespace":"example","fields":[]}`, "", cache)
	require.NoError(t, err)

	schema, err := avro.ParseWithCache("example.Referenced", "", cache)
	require.NoError(t, err)
	require.Equal(t, "example.Referenced", schema.(avro.NamedSchema).FullName())
}

func TestParseWithCache_NilCache(t *testing.T) {
	const name = "NilCacheRecord"
	require.Nil(t, avro.DefaultSchemaCache.Get(name))

	schema, err := avro.ParseWithCache(`{"type":"record","name":"`+name+`","fields":[]}`, "", nil)

	require.NoError(t, err)
	assert.Equal(t, name, schema.(avro.NamedSchema).FullName())
	assert.Nil(t, avro.DefaultSchemaCache.Get(name))
}

func TestSchemaCache_AddIgnoresNil(t *testing.T) {
	tests := []struct {
		name   string
		schema avro.Schema
	}{
		{name: "nil"},
		{name: "typed nil", schema: (*avro.PrimitiveSchema)(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := &avro.SchemaCache{}
			assert.NotPanics(t, func() {
				cache.Add("test", tt.schema)
			})
			assert.Nil(t, cache.Get("test"))
		})
	}
}

func TestParse_AttributeNamesAreCaseSensitive(t *testing.T) {
	schemas := []string{
		`{"type":"record","Name":"R","Fields":[]}`,
		`{"type":"enum","name":"E","Symbols":["A"]}`,
		`{"type":"array","Items":"int"}`,
		`{"type":"map","Values":"int"}`,
		`{"type":"fixed","name":"F","Size":3}`,
	}

	for _, schema := range schemas {
		_, err := avro.ParseWithCache(schema, "", &avro.SchemaCache{})
		assert.Error(t, err)
	}
}

func TestMustParse(t *testing.T) {
	s := avro.MustParse("null")

	assert.Equal(t, avro.Null, s.Type())
}

func TestMustParse_PanicsOnError(t *testing.T) {
	assert.Panics(t, func() {
		avro.MustParse("123")
	})
}

func TestParseFiles(t *testing.T) {
	s, err := avro.ParseFiles("testdata/schema.avsc")

	require.NoError(t, err)
	assert.Equal(t, avro.String, s.Type())
}

func TestParseFiles_FileDoesntExist(t *testing.T) {
	_, err := avro.ParseFiles("test.something")

	assert.Error(t, err)
}

func TestParseFiles_InvalidSchema(t *testing.T) {
	_, err := avro.ParseFiles("testdata/bad-schema.avsc")

	assert.Error(t, err)
}

func TestParseFiles_IsolatedFromDefaultCache(t *testing.T) {
	const name = "ParseFilesExternalIsolation"
	external, err := avro.ParseWithCache(`{"type":"record","name":"`+name+`","fields":[]}`, "", &avro.SchemaCache{})
	require.NoError(t, err)
	avro.DefaultSchemaCache.Add(name, external)

	dir := avroTestTempDir(t)
	path := filepath.Join(dir, "external.avsc")
	require.NoError(t, os.WriteFile(path, []byte(`"`+name+`"`), 0o600))

	_, err = avro.ParseFiles(path)
	require.ErrorContains(t, err, "unknown type")
}

func TestParseFiles_ConcurrentInvocationsOwnTheirCaches(t *testing.T) {
	dir := avroTestTempDir(t)
	type files struct {
		name       string
		definition string
		reference  string
	}
	tests := []files{
		{name: "ConcurrentOne", definition: filepath.Join(dir, "one-definition.avsc"), reference: filepath.Join(dir, "one-reference.avsc")},
		{name: "ConcurrentTwo", definition: filepath.Join(dir, "two-definition.avsc"), reference: filepath.Join(dir, "two-reference.avsc")},
	}
	for _, test := range tests {
		require.NoError(t, os.WriteFile(test.definition, []byte(`{"type":"record","name":"`+test.name+`","fields":[]}`), 0o600))
		require.NoError(t, os.WriteFile(test.reference, []byte(`"`+test.name+`"`), 0o600))
	}

	errs := make(chan error, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		test := tests[i%len(tests)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			schema, err := avro.ParseFiles(test.definition, test.reference)
			if err != nil {
				errs <- err
				return
			}
			if got := schema.(avro.NamedSchema).FullName(); got != test.name {
				errs <- fmt.Errorf("got schema %s, want %s", got, test.name)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err)
	}
}

func TestNullSchema(t *testing.T) {
	schemas := []string{
		`null`,
		`{"type":"null"}`,
		`{"type":"null", "other-property": 123, "another-property": ["a","b","c"]}`,
	}

	for _, schm := range schemas {
		schema, err := avro.Parse(schm)

		require.NoError(t, err)
		assert.Equal(t, avro.Null, schema.Type())
		want := [32]byte{0xf0, 0x72, 0xcb, 0xec, 0x3b, 0xf8, 0x84, 0x18, 0x71, 0xd4, 0x28, 0x42, 0x30, 0xc5, 0xe9, 0x83, 0xdc, 0x21, 0x1a, 0x56, 0x83, 0x7a, 0xed, 0x86, 0x24, 0x87, 0x14, 0x8f, 0x94, 0x7d, 0x1a, 0x1f}
		assert.Equal(t, want, schema.Fingerprint())
	}
}

func TestPrimitiveSchema(t *testing.T) {
	tests := []struct {
		schema          string
		want            avro.Type
		wantFingerprint [32]byte
	}{
		{
			schema:          "string",
			want:            avro.String,
			wantFingerprint: [32]byte{0xe9, 0xe5, 0xc1, 0xc9, 0xe4, 0xf6, 0x27, 0x73, 0x39, 0xd1, 0xbc, 0xde, 0x7, 0x33, 0xa5, 0x9b, 0xd4, 0x2f, 0x87, 0x31, 0xf4, 0x49, 0xda, 0x6d, 0xc1, 0x30, 0x10, 0xa9, 0x16, 0x93, 0xd, 0x48},
		},
		{
			schema:          `{"type":"string"}`,
			want:            avro.String,
			wantFingerprint: [32]byte{0xe9, 0xe5, 0xc1, 0xc9, 0xe4, 0xf6, 0x27, 0x73, 0x39, 0xd1, 0xbc, 0xde, 0x7, 0x33, 0xa5, 0x9b, 0xd4, 0x2f, 0x87, 0x31, 0xf4, 0x49, 0xda, 0x6d, 0xc1, 0x30, 0x10, 0xa9, 0x16, 0x93, 0xd, 0x48},
		},
		{
			schema:          "bytes",
			want:            avro.Bytes,
			wantFingerprint: [32]byte{0x9a, 0xe5, 0x7, 0xa9, 0xdd, 0x39, 0xee, 0x5b, 0x7c, 0x7e, 0x28, 0x5d, 0xa2, 0xc0, 0x84, 0x65, 0x21, 0xc8, 0xae, 0x8d, 0x80, 0xfe, 0xea, 0xe5, 0x50, 0x4e, 0xc, 0x98, 0x1d, 0x53, 0xf5, 0xfa},
		},
		{
			schema:          `{"type":"bytes"}`,
			want:            avro.Bytes,
			wantFingerprint: [32]byte{0x9a, 0xe5, 0x7, 0xa9, 0xdd, 0x39, 0xee, 0x5b, 0x7c, 0x7e, 0x28, 0x5d, 0xa2, 0xc0, 0x84, 0x65, 0x21, 0xc8, 0xae, 0x8d, 0x80, 0xfe, 0xea, 0xe5, 0x50, 0x4e, 0xc, 0x98, 0x1d, 0x53, 0xf5, 0xfa},
		},
		{
			schema:          "int",
			want:            avro.Int,
			wantFingerprint: [32]byte{0x3f, 0x2b, 0x87, 0xa9, 0xfe, 0x7c, 0xc9, 0xb1, 0x38, 0x35, 0x59, 0x8c, 0x39, 0x81, 0xcd, 0x45, 0xe3, 0xe3, 0x55, 0x30, 0x9e, 0x50, 0x90, 0xaa, 0x9, 0x33, 0xd7, 0xbe, 0xcb, 0x6f, 0xba, 0x45},
		},
		{
			schema:          `{"type":"int"}`,
			want:            avro.Int,
			wantFingerprint: [32]byte{0x3f, 0x2b, 0x87, 0xa9, 0xfe, 0x7c, 0xc9, 0xb1, 0x38, 0x35, 0x59, 0x8c, 0x39, 0x81, 0xcd, 0x45, 0xe3, 0xe3, 0x55, 0x30, 0x9e, 0x50, 0x90, 0xaa, 0x9, 0x33, 0xd7, 0xbe, 0xcb, 0x6f, 0xba, 0x45},
		},
		{
			schema:          "long",
			want:            avro.Long,
			wantFingerprint: [32]byte{0xc3, 0x2c, 0x49, 0x7d, 0xf6, 0x73, 0xc, 0x97, 0xfa, 0x7, 0x36, 0x2a, 0xa5, 0x2, 0x3f, 0x37, 0xd4, 0x9a, 0x2, 0x7e, 0xc4, 0x52, 0x36, 0x7, 0x78, 0x11, 0x4c, 0xf4, 0x27, 0x96, 0x5a, 0xdd},
		},
		{
			schema:          `{"type":"long"}`,
			want:            avro.Long,
			wantFingerprint: [32]byte{0xc3, 0x2c, 0x49, 0x7d, 0xf6, 0x73, 0xc, 0x97, 0xfa, 0x7, 0x36, 0x2a, 0xa5, 0x2, 0x3f, 0x37, 0xd4, 0x9a, 0x2, 0x7e, 0xc4, 0x52, 0x36, 0x7, 0x78, 0x11, 0x4c, 0xf4, 0x27, 0x96, 0x5a, 0xdd},
		},
		{
			schema:          "float",
			want:            avro.Float,
			wantFingerprint: [32]byte{0x1e, 0x71, 0xf9, 0xec, 0x5, 0x1d, 0x66, 0x3f, 0x56, 0xb0, 0xd8, 0xe1, 0xfc, 0x84, 0xd7, 0x1a, 0xa5, 0x6c, 0xcf, 0xe9, 0xfa, 0x93, 0xaa, 0x20, 0xd1, 0x5, 0x47, 0xa7, 0xab, 0xeb, 0x5c, 0xc0},
		},
		{
			schema:          `{"type":"float"}`,
			want:            avro.Float,
			wantFingerprint: [32]byte{0x1e, 0x71, 0xf9, 0xec, 0x5, 0x1d, 0x66, 0x3f, 0x56, 0xb0, 0xd8, 0xe1, 0xfc, 0x84, 0xd7, 0x1a, 0xa5, 0x6c, 0xcf, 0xe9, 0xfa, 0x93, 0xaa, 0x20, 0xd1, 0x5, 0x47, 0xa7, 0xab, 0xeb, 0x5c, 0xc0},
		},
		{
			schema:          "double",
			want:            avro.Double,
			wantFingerprint: [32]byte{0x73, 0xa, 0x9a, 0x8c, 0x61, 0x16, 0x81, 0xd7, 0xee, 0xf4, 0x42, 0xe0, 0x3c, 0x16, 0xc7, 0xd, 0x13, 0xbc, 0xa3, 0xeb, 0x8b, 0x97, 0x7b, 0xb4, 0x3, 0xea, 0xff, 0x52, 0x17, 0x6a, 0xf2, 0x54},
		},
		{
			schema:          `{"type":"double"}`,
			want:            avro.Double,
			wantFingerprint: [32]byte{0x73, 0xa, 0x9a, 0x8c, 0x61, 0x16, 0x81, 0xd7, 0xee, 0xf4, 0x42, 0xe0, 0x3c, 0x16, 0xc7, 0xd, 0x13, 0xbc, 0xa3, 0xeb, 0x8b, 0x97, 0x7b, 0xb4, 0x3, 0xea, 0xff, 0x52, 0x17, 0x6a, 0xf2, 0x54},
		},
		{
			schema:          "boolean",
			want:            avro.Boolean,
			wantFingerprint: [32]byte{0xa5, 0xb0, 0x31, 0xab, 0x62, 0xbc, 0x41, 0x6d, 0x72, 0xc, 0x4, 0x10, 0xd8, 0x2, 0xea, 0x46, 0xb9, 0x10, 0xc4, 0xfb, 0xe8, 0x5c, 0x50, 0xa9, 0x46, 0xcc, 0xc6, 0x58, 0xb7, 0x4e, 0x67, 0x7e},
		},
		{
			schema:          `{"type":"boolean"}`,
			want:            avro.Boolean,
			wantFingerprint: [32]byte{0xa5, 0xb0, 0x31, 0xab, 0x62, 0xbc, 0x41, 0x6d, 0x72, 0xc, 0x4, 0x10, 0xd8, 0x2, 0xea, 0x46, 0xb9, 0x10, 0xc4, 0xfb, 0xe8, 0x5c, 0x50, 0xa9, 0x46, 0xcc, 0xc6, 0x58, 0xb7, 0x4e, 0x67, 0x7e},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.schema, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			require.NoError(t, err)
			assert.Equal(t, test.want, schema.Type())
			assert.Equal(t, test.wantFingerprint, schema.Fingerprint())
		})
	}
}

func TestPrimitiveSchema_HandlesProps(t *testing.T) {
	schm := `
{
   "type": "string",
   "foo": "bar",
   "baz": 1
}
`

	s, err := avro.Parse(schm)

	assert.NoError(t, err)
	assert.Equal(t, avro.String, s.Type())
	assert.Equal(t, "bar", s.(*avro.PrimitiveSchema).Prop("foo"))
	assert.Equal(t, float64(1), s.(*avro.PrimitiveSchema).Prop("baz"))
	assert.Equal(t, map[string]any{"foo": "bar", "baz": float64(1)}, s.(*avro.PrimitiveSchema).Props())
}

func TestRecordSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "Valid",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "doc": "docs", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.NoError,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"record", "name":"test", "namespace": "", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: require.NoError,
		},
		{
			name:    "Invalid Name First Char",
			schema:  `{"type":"record", "name":"0test", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Name Other Char",
			schema:  `{"type":"record", "name":"test+", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"record", "name":"", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Name",
			schema:  `{"type":"record", "namespace": "org.hamba.avro", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro+", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Fields",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro"}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":["test"]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Field Name",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Field Name",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "field+", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Historical Field Alias",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "field", "aliases": ["test+"], "type": "int"}]}`,
			wantErr: require.NoError,
		},
		{
			name:    "No Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "field"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "blah"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Duplicate Field Names",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "doc": "docs", "fields":[{"name": "field", "type": "int"}, {"name": "field", "type": "string"}]}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			test.wantErr(t, err)
			if schema != nil {
				assert.Equal(t, avro.Record, schema.Type())
			}
		})
	}
}

func TestErrorRecordSchema(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		wantSchema bool
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name:       "Valid",
			schema:     `{"type":"error", "name":"test", "namespace": "org.hamba.avro", "doc": "docs", "fields":[{"name": "field", "type": "int"}]}`,
			wantSchema: true,
			wantErr:    require.NoError,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"error", "name":"test", "namespace": "", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: require.NoError,
		},
		{
			name:    "Invalid Name First Char",
			schema:  `{"type":"error", "name":"0test", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Name Other Char",
			schema:  `{"type":"error", "name":"test+", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"error", "name":"", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Name",
			schema:  `{"type":"error", "namespace": "org.hamba.avro", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"error", "name":"test", "namespace": "org.hamba.avro+", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			test.wantErr(t, err)
			if test.wantSchema {
				assert.Equal(t, avro.Record, schema.Type())
				recSchema := schema.(*avro.RecordSchema)
				assert.True(t, recSchema.IsError())
			}
		})
	}
}

func TestRecordSchema_ValidatesDefault(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "String",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "string", "default": "test"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Int",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int", "default": 1}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Int Fractional",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int", "default": 1.5}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Int Out Of Range",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int", "default": 2147483648}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Long",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "long", "default": 1}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Long Above Binary64 Exact Range",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "long", "default": 9007199254740993}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Long Out Of Range",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "long", "default": 9223372036854775808}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Float",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "float", "default": 1}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Double",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "double", "default": 1}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Array",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"array", "items": "int"}, "default": [1,2]}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Array Not Array",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"array", "items": "int"}, "default": "test"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Array Invalid Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"array", "items": "int"}, "default": ["test"]}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Map",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"map", "values": "int"}, "default": {"b": 1}}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Map Not Map",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"map", "values": "int"}, "default": "test"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Map Invalid Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"map", "values": "int"}, "default": {"b": "test"}}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Union",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": ["string", "null"]}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Default",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": ["null", "string"], "default": null}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Default Matches Later Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": ["null", "string"], "default": "string"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Default Matches No Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": ["null", "string"], "default": true}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": "int", "default": 1}]}, "default": {"b": 1}}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Missing Bytes Field With Default",
			schema:  `{"type":"record","name":"test","fields":[{"name":"a","type":{"type":"record","name":"child","fields":[{"name":"b","type":"bytes","default":"\u00ff"}]},"default":{}}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Missing Fixed Field With Default",
			schema:  `{"type":"record","name":"test","fields":[{"name":"a","type":{"type":"record","name":"child","fields":[{"name":"b","type":{"type":"fixed","name":"fixed1","size":1},"default":"\u00ff"}]},"default":{}}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record With Nullable Field",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": ["null","int"]}]}, "default": {"b": 1, "c": null}}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Missing Nullable Field Without Default",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": ["null","int"]}]}, "default": {"b": 1}}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record Not Map",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": "int", "default": 1}]}, "default": "test"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record Invalid Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": "int", "default": 1}]}, "default": {"b": "test"}}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record Invalid Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": "int", "default": "test"}]}, "default": {"b": 1}}]}`,
			wantErr: assert.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			test.wantErr(t, err)
			if err != nil {
				return
			}

			// Ensure MarshalJSON Generate the same schema as it considers default values
			b, err := schema.(*avro.RecordSchema).MarshalJSON()
			assert.NoError(t, err)
			schema2, err := avro.ParseWithCache(string(b), "", &avro.SchemaCache{})
			assert.NoError(t, err)
			assert.Equal(t, schema, schema2)
		})
	}
}

func TestNewField_RejectsNonFiniteFloatDefaults(t *testing.T) {
	tests := []struct {
		name   string
		schema avro.Schema
		value  any
	}{
		{name: "float32 NaN", schema: avro.NewPrimitiveSchema(avro.Float, nil), value: float32(math.NaN())},
		{name: "float32 positive infinity", schema: avro.NewPrimitiveSchema(avro.Float, nil), value: float32(math.Inf(1))},
		{name: "float64 NaN as float", schema: avro.NewPrimitiveSchema(avro.Float, nil), value: math.NaN()},
		{name: "JSON number infinity as float", schema: avro.NewPrimitiveSchema(avro.Float, nil), value: json.Number("+Inf")},
		{name: "float64 NaN as double", schema: avro.NewPrimitiveSchema(avro.Double, nil), value: math.NaN()},
		{name: "float64 negative infinity", schema: avro.NewPrimitiveSchema(avro.Double, nil), value: math.Inf(-1)},
		{name: "JSON number NaN as double", schema: avro.NewPrimitiveSchema(avro.Double, nil), value: json.Number("NaN")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := avro.NewField("value", tt.schema, avro.WithDefault(tt.value))
			require.Error(t, err)
		})
	}

	_, err := avro.NewField("value", avro.NewPrimitiveSchema(avro.Float, nil), avro.WithDefault(float32(math.MaxFloat32)))
	require.NoError(t, err)
	_, err = avro.NewField("value", avro.NewPrimitiveSchema(avro.Double, nil), avro.WithDefault(math.MaxFloat64))
	require.NoError(t, err)
}

func TestParse_RejectsRecursiveRecordDefaultWithoutPanic(t *testing.T) {
	const input = `{
		"type":"record",
		"name":"RecursiveDefault",
		"fields":[{"name":"self","type":"RecursiveDefault","default":{}}]
	}`

	var err error
	assert.NotPanics(t, func() {
		_, err = avro.ParseWithCache(input, "", &avro.SchemaCache{})
	})
	assert.Error(t, err)
}

func TestRecordSchema_PreservesLargeLongDefault(t *testing.T) {
	schema, err := avro.ParseWithCache(`{"type":"record","name":"R","fields":[{"name":"v","type":"long","default":9007199254740993}]}`, "", &avro.SchemaCache{})
	require.NoError(t, err)

	assert.Equal(t, int64(9007199254740993), schema.(*avro.RecordSchema).Fields()[0].Default())
}

func TestRecordSchema_UnionDefaultUsesFirstMatchingBranch(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    any
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "later string",
			schema:  `{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"],"default":"value"}]}`,
			want:    "value",
			wantErr: require.NoError,
		},
		{
			name:    "first ambiguous numeric branch",
			schema:  `{"type":"record","name":"R","fields":[{"name":"v","type":["long","int"],"default":1}]}`,
			want:    int64(1),
			wantErr: require.NoError,
		},
		{
			name:    "later named branch",
			schema:  `{"type":"record","name":"R","fields":[{"name":"v","type":["null",{"type":"record","name":"Nested","fields":[{"name":"n","type":"int"}]}],"default":{"n":1}}]}`,
			want:    map[string]any{"n": 1},
			wantErr: require.NoError,
		},
		{
			name:    "no matching branch",
			schema:  `{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"],"default":true}]}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})
			test.wantErr(t, err)
			if err != nil {
				return
			}
			require.Equal(t, test.want, schema.(*avro.RecordSchema).Fields()[0].Default())
		})
	}
}

func TestRecordSchema_ValidatesOrder(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    avro.Order
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "empty",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string"}]}`,
			want:    avro.Asc,
			wantErr: assert.NoError,
		},
		{
			name:    "asc",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string", "order": "ascending"}]}`,
			want:    avro.Asc,
			wantErr: assert.NoError,
		},
		{
			name:    "desc",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string", "order": "descending"}]}`,
			want:    avro.Desc,
			wantErr: assert.NoError,
		},
		{
			name:    "ignore",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string", "order": "ignore"}]}`,
			want:    avro.Ignore,
			wantErr: assert.NoError,
		},
		{
			name:    "invalid",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string", "order": "blah"}]}`,
			wantErr: assert.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			test.wantErr(t, err)
			if test.want != "" {
				rs := schema.(*avro.RecordSchema)
				require.Len(t, rs.Fields(), 1)
				assert.Equal(t, test.want, rs.Fields()[0].Order())
			}
		})
	}
}

func TestRecordSchema_HandlesProps(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "valid_name",
   "namespace": "org.hamba.avro",
   "doc": "foo",
   "foo": "bar1",
   "bar": "foo1",
   "fields": [
       {"name": "intField", "doc": "bar", "type": "int", "foo": "bar2"}
   ]
}
`

	s, err := avro.Parse(schm)
	require.NoError(t, err)

	rs := s.(*avro.RecordSchema)
	assert.Equal(t, avro.Record, s.Type())
	assert.Equal(t, "foo", rs.Doc())
	assert.Equal(t, "bar1", rs.Prop("foo"))
	assert.Equal(t, map[string]any{"foo": "bar1", "bar": "foo1"}, rs.Props())
	require.Len(t, rs.Fields(), 1)
	assert.Equal(t, "bar", rs.Fields()[0].Doc())
	assert.Equal(t, "bar2", rs.Fields()[0].Prop("foo"))
	assert.Equal(t, map[string]any{"foo": "bar2"}, rs.Fields()[0].Props())
}

func TestRecordSchema_WithReference(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "valid_name",
   "namespace": "org.hamba.avro",
   "fields": [
       {"name": "intField", "type": "int"},
       {"name": "Ref", "type": "valid_name"}
   ]
}
`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Record, s.Type())
	assert.Equal(t, avro.Ref, s.(*avro.RecordSchema).Fields()[1].Type().Type())
	assert.Equal(t, s.Fingerprint(), s.(*avro.RecordSchema).Fields()[1].Type().Fingerprint())
}

func TestRecordSchema_WithReferenceFullName(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "org.hamba.avro.ValidName",
    "fields": [
        {
            "name": "recordType1",
            "type": {
                "name": "refIntType",
                "type": "record",
                "fields": [
                    {"name": "intField", "type": "int"}
                ]
            }
        },
        {
            "name": "recordType2",
            "type": "org.hamba.avro.refIntType"
        }
    ]
}
`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Record, s.Type())
	assert.Equal(t, avro.Ref, s.(*avro.RecordSchema).Fields()[1].Type().Type())
	assert.Equal(t, s.(*avro.RecordSchema).Fields()[0].Type().Fingerprint(), s.(*avro.RecordSchema).Fields()[1].Type().Fingerprint())
}

func TestRecordSchema_WithAliasReference(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "valid_name",
   "namespace": "org.hamba.avro",
   "aliases": ["valid_alias"],
   "fields": [
       {"name": "intField", "type": "int"},
       {"name": "ref", "type": "valid_alias"}
   ]
}
`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Record, s.Type())
	assert.Equal(t, avro.Ref, s.(*avro.RecordSchema).Fields()[1].Type().Type())
	assert.Equal(t, s.Fingerprint(), s.(*avro.RecordSchema).Fields()[1].Type().Fingerprint())
}

func TestRecordSchema_WithHistoricalAliasReference(t *testing.T) {
	schema := `{"type":"record","name":"Current","namespace":"example","aliases":["old-name"],"fields":[{"name":"previous","type":"old-name"}]}`

	parsed, err := avro.ParseWithCache(schema, "", &avro.SchemaCache{})
	require.NoError(t, err)
	require.Equal(t, []string{"example.old-name"}, parsed.(avro.NamedSchema).Aliases())
	require.Equal(t, avro.Ref, parsed.(*avro.RecordSchema).Fields()[0].Type().Type())
}

func TestEnumSchema(t *testing.T) {
	tests := []struct {
		name        string
		schema      string
		wantName    string
		wantDefault string
		wantErr     require.ErrorAssertionFunc
	}{
		{
			name:     "Valid",
			schema:   `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			wantName: "org.hamba.avro.test",
			wantErr:  require.NoError,
		},
		{
			name:        "Valid With Default",
			schema:      `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"], "default": "TEST"}`,
			wantName:    "org.hamba.avro.test",
			wantDefault: "TEST",
			wantErr:     require.NoError,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"enum", "name":"test", "namespace": "", "symbols":["TEST"]}`,
			wantErr: require.NoError,
		},
		{
			name:    "Invalid Name",
			schema:  `{"type":"enum", "name":"test+", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"enum", "name":"", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Name",
			schema:  `{"type":"enum", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro+", "symbols":["TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Symbols",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro"}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Symbols",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":[]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Symbol",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST+"]}`,
			wantErr: require.Error,
		},
		{
			name:    "Duplicate Symbols",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST", "TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Symbol Type",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":[1]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Default",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"], "default": "foo"}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Default",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"], "default": ""}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Default Type",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"], "default": 1}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			test.wantErr(t, err)
			if test.wantName != "" {
				assert.Equal(t, avro.Enum, schema.Type())
				named := schema.(*avro.EnumSchema)
				assert.Equal(t, test.wantName, named.FullName())
				assert.Equal(t, test.wantDefault, named.Default())
			}
		})
	}
}

func TestEnumSchema_HandlesProps(t *testing.T) {
	schm := `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "doc": "hello", "symbols":["TEST"], "foo":"bar"}`

	s, err := avro.Parse(schm)
	require.NoError(t, err)

	es := s.(*avro.EnumSchema)
	assert.Equal(t, avro.Enum, s.Type())
	assert.Equal(t, "hello", es.Doc())
	assert.Equal(t, "bar", es.Prop("foo"))
	assert.Equal(t, map[string]any{"foo": "bar"}, es.Props())
}

func TestArraySchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    avro.Schema
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "Valid",
			schema:  `{"type":"array", "items": "int"}`,
			want:    avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Int, nil)),
			wantErr: require.NoError,
		},
		{
			name:    "No Items",
			schema:  `{"type":"array"}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Items Type",
			schema:  `{"type":"array", "items": "blah"}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			test.wantErr(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestArraySchema_HandlesProps(t *testing.T) {
	schm := `{"type":"array", "items": "int", "foo":"bar"}`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Array, s.Type())
	assert.Equal(t, "bar", s.(*avro.ArraySchema).Prop("foo"))
	assert.Equal(t, map[string]any{"foo": "bar"}, s.(*avro.ArraySchema).Props())
}

func TestMapSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    avro.Schema
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "Valid",
			schema:  `{"type":"map", "values": "int"}`,
			want:    avro.NewMapSchema(avro.NewPrimitiveSchema(avro.Int, nil)),
			wantErr: require.NoError,
		},
		{
			name:    "No Values",
			schema:  `{"type":"map"}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Values Type",
			schema:  `{"type":"map", "values": "blah"}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			test.wantErr(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestMapSchema_HandlesProps(t *testing.T) {
	schm := `{"type":"map", "values": "int", "foo":"bar"}`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Map, s.Type())
	assert.Equal(t, "bar", s.(*avro.MapSchema).Prop("foo"))
	assert.Equal(t, map[string]any{"foo": "bar"}, s.(*avro.MapSchema).Props())
}

func TestUnionSchema(t *testing.T) {
	tests := []struct {
		name            string
		schema          string
		wantFingerprint [32]byte
		wantErr         require.ErrorAssertionFunc
	}{
		{
			name:            "Valid Simple",
			schema:          `["null", "int"]`,
			wantFingerprint: [32]byte{0xb4, 0x94, 0x95, 0xc5, 0xb1, 0xc2, 0x6f, 0x4, 0x89, 0x6a, 0x5f, 0x68, 0x65, 0xf, 0xe2, 0xb7, 0x64, 0x23, 0x62, 0xc3, 0x41, 0x98, 0xd6, 0xbc, 0x74, 0x65, 0xa1, 0xd9, 0xf7, 0xe1, 0xaf, 0xce},
			wantErr:         require.NoError,
		},
		{
			name:    "No Object Wrapped Union",
			schema:  `{"type":["null", "int"]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Object Wrapped Union With Discarded Property",
			schema:  `{"type":["null", "int"],"custom":"value"}`,
			wantErr: require.Error,
		},
		{
			name:    "No Nested Union Type",
			schema:  `["null", ["string"]]`,
			wantErr: require.Error,
		},
		{
			name:    "No Duplicate Types",
			schema:  `["string", "string"]`,
			wantErr: require.Error,
		},
		{
			name:    "No Duplicate Names",
			schema:  `[{"type":"enum", "name":"test", "symbols":["TEST"]}, {"type":"enum", "name":"test", "symbols":["TEST"]}]`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Type",
			schema:  `["null", "blah"]`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Union",
			schema:  `[]`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Union In Record Field",
			schema:  `{"type":"record","name":"R","fields":[{"name":"f","type":[]}]}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			test.wantErr(t, err)
			if test.wantFingerprint != [32]byte{} {
				assert.Equal(t, avro.Union, schema.Type())
				assert.Equal(t, test.wantFingerprint, schema.Fingerprint())
			}
		})
	}
}

func TestUnionSchema_RejectsDuplicateUnderlyingLogicalTypes(t *testing.T) {
	tests := []string{
		`["string",{"type":"string","logicalType":"uuid"}]`,
		`["int",{"type":"int","logicalType":"date"}]`,
		`["int",{"type":"int","logicalType":"time-millis"}]`,
		`["long",{"type":"long","logicalType":"time-micros"}]`,
		`["long",{"type":"long","logicalType":"timestamp-millis"}]`,
		`["long",{"type":"long","logicalType":"timestamp-micros"}]`,
		`["long",{"type":"long","logicalType":"local-timestamp-millis"}]`,
		`["long",{"type":"long","logicalType":"local-timestamp-micros"}]`,
		`["bytes",{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}]`,
	}

	for _, schema := range tests {
		_, err := avro.ParseWithCache(schema, "", &avro.SchemaCache{})
		require.ErrorContains(t, err, "union type must be unique")
	}
}

func TestUnionSchema_PreservesLogicalLookupKeys(t *testing.T) {
	schema, err := avro.ParseWithCache(`[{"type":"int","logicalType":"date"},"string"]`, "", &avro.SchemaCache{})
	require.NoError(t, err)

	logical, position := schema.(*avro.UnionSchema).Types().Get("int.date")
	require.Equal(t, 0, position)
	require.Equal(t, avro.Date, logical.(avro.LogicalTypeSchema).Logical().Type())
}

func TestUnionSchema_ContainsNamedAndLogicalTypes(t *testing.T) {
	schema := avro.MustParse(`[
		{"type":"record","name":"R","fields":[]},
		{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}
	]`).(*avro.UnionSchema)

	assert.True(t, schema.Contains(avro.Record))
	assert.True(t, schema.Contains(avro.Bytes))
	assert.False(t, schema.Contains(avro.Map))
}

func TestUnionSchema_Indices(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   [2]int
	}{
		{
			name:   "Null First",
			schema: `["null", "string"]`,
			want:   [2]int{0, 1},
		},
		{
			name:   "Null Second",
			schema: `["string", "null"]`,
			want:   [2]int{1, 0},
		},
		{
			name:   "Not Nullable",
			schema: `["null", "string", "int"]`,
			want:   [2]int{0, 0},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			require.NoError(t, err)
			null, typ := schema.(*avro.UnionSchema).Indices()
			assert.Equal(t, test.want[0], null)
			assert.Equal(t, test.want[1], typ)
		})
	}
}

func TestFixedSchema(t *testing.T) {
	tests := []struct {
		name            string
		schema          string
		wantName        string
		wantFingerprint [32]byte
		wantErr         require.ErrorAssertionFunc
	}{
		{
			name:            "Valid",
			schema:          `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			wantName:        "org.hamba.avro.test",
			wantFingerprint: [32]uint8{0x8c, 0x9e, 0xcb, 0x4, 0x83, 0x2f, 0x3b, 0xa7, 0x58, 0x85, 0x9, 0x99, 0x41, 0xe, 0xbf, 0xd4, 0x7, 0xc7, 0x87, 0x4f, 0x8a, 0x12, 0xf4, 0xd0, 0x7f, 0x45, 0xdd, 0xaa, 0x10, 0x6b, 0x2f, 0xb3},
			wantErr:         require.NoError,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"fixed", "name":"test", "namespace": "", "size": 12}`,
			wantErr: require.NoError,
		},
		{
			name:    "Invalid Name",
			schema:  `{"type":"fixed", "name":"test+", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"fixed", "name":"", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: require.Error,
		},
		{
			name:    "No Name",
			schema:  `{"type":"fixed", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro+", "size": 12}`,
			wantErr: require.Error,
		},
		{
			name:    "No Size",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro"}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Size Type",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": "test"}`,
			wantErr: require.Error,
		},
		{
			name:    "Fractional Size Value",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 1.5}`,
			wantErr: require.Error,
		},
		{
			name:    "Out Of Range Size Value",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 1e20}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Size Value",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": -1}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})

			test.wantErr(t, err)
			if test.wantFingerprint != [32]byte{} {
				assert.Equal(t, avro.Fixed, schema.Type())
				named := schema.(avro.NamedSchema)
				assert.Equal(t, test.wantName, named.FullName())
				assert.Equal(t, test.wantFingerprint, named.Fingerprint())
			}
		})
	}
}

func TestFixedSchema_HandlesProps(t *testing.T) {
	schm := `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12, "foo":"bar"}`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Fixed, s.Type())
	assert.Equal(t, "bar", s.(*avro.FixedSchema).Prop("foo"))
	assert.Equal(t, map[string]any{"foo": "bar"}, s.(*avro.FixedSchema).Props())
}

func TestNamedSchema_ExplicitEmptyNamespace(t *testing.T) {
	schema, err := avro.ParseWithCache(`{"type":"record","name":"Outer","namespace":"parent","fields":[{"name":"r","type":{"type":"record","name":"R","namespace":"","fields":[]}},{"name":"e","type":{"type":"enum","name":"E","namespace":"","symbols":["A"]}},{"name":"f","type":{"type":"fixed","name":"F","namespace":"","size":1}}]}`, "", &avro.SchemaCache{})
	require.NoError(t, err)

	fields := schema.(*avro.RecordSchema).Fields()
	assert.Equal(t, "R", fields[0].Type().(avro.NamedSchema).FullName())
	assert.Equal(t, "E", fields[1].Type().(avro.NamedSchema).FullName())
	assert.Equal(t, "F", fields[2].Type().(avro.NamedSchema).FullName())
}

func TestSchema_LogicalAnnotationsDoNotChangeStandardFingerprints(t *testing.T) {
	tests := []struct {
		name    string
		plain   string
		logical string
	}{
		{
			name:    "primitive",
			plain:   `"long"`,
			logical: `{"type":"long","logicalType":"timestamp-millis"}`,
		},
		{
			name:    "decimal bytes",
			plain:   `"bytes"`,
			logical: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			name:    "fixed",
			plain:   `{"type":"fixed","name":"F","size":12}`,
			logical: `{"type":"fixed","name":"F","size":12,"logicalType":"duration"}`,
		},
		{
			name:    "nested",
			plain:   `{"type":"record","name":"R","fields":[{"name":"v","type":"long"}]}`,
			logical: `{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plain, err := avro.ParseWithCache(test.plain, "", &avro.SchemaCache{})
			require.NoError(t, err)
			logical, err := avro.ParseWithCache(test.logical, "", &avro.SchemaCache{})
			require.NoError(t, err)

			require.Equal(t, plain.String(), logical.String())
			require.Equal(t, plain.Fingerprint(), logical.Fingerprint())
			plainCRC, err := plain.FingerprintUsing(avro.CRC64Avro)
			require.NoError(t, err)
			logicalCRC, err := logical.FingerprintUsing(avro.CRC64Avro)
			require.NoError(t, err)
			require.Equal(t, plainCRC, logicalCRC)
			require.NotEqual(t, plain.CacheFingerprint(), logical.CacheFingerprint())

			encoded, err := json.Marshal(logical)
			require.NoError(t, err)
			require.Contains(t, string(encoded), `"logicalType"`)
		})
	}
}

func TestSchema_LegacyLogicalFingerprintMigration(t *testing.T) {
	const oldCanonical = `{"name":"R","type":"record","fields":[{"name":"v","type":{"type":"long","logicalType":"timestamp-millis"}}]}`
	schema, err := avro.ParseWithCache(`{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"long","logicalType":"timestamp-millis"}}]}`, "", &avro.SchemaCache{})
	require.NoError(t, err)

	require.Equal(t, sha256.Sum256([]byte(oldCanonical)), avro.LegacyFingerprint(schema))
	legacyCRC, err := avro.LegacyFingerprintUsing(schema, avro.CRC64Avro)
	require.NoError(t, err)
	standardCRC, err := schema.FingerprintUsing(avro.CRC64Avro)
	require.NoError(t, err)
	require.NotEqual(t, legacyCRC, standardCRC)
}

func TestSchema_LogicalTypes(t *testing.T) {
	tests := []struct {
		name            string
		schema          string
		wantType        avro.Type
		wantLogical     bool
		wantLogicalType avro.LogicalType
		assertFn        func(t *testing.T, ls avro.LogicalSchema)
	}{
		{
			name:        "Invalid",
			schema:      `{"type": "int", "logicalType": "test"}`,
			wantType:    avro.Int,
			wantLogical: false,
		},
		{
			name:            "Date",
			schema:          `{"type": "int", "logicalType": "date"}`,
			wantType:        avro.Int,
			wantLogical:     true,
			wantLogicalType: avro.Date,
		},
		{
			name:            "Time Millis",
			schema:          `{"type": "int", "logicalType": "time-millis"}`,
			wantType:        avro.Int,
			wantLogical:     true,
			wantLogicalType: avro.TimeMillis,
		},
		{
			name:            "Time Micros",
			schema:          `{"type": "long", "logicalType": "time-micros"}`,
			wantType:        avro.Long,
			wantLogical:     true,
			wantLogicalType: avro.TimeMicros,
		},
		{
			name:            "Timestamp Millis",
			schema:          `{"type": "long", "logicalType": "timestamp-millis"}`,
			wantType:        avro.Long,
			wantLogical:     true,
			wantLogicalType: avro.TimestampMillis,
		},
		{
			name:            "Timestamp Micros",
			schema:          `{"type": "long", "logicalType": "timestamp-micros"}`,
			wantType:        avro.Long,
			wantLogical:     true,
			wantLogicalType: avro.TimestampMicros,
		},
		{
			name:            "Local Timestamp Millis",
			schema:          `{"type": "long", "logicalType": "local-timestamp-millis"}`,
			wantType:        avro.Long,
			wantLogical:     true,
			wantLogicalType: avro.LocalTimestampMillis,
		},
		{
			name:            "Local Timestamp Micros",
			schema:          `{"type": "long", "logicalType": "local-timestamp-micros"}`,
			wantType:        avro.Long,
			wantLogical:     true,
			wantLogicalType: avro.LocalTimestampMicros,
		},
		{
			name:            "UUID",
			schema:          `{"type": "string", "logicalType": "uuid"}`,
			wantType:        avro.String,
			wantLogical:     true,
			wantLogicalType: avro.UUID,
		},
		{
			name:            "Duration",
			schema:          `{"type": "fixed", "name":"test", "size": 12, "logicalType": "duration"}`,
			wantType:        avro.Fixed,
			wantLogical:     true,
			wantLogicalType: avro.Duration,
		},
		{
			name:        "Invalid Duration",
			schema:      `{"type": "fixed", "name":"test", "size": 11, "logicalType": "duration"}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:            "Bytes Decimal",
			schema:          `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}`,
			wantType:        avro.Bytes,
			wantLogical:     true,
			wantLogicalType: avro.Decimal,
			assertFn: func(t *testing.T, ls avro.LogicalSchema) {
				dec, ok := ls.(*avro.DecimalLogicalSchema)
				require.True(t, ok)
				assert.Equal(t, 4, dec.Precision())
				assert.Equal(t, 2, dec.Scale())
			},
		},
		{
			name:            "Bytes Decimal No Scale",
			schema:          `{"type": "bytes", "logicalType": "decimal", "precision": 4}`,
			wantType:        avro.Bytes,
			wantLogical:     true,
			wantLogicalType: avro.Decimal,
			assertFn: func(t *testing.T, ls avro.LogicalSchema) {
				dec, ok := ls.(*avro.DecimalLogicalSchema)
				require.True(t, ok)
				assert.Equal(t, 4, dec.Precision())
				assert.Equal(t, 0, dec.Scale())
			},
		},
		{
			name:        "Bytes Decimal Negative Precision",
			schema:      `{"type": "bytes", "logicalType": "decimal", "precision": 0}`,
			wantType:    avro.Bytes,
			wantLogical: false,
		},
		{
			name:        "Bytes Decimal Negative Scale",
			schema:      `{"type": "bytes", "logicalType": "decimal", "precision": 1, "scale": -1}`,
			wantType:    avro.Bytes,
			wantLogical: false,
		},
		{
			name:        "Bytes Decimal Scale Larger Than Precision",
			schema:      `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 6}`,
			wantType:    avro.Bytes,
			wantLogical: false,
		},
		{
			name:            "Fixed Decimal",
			schema:          `{"type": "fixed", "name":"test", "size": 12, "logicalType": "decimal", "precision": 4, "scale": 2}`,
			wantType:        avro.Fixed,
			wantLogical:     true,
			wantLogicalType: avro.Decimal,
			assertFn: func(t *testing.T, ls avro.LogicalSchema) {
				dec, ok := ls.(*avro.DecimalLogicalSchema)
				require.True(t, ok)
				assert.Equal(t, 4, dec.Precision())
				assert.Equal(t, 2, dec.Scale())
			},
		},
		{
			name:            "Fixed Decimal No Scale",
			schema:          `{"type": "fixed", "name":"test", "size": 12, "logicalType": "decimal", "precision": 4}`,
			wantType:        avro.Fixed,
			wantLogical:     true,
			wantLogicalType: avro.Decimal,
			assertFn: func(t *testing.T, ls avro.LogicalSchema) {
				dec, ok := ls.(*avro.DecimalLogicalSchema)
				require.True(t, ok)
				assert.Equal(t, 4, dec.Precision())
				assert.Equal(t, 0, dec.Scale())
			},
		},
		{
			name:        "Fixed Decimal Negative Precision",
			schema:      `{"type": "fixed", "name":"test", "size": 12, "logicalType": "decimal", "precision": 0}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:        "Fixed Decimal Zero Size",
			schema:      `{"type": "fixed", "name":"test", "size": 0, "logicalType": "decimal", "precision": 1}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:        "Fixed Decimal Precision Too Large",
			schema:      `{"type": "fixed", "name":"test", "size": 4, "logicalType": "decimal", "precision": 10}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:        "Fixed Decimal Precision Exceeds Signed Capacity",
			schema:      `{"type": "fixed", "name":"test", "size": 3, "logicalType": "decimal", "precision": 7}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:        "Fixed Decimal Fractional Precision",
			schema:      `{"type": "fixed", "name":"test", "size": 3, "logicalType": "decimal", "precision": 6.9}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:        "Fixed Decimal Fractional Scale",
			schema:      `{"type": "fixed", "name":"test", "size": 3, "logicalType": "decimal", "precision": 6, "scale": 1.9}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:            "Fixed Decimal Large Size",
			schema:          `{"type": "fixed", "name":"test", "size": 128, "logicalType": "decimal", "precision": 1}`,
			wantType:        avro.Fixed,
			wantLogical:     true,
			wantLogicalType: avro.Decimal,
		},
		{
			name:        "Fixed Decimal Scale Larger Than Precision",
			schema:      `{"type": "fixed", "name":"test", "size": 12, "logicalType": "decimal", "precision": 4, "scale": 6}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})
			require.NoError(t, err)

			assert.Equal(t, test.wantType, schema.Type())

			lts, ok := schema.(avro.LogicalTypeSchema)
			if !ok {
				assert.Fail(t, "logical type schema expected")
				return
			}

			ls := lts.Logical()
			require.Equal(t, test.wantLogical, ls != nil)
			if !test.wantLogical {
				return
			}

			assert.Equal(t, test.wantLogicalType, ls.Type())

			if test.assertFn != nil {
				test.assertFn(t, ls)
			}
		})
	}
}

func TestSchema_FingerprintUsing(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		typ    avro.FingerprintType
		want   []byte
	}{
		{
			name:   "Null CRC64",
			schema: "null",
			typ:    avro.CRC64Avro,
			want:   []byte{0x63, 0xdd, 0x24, 0xe7, 0xcc, 0x25, 0x8f, 0x8a},
		},
		{
			name:   "Null CRC64-AVRO-LE",
			schema: "null",
			typ:    avro.CRC64AvroLE,
			want:   []byte{0x8a, 0x8f, 0x25, 0xcc, 0xe7, 0x24, 0xdd, 0x63},
		},
		{
			name:   "Null MD5",
			schema: "null",
			typ:    avro.MD5,
			want:   []byte{0x9b, 0x41, 0xef, 0x67, 0x65, 0x1c, 0x18, 0x48, 0x8a, 0x8b, 0x8, 0xbb, 0x67, 0xc7, 0x56, 0x99},
		},
		{
			name:   "Null SHA256",
			schema: "null",
			typ:    avro.SHA256,
			want:   []byte{0xf0, 0x72, 0xcb, 0xec, 0x3b, 0xf8, 0x84, 0x18, 0x71, 0xd4, 0x28, 0x42, 0x30, 0xc5, 0xe9, 0x83, 0xdc, 0x21, 0x1a, 0x56, 0x83, 0x7a, 0xed, 0x86, 0x24, 0x87, 0x14, 0x8f, 0x94, 0x7d, 0x1a, 0x1f},
		},
		{
			name:   "Primitive CRC64",
			schema: "string",
			typ:    avro.CRC64Avro,
			want:   []byte{0x8f, 0x1, 0x48, 0x72, 0x63, 0x45, 0x3, 0xc7},
		},
		{
			name:   "Record CRC64",
			schema: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "doc": "docs", "fields":[{"name": "field", "type": "int"}]}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0xaf, 0x30, 0x30, 0xf0, 0x1c, 0x99, 0x76, 0xda},
		},
		{
			name:   "Enum CRC64",
			schema: `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0xc, 0xb0, 0xa2, 0xa6, 0x5f, 0x96, 0x8, 0xd1},
		},
		{
			name:   "Array CRC64",
			schema: `{"type":"array", "items": "int"}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0x52, 0x2b, 0x81, 0x4f, 0xc9, 0x63, 0xb4, 0xbe},
		},
		{
			name:   "Map CRC64",
			schema: `{"type":"map", "values": "int"}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0xdb, 0x39, 0xe2, 0xc2, 0x53, 0x4c, 0x89, 0x73},
		},
		{
			name:   "Union CRC64",
			schema: `["null", "int"]`,
			typ:    avro.CRC64Avro,
			want:   []byte{0xd5, 0x1c, 0xc0, 0x92, 0x2b, 0x46, 0xb1, 0xd7},
		},
		{
			name:   "Fixed CRC64",
			schema: `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0x1, 0x7c, 0x1f, 0x7f, 0xa7, 0x6d, 0xa0, 0xa1},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})
			require.NoError(t, err)

			got, err := schema.FingerprintUsing(test.typ)

			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestSchema_FingerprintUsingReference(t *testing.T) {
	schema := avro.MustParse(`
{
   "type": "record",
   "name": "valid_name",
   "namespace": "org.hamba.avro",
   "fields": [
       {"name": "intField", "type": "int"},
       {"name": "Ref", "type": "valid_name"}
   ]
}
`)

	got, err := schema.(*avro.RecordSchema).Fields()[1].Type().FingerprintUsing(avro.CRC64Avro)

	require.NoError(t, err)
	assert.Equal(t, []byte{0xe1, 0xd6, 0x1e, 0x7c, 0x2f, 0xe3, 0x3c, 0x2b}, got)
}

func TestSchema_FingerprintUsingInvalidType(t *testing.T) {
	schema := avro.MustParse("string")

	_, err := schema.FingerprintUsing("test")

	assert.Error(t, err)
}

func TestSchema_MultiFile(t *testing.T) {
	got, err := avro.ParseFiles("testdata/superhero-part1.avsc", "testdata/superhero-part2.avsc")

	require.NoError(t, err)
	want, err := avro.ParseFiles("testdata/superhero.avsc")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestParseWithCache_DoesNotMutateEarlierRecursiveRoots(t *testing.T) {
	cache := &avro.SchemaCache{}
	first, err := avro.ParseWithCache(`{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"a","type":"A"}]}}]}`, "", cache)
	require.NoError(t, err)
	wantString := first.String()
	wantFingerprint := first.Fingerprint()

	second, err := avro.ParseWithCache(`"B"`, "", cache)
	require.NoError(t, err)
	require.Equal(t, "B", second.(avro.NamedSchema).FullName())
	require.Equal(t, wantString, first.String())
	require.Equal(t, wantFingerprint, first.Fingerprint())

	again, err := avro.ParseWithCache(`"A"`, "", cache)
	require.NoError(t, err)
	require.Equal(t, wantString, again.String())
	require.Equal(t, wantString, first.String())
	require.Equal(t, wantFingerprint, first.Fingerprint())
}

func TestParseWithCache_ConcurrentRecursiveRootsAreIndependent(t *testing.T) {
	cache := &avro.SchemaCache{}
	first, err := avro.ParseWithCache(`{"type":"record","name":"ConcurrentA","fields":[{"name":"b","type":{"type":"record","name":"ConcurrentB","fields":[{"name":"a","type":"ConcurrentA"}]}}]}`, "", cache)
	require.NoError(t, err)
	wantString := first.String()
	wantFingerprint := first.Fingerprint()

	errs := make(chan error, 200)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		name := "ConcurrentA"
		if i%2 == 0 {
			name = "ConcurrentB"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := avro.ParseWithCache(`"`+name+`"`, "", cache); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err)
	}
	require.Equal(t, wantString, first.String())
	require.Equal(t, wantFingerprint, first.Fingerprint())
}

func TestSchema_DoesNotAllowDuplicateFullNames(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "Interop",
   "namespace": "org.hamba.avro",
   "fields": [
       {
           "name": "intField",
           "type": {
             "type": "record",
             "name": "Interop",
             "fields": []
           }
       }
  ]
}
`

	_, err := avro.Parse(schm)

	assert.Error(t, err)
}

func TestSchema_DoesNotAllowDuplicateAliases(t *testing.T) {
	schema := `{"type":"record","name":"Root","fields":[{"name":"a","type":{"type":"record","name":"A","aliases":["shared"],"fields":[]}},{"name":"b","type":{"type":"record","name":"B","aliases":["shared"],"fields":[]}}]}`

	_, err := avro.ParseWithCache(schema, "", &avro.SchemaCache{})
	assert.Error(t, err)
}

func TestSchema_Interop(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "Interop",
   "namespace": "org.hamba.avro",
   "fields": [
       {
           "name": "intField",
           "type": "int"
       },
       {
           "name": "longField",
           "type": "long"
       },
       {
           "name": "stringField",
           "type": "string"
       },
       {
           "name": "boolField",
           "type": "boolean"
       },
       {
           "name": "floatField",
           "type": "float"
       },
       {
           "name": "doubleField",
           "type": "double"
       },
       {
           "name": "bytesField",
           "type": "bytes"
       },
       {
           "name": "nullField",
           "type": "null"
       },
       {
           "name": "arrayField",
           "type": {
               "type": "array",
               "items": "double"
           }
       },
       {
           "name": "mapField",
           "type": {
               "type": "map",
               "values": {
                   "type": "record",
                   "name": "Foo",
                   "fields": [
                       {
                           "name": "label",
                           "type": "string"
                       }
                   ]
               }
           }
       },
       {
           "name": "unionField",
           "type": [
               "boolean",
               "double",
               {
                   "type": "array",
                   "items": "bytes"
               }
           ]
       },
       {
           "name": "enumField",
           "type": {
               "type": "enum",
               "name": "Kind",
               "symbols": [
                   "A",
                   "B",
                   "C"
               ]
           }
       },
       {
           "name": "fixedField",
           "type": {
               "type": "fixed",
               "name": "MD5",
               "size": 16
           }
       },
       {
           "name": "recordField",
           "type": {
               "type": "record",
               "name": "Node",
               "fields": [
                   {
                       "name": "label",
                       "type": "string"
                   },
                   {
                       "name": "child",
                       "type": {"type": "org.hamba.avro.Node"}
                   },
                   {
                       "name": "children",
                       "type": {
                           "type": "array",
                           "items": "Node"
                       }
                   }
               ]
           }
       }
   ]
}`

	_, err := avro.Parse(schm)

	assert.NoError(t, err)
}

func TestSchema_ParseBackAndForth(t *testing.T) {
	schemaStr := `
{
    "name": "a.b.rootType",
    "type": "record",
    "fields": [
        {
            "name": "someEnum1",
            "type": {
                "name": "a.b.rootType.classEnum",
                "type": "enum",
                "symbols": ["A", "B", "C"]
            }
        },
        {
            "name": "someEnum2",
            "type": "a.b.rootType.classEnum"
        }
    ]
}`

	schema, err := avro.Parse(schemaStr)
	require.NoError(t, err)

	b, err := json.Marshal(schema)
	require.NoError(t, err)

	schema2, err := avro.ParseBytes(b)

	assert.NoError(t, err)
	assert.Equal(t, schema, schema2)
}

func TestSchema_DereferencingRectifiesAlreadySeenSchema(t *testing.T) {
	schmCache := &avro.SchemaCache{}
	_, err := avro.ParseWithCache(`{
  "type": "record",
  "name": "test1",
  "namespace": "org.hamba.avro",
  "fields": [
    {"name": "a", "type": "string"}
  ]
}`, "", schmCache)
	require.NoError(t, err)
	_, err = avro.ParseWithCache(`{
  "type": "record",
  "name": "test2",
  "namespace": "org.hamba.avro",
  "fields": [
    {
      "name": "inner",
      "type": {
        "name": "InnerRecord",
        "type": "record",
        "fields": [
          {"name": "b", "type": "org.hamba.avro.test1"}
        ]
      }
    }
  ]
}`, "", schmCache)
	require.NoError(t, err)
	schm, err := avro.ParseWithCache(`{
  "type": "record",
  "name": "test3",
  "namespace": "org.hamba.avro",
  "fields": [
    {"name": "c", "type": "org.hamba.avro.test1"},
    {"name": "d", "type": "org.hamba.avro.test2"}
  ]
}
`, "", schmCache)
	require.NoError(t, err)

	strSchema := schm.String()

	n := strings.Count(strSchema, `"name":"org.hamba.avro.test1"`)
	assert.Equal(t, 1, n)
}

func TestParse_PreservesAllProperties(t *testing.T) {
	testCases := []struct {
		name   string
		schema string
		check  func(t *testing.T, schema avro.Schema)
	}{
		{
			name: "record",
			schema: `{
				"type": "record",
				"name": "SomeRecord",
				"logicalType": "complex-number",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3],
				"fields": [
					{
						"name": "r",
						"type": "double"
					},
					{
						"name": "i",
						"type": "double"
					}
				]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.RecordSchema)
				assert.Equal(t, map[string]any{
					"logicalType": "complex-number",
					"precision":   "abc",
					"scale":       "def",
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "array",
			schema: `{
				"type": "array",
				"name": "SomeArray",
				"logicalType": "complex-number",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3],
				"items": "double"
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.ArraySchema)
				assert.Equal(t, map[string]any{
					"name":        "SomeArray",
					"logicalType": "complex-number",
					"precision":   "abc",
					"scale":       "def",
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "map",
			schema: `{
				"type": "map",
				"name": "SomeMap",
				"logicalType": "weights",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3],
				"values": "double"
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.MapSchema)
				assert.Equal(t, map[string]any{
					"name":        "SomeMap",
					"logicalType": "weights",
					"precision":   "abc",
					"scale":       "def",
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "enum",
			schema: `{
				"type": "enum",
				"name": "SomeEnum",
				"logicalType": "status",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3],
				"symbols": ["A", "B", "C"],
				"default": "A"
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.EnumSchema)
				assert.Equal(t, map[string]any{
					"logicalType": "status",
					"precision":   "abc",
					"scale":       "def",
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "fixed-no-logical-type",
			schema: `{
				"type": "fixed",
				"name": "SomeFixed",
				"size": 16,
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.FixedSchema)
				assert.Equal(t, map[string]any{
					"precision": "abc",
					"scale":     "def",
					"other":     []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "fixed-unknown-logical-type",
			schema: `{
				"type": "fixed",
				"name": "SomeFixed",
				"size": 16,
				"logicalType": "uuid",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.FixedSchema)
				assert.Equal(t, map[string]any{
					"logicalType": "uuid",
					"precision":   "abc",
					"scale":       "def",
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "fixed-invalid-logical-type",
			schema: `{
				"type": "fixed",
				"name": "SomeFixed",
				"size": 16,
				"logicalType": {"foo": "bar", "baz": ["x","y","z"]},
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.FixedSchema)
				assert.Equal(t, map[string]any{
					"logicalType": map[string]any{
						"foo": "bar",
						"baz": []any{"x", "y", "z"},
					},
					"precision": "abc",
					"scale":     "def",
					"other":     []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "fixed-decimal-logical-type",
			schema: `{
				"type": "fixed",
				"name": "SomeFixed",
				"size": 16,
				"logicalType": "decimal",
				"precision": 10,
				"scale": 3,
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.FixedSchema)
				assert.Equal(t, map[string]any{
					"other": []any{1.0, 2.0, 3.0},
				}, rec.Props())
				assert.Equal(t, avro.Decimal, rec.Logical().Type())
			},
		},
		{
			name: "fixed-invalid-decimal-logical-type-bad-values", // scale is too high
			schema: `{
				"type": "fixed",
				"name": "SomeFixed",
				"size": 16,
				"logicalType": "decimal",
				"precision": 10,
				"scale": 20,
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.FixedSchema)
				assert.Equal(t, map[string]any{
					"logicalType": "decimal",
					"precision":   10.0,
					"scale":       20.0,
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "fixed-invalid-decimal-logical-type-bad-type", // precision and scale aren't ints
			schema: `{
				"type": "fixed",
				"name": "SomeFixed",
				"size": 16,
				"logicalType": "decimal",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.FixedSchema)
				assert.Equal(t, map[string]any{
					"logicalType": "decimal",
					"precision":   "abc",
					"scale":       "def",
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "fixed-duration-logical",
			schema: `{
				"type": "fixed",
				"name": "SomeFixed",
				"size": 12,
				"logicalType": "duration",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.FixedSchema)
				assert.Equal(t, map[string]any{
					"precision": "abc",
					"scale":     "def",
					"other":     []any{1.0, 2.0, 3.0},
				}, rec.Props())
				assert.Equal(t, avro.Duration, rec.Logical().Type())
			},
		},
		{
			name: "primitive-no-logical-type",
			schema: `{
				"type": "long",
				"name": "SomeLong",
				"size": 16,
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.PrimitiveSchema)
				assert.Equal(t, map[string]any{
					"name":      "SomeLong",
					"size":      16.0,
					"precision": "abc",
					"scale":     "def",
					"other":     []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "primitive-unknown-logical-type",
			schema: `{
				"type": "string",
				"name": "SomeString",
				"logicalType": "enum-name",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.PrimitiveSchema)
				assert.Equal(t, map[string]any{
					"name":        "SomeString",
					"logicalType": "enum-name",
					"precision":   "abc",
					"scale":       "def",
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "primitive-invalid-logical-type",
			schema: `{
				"type": "string",
				"name": "SomeString",
				"logicalType": {"foo": "bar", "baz": ["x","y","z"]},
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.PrimitiveSchema)
				assert.Equal(t, map[string]any{
					"name": "SomeString",
					"logicalType": map[string]any{
						"foo": "bar",
						"baz": []any{"x", "y", "z"},
					},
					"precision": "abc",
					"scale":     "def",
					"other":     []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "primitive-date-logical-type",
			schema: `{
				"type": "int",
				"logicalType": "date",
				"precision": 10,
				"scale": 3,
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.PrimitiveSchema)
				assert.Equal(t, map[string]any{
					"precision": 10.0,
					"scale":     3.0,
					"other":     []any{1.0, 2.0, 3.0},
				}, rec.Props())
				assert.Equal(t, avro.Date, rec.Logical().Type())
			},
		},
		{
			name: "primitive-decimal-logical-type",
			schema: `{
				"type": "bytes",
				"logicalType": "decimal",
				"precision": 10,
				"scale": 3,
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.PrimitiveSchema)
				assert.Equal(t, map[string]any{
					"other": []any{1.0, 2.0, 3.0},
				}, rec.Props())
				assert.Equal(t, avro.Decimal, rec.Logical().Type())
			},
		},
		{
			name: "primitive-invalid-decimal-logical-type-bad-values", // scale is too high
			schema: `{
				"type": "bytes",
				"logicalType": "decimal",
				"precision": 10,
				"scale": 20,
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.PrimitiveSchema)
				assert.Equal(t, map[string]any{
					"logicalType": "decimal",
					"precision":   10.0,
					"scale":       20.0,
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "primitive-invalid-decimal-logical-type-bad-type", // precision and scale aren't ints
			schema: `{
				"type": "bytes",
				"logicalType": "decimal",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.PrimitiveSchema)
				assert.Equal(t, map[string]any{
					"logicalType": "decimal",
					"precision":   "abc",
					"scale":       "def",
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
		{
			name: "null",
			schema: `{
				"type": "null",
				"name": "SomeMap",
				"logicalType": "weights",
				"precision": "abc",
				"scale": "def",
				"other": [1,2,3]
			}`,
			check: func(t *testing.T, schema avro.Schema) {
				rec := schema.(*avro.NullSchema)
				assert.Equal(t, map[string]any{
					"name":        "SomeMap",
					"logicalType": "weights",
					"precision":   "abc",
					"scale":       "def",
					"other":       []any{1.0, 2.0, 3.0},
				}, rec.Props())
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			schema, err := avro.Parse(testCase.schema)
			require.NoError(t, err)
			testCase.check(t, schema)
		})
	}
}

func TestNewSchema_IgnoresInvalidProperties(t *testing.T) {
	t.Run("record", func(t *testing.T) {
		rec, err := avro.NewRecordSchema("abc.def.Xyz", "", nil,
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"name":      123,
				"namespace": "abc",
				"doc":       "blah",
				"fields":    []any{1, 2, 3},
				"aliases":   "foo",
				"type":      false,
				// valid
				"other":       true,
				"logicalType": "baz",
			}))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{
			"other":       true,
			"logicalType": "baz",
		}, rec.Props())
	})
	t.Run("enum", func(t *testing.T) {
		rec, err := avro.NewEnumSchema("abc.def.Xyz", "", []string{"ABC"},
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"name":      123,
				"namespace": "abc",
				"doc":       "blah",
				"symbols":   []any{1, 2, 3},
				"aliases":   "foo",
				"type":      false,
				"default":   123.456,
				// valid
				"other":       true,
				"logicalType": "baz",
			}))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{
			"other":       true,
			"logicalType": "baz",
		}, rec.Props())
	})
	t.Run("fixed-no-logical-type", func(t *testing.T) {
		rec, err := avro.NewFixedSchema("abc.def.Xyz", "", 10, nil,
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"name":      123,
				"namespace": "abc",
				"size":      []any{1, 2, 3},
				"aliases":   "foo",
				"type":      false,
				// valid
				"doc":         "blah",
				"other":       true,
				"logicalType": "baz",
				"precision":   "abc",
				"scale":       "def",
			}))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{
			"doc":         "blah",
			"other":       true,
			"logicalType": "baz",
			"precision":   "abc",
			"scale":       "def",
		}, rec.Props())
	})
	t.Run("fixed-logical-type", func(t *testing.T) {
		rec, err := avro.NewFixedSchema("abc.def.Xyz", "", 10, avro.NewPrimitiveLogicalSchema(avro.Duration),
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"name":        123,
				"namespace":   "abc",
				"size":        []any{1, 2, 3},
				"aliases":     "foo",
				"type":        false,
				"logicalType": "baz",
				// valid
				"doc":       "blah",
				"other":     true,
				"precision": "abc",
				"scale":     "def",
			}))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{
			"doc":       "blah",
			"other":     true,
			"precision": "abc",
			"scale":     "def",
		}, rec.Props())
	})
	t.Run("fixed-decimal-logical-type", func(t *testing.T) {
		rec, err := avro.NewFixedSchema("abc.def.Xyz", "", 10, avro.NewDecimalLogicalSchema(10, 0),
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"name":        123,
				"namespace":   "abc",
				"size":        []any{1, 2, 3},
				"aliases":     "foo",
				"type":        false,
				"logicalType": "baz",
				"precision":   "abc",
				"scale":       "def",
				// valid
				"doc":   "blah",
				"other": true,
			}))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{
			"doc":   "blah",
			"other": true,
		}, rec.Props())
	})
	t.Run("array", func(t *testing.T) {
		rec := avro.NewArraySchema(avro.NewPrimitiveSchema(avro.String, nil),
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"items": []any{1, 2, 3},
				"type":  false,
				// valid
				"name":        123,
				"namespace":   "abc",
				"doc":         "blah",
				"aliases":     "foo",
				"other":       true,
				"logicalType": "baz",
			}))
		assert.Equal(t, map[string]any{
			"name":        123,
			"namespace":   "abc",
			"doc":         "blah",
			"aliases":     "foo",
			"other":       true,
			"logicalType": "baz",
		}, rec.Props())
	})
	t.Run("map", func(t *testing.T) {
		rec := avro.NewMapSchema(avro.NewPrimitiveSchema(avro.String, nil),
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"values": []any{1, 2, 3},
				"type":   false,
				// valid
				"name":        123,
				"namespace":   "abc",
				"doc":         "blah",
				"aliases":     "foo",
				"other":       true,
				"logicalType": "baz",
			}))
		assert.Equal(t, map[string]any{
			"name":        123,
			"namespace":   "abc",
			"doc":         "blah",
			"aliases":     "foo",
			"other":       true,
			"logicalType": "baz",
		}, rec.Props())
	})
	t.Run("primitive-no-logical-type", func(t *testing.T) {
		rec := avro.NewPrimitiveSchema(avro.Long, nil,
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"type": false,
				// valid
				"name":        123,
				"namespace":   "abc",
				"size":        []any{1, 2, 3},
				"aliases":     "foo",
				"doc":         "blah",
				"other":       true,
				"logicalType": "baz",
				"precision":   "abc",
				"scale":       "def",
			}))
		assert.Equal(t, map[string]any{
			"name":        123,
			"namespace":   "abc",
			"size":        []any{1, 2, 3},
			"aliases":     "foo",
			"doc":         "blah",
			"other":       true,
			"logicalType": "baz",
			"precision":   "abc",
			"scale":       "def",
		}, rec.Props())
	})
	t.Run("primitive-logical-type", func(t *testing.T) {
		rec := avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMicros),
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"type":        false,
				"logicalType": "baz",
				// valid
				"name":      123,
				"namespace": "abc",
				"size":      []any{1, 2, 3},
				"aliases":   "foo",
				"doc":       "blah",
				"other":     true,
				"precision": "abc",
				"scale":     "def",
			}))
		assert.Equal(t, map[string]any{
			"name":      123,
			"namespace": "abc",
			"size":      []any{1, 2, 3},
			"aliases":   "foo",
			"doc":       "blah",
			"other":     true,
			"precision": "abc",
			"scale":     "def",
		}, rec.Props())
	})
	t.Run("primitive-decimal-logical-type", func(t *testing.T) {
		rec := avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(10, 0),
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"type":        false,
				"logicalType": "baz",
				"precision":   "abc",
				"scale":       "def",
				// valid
				"name":      123,
				"namespace": "abc",
				"size":      []any{1, 2, 3},
				"aliases":   "foo",
				"doc":       "blah",
				"other":     true,
			}))
		assert.Equal(t, map[string]any{
			"name":      123,
			"namespace": "abc",
			"size":      []any{1, 2, 3},
			"aliases":   "foo",
			"doc":       "blah",
			"other":     true,
		}, rec.Props())
	})
	t.Run("null", func(t *testing.T) {
		rec := avro.NewNullSchema(
			avro.WithProps(map[string]any{
				// invalid (conflict with other type properties)
				"type": false,
				// valid
				"name":        123,
				"namespace":   "abc",
				"doc":         "blah",
				"aliases":     "foo",
				"other":       true,
				"logicalType": "baz",
				"precision":   "abc",
				"scale":       "def",
			}))
		assert.Equal(t, map[string]any{
			"name":        123,
			"namespace":   "abc",
			"doc":         "blah",
			"aliases":     "foo",
			"other":       true,
			"logicalType": "baz",
			"precision":   "abc",
			"scale":       "def",
		}, rec.Props())
	})
}

func TestConcurrentParse(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := avro.ParseFiles("testdata/concurrent-schema.avsc")
			assert.NoError(t, err)
		}()
	}

	wg.Wait()
}

func FuzzSchemaParse(f *testing.F) {
	for _, schema := range []string{
		`"null"`,
		`{"type":"array","items":"long"}`,
		`{"type":"map","values":"string"}`,
		`["null","int","string"]`,
		`{"type":"record","name":"R","fields":[{"name":"value","type":"bytes"}]}`,
	} {
		f.Add([]byte(schema))
	}
	f.Add([]byte{})

	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 4<<20 {
			return
		}
		_, _ = avro.ParseBytes(data)
	})
}

func avroTestTempDir(t *testing.T) string {
	t.Helper()
	tempRoot, err := filepath.Abs("../../../tmp")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(tempRoot, 0o700))
	tempDir, err := os.MkdirTemp(tempRoot, "avro-schema-test-")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Errorf("remove Avro schema test directory: %v", err)
		}
	})
	return tempDir
}
