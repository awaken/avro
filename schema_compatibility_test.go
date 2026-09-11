package avro_test

import (
	"encoding/json"
	"math/big"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type blockingCompatibilitySchema struct {
	typ         avro.Type
	fingerprint [32]byte
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
}

func (s *blockingCompatibilitySchema) Type() avro.Type {
	if s.entered != nil {
		s.once.Do(func() {
			close(s.entered)
			<-s.release
		})
	}
	return s.typ
}

func (s *blockingCompatibilitySchema) String() string {
	return `"` + string(s.typ) + `"`
}

func (s *blockingCompatibilitySchema) Fingerprint() [32]byte {
	return s.fingerprint
}

func (s *blockingCompatibilitySchema) FingerprintUsing(avro.FingerprintType) ([]byte, error) {
	return append([]byte(nil), s.fingerprint[:]...), nil
}

func (s *blockingCompatibilitySchema) CacheFingerprint() [32]byte {
	return s.fingerprint
}

func TestNewSchemaCompatibility(t *testing.T) {
	sc := avro.NewSchemaCompatibility()

	assert.IsType(t, &avro.SchemaCompatibility{}, sc)
}

func TestSchemaCompatibility_AliasAmbiguity(t *testing.T) {
	readers := []string{
		`{"type":"record","name":"R","fields":[{"name":"value","type":"int","aliases":["a","b"]}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int","aliases":["b"]}]}`,
		`{"type":"record","name":"R","fields":[{"name":"value","type":"int","aliases":["a","b"],"default":0},{"name":"next","type":["null","R"],"default":null}]}`,
	}
	writers := []string{
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"b","type":"int"},{"name":"a","type":"int"}]}`,
	}
	for _, reader := range readers {
		for _, writer := range writers {
			r, err := avro.ParseWithCache(reader, "", nil)
			require.NoError(t, err)
			w, err := avro.ParseWithCache(writer, "", nil)
			require.NoError(t, err)
			c := avro.NewSchemaCompatibility()
			require.Error(t, c.Compatible(r, w), "ambiguous reader: %s; writer: %s", reader, writer)
			_, err = c.Resolve(r, w)
			require.Error(t, err)
		}
	}
}

func TestSchemaCompatibility_AliasMapping(t *testing.T) {
	r, err := avro.ParseWithCache(`{"type":"record","name":"R","fields":[{"name":"value","type":"long","aliases":["old"]},{"name":"added","type":"int","default":7}]}`, "", nil)
	require.NoError(t, err)
	for _, fields := range []string{
		`{"name":"skip","type":"int","aliases":["value","added"]},{"name":"old","type":"int"}`,
		`{"name":"old","type":"int"},{"name":"skip","type":"int","aliases":["value","added"]}`,
	} {
		w, err := avro.ParseWithCache(`{"type":"record","name":"R","fields":[`+fields+`]}`, "", nil)
		require.NoError(t, err)
		c := avro.NewSchemaCompatibility()
		require.NoError(t, c.Compatible(r, w))
		resolved, err := c.Resolve(r, w)
		require.NoError(t, err)
		data, err := avro.Marshal(w, map[string]any{"old": 23, "skip": 99})
		require.NoError(t, err)
		var got map[string]any
		require.NoError(t, avro.Unmarshal(resolved, data, &got))
		require.Equal(t, map[string]any{"value": int64(23), "added": 7}, got)
	}
}

func TestSchemaCompatibility_RejectsNilSchemas(t *testing.T) {
	valid := avro.MustParse(`"int"`)
	var typedNil *avro.PrimitiveSchema

	tests := []struct {
		name   string
		reader avro.Schema
		writer avro.Schema
	}{
		{name: "nil reader", writer: valid},
		{name: "typed nil reader", reader: typedNil, writer: valid},
		{name: "nil writer", reader: valid},
		{name: "typed nil writer", reader: valid, writer: typedNil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compatibility := avro.NewSchemaCompatibility()
			assert.NotPanics(t, func() {
				assert.Error(t, compatibility.Compatible(tt.reader, tt.writer))
			})
			assert.NotPanics(t, func() {
				_, err := compatibility.Resolve(tt.reader, tt.writer)
				assert.Error(t, err)
			})
		})
	}
}

func TestSchemaCompatibility_Compatible(t *testing.T) {
	tests := []struct {
		name    string
		reader  string
		writer  string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "Primitive Matching",
			reader:  `"int"`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Int Promote Long",
			reader:  `"long"`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Int Promote Float",
			reader:  `"float"`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Int Promote Double",
			reader:  `"double"`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Long Promote Float",
			reader:  `"float"`,
			writer:  `"long"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Long Promote Double",
			reader:  `"double"`,
			writer:  `"long"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Float Promote Double",
			reader:  `"double"`,
			writer:  `"float"`,
			wantErr: assert.NoError,
		},
		{
			name:    "String Promote Bytes",
			reader:  `"bytes"`,
			writer:  `"string"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Bytes Promote String",
			reader:  `"string"`,
			writer:  `"bytes"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Match",
			reader:  `["int", "long", "string"]`,
			writer:  `["string", "int", "long"]`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Reader Missing Schema",
			reader:  `["int", "string"]`,
			writer:  `["string", "int", "long"]`,
			wantErr: assert.Error,
		},
		{
			name:    "Union Writer Missing Schema",
			reader:  `["int", "long", "string"]`,
			writer:  `["string", "int"]`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Writer Not Union",
			reader:  `["int", "long", "string"]`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Writer Not Union With Error",
			reader:  `["string"]`,
			writer:  `"int"`,
			wantErr: assert.Error,
		},
		{
			name:    "Union Reader Not Union",
			reader:  `"int"`,
			writer:  `["int"]`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Reader Not Union With Error",
			reader:  `"int"`,
			writer:  `["string", "int", "long"]`,
			wantErr: assert.Error,
		},
		{
			name:    "Array Match",
			reader:  `{"type":"array", "items": "int"}`,
			writer:  `{"type":"array", "items": "int"}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Array Items Mismatch",
			reader:  `{"type":"array", "items": "int"}`,
			writer:  `{"type":"array", "items": "string"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Map Match",
			reader:  `{"type":"map", "values": "int"}`,
			writer:  `{"type":"map", "values": "int"}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Map Items Mismatch",
			reader:  `{"type":"map", "values": "int"}`,
			writer:  `{"type":"map", "values": "string"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Fixed Match",
			reader:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			writer:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Fixed Name Mismatch",
			reader:  `{"type":"fixed", "name":"test1", "namespace": "org.hamba.avro", "size": 12}`,
			writer:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: assert.Error,
		},
		{
			name:    "Fixed Size Mismatch",
			reader:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 13}`,
			writer:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: assert.Error,
		},
		{
			name:    "Enum Match",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Enum Name Mismatch",
			reader:  `{"type":"enum", "name":"test1", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Enum Reader Missing Symbol",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Enum Reader Missing Symbol With Default",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1"], "default": "TEST1"}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Enum Writer Missing Symbol",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1"]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Match",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Name Mismatch",
			reader:  `{"type":"record", "name":"test1", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int", "default": 1}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string", "default": "b"}, {"name": "a", "type": "int"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record Schema Mismatch",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record Reader Field Missing",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Writer Field Missing With Default",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string", "default": "test"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Writer Field Missing Without Default",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Ref Dereference",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name":"test1", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "int"}]}}, {"name": "b", "type": "test1"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name":"test1", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "int"}]}}, {"name": "b", "type": "test"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Breaks Recursion",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "test"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "test"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Comparison with different namespaces",
			writer:  `{"type":"record", "name":"Obj", "namespace": "ns", "fields":[{"name": "a", "type": "int"}]}`,
			reader:  `{"type":"record", "name":"Obj", "fields":[{"name": "a", "type": "int"}]}`,
			wantErr: assert.NoError,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			r, err := avro.ParseWithCache(test.reader, "", &avro.SchemaCache{})
			require.NoError(t, err)
			w, err := avro.ParseWithCache(test.writer, "", &avro.SchemaCache{})
			require.NoError(t, err)
			sc := avro.NewSchemaCompatibility()

			err = sc.Compatible(r, w)

			test.wantErr(t, err)
		})
	}
}

func TestSchemaCompatibility_CompatibleUsesCacheWithNoError(t *testing.T) {
	reader := `"int"`
	writer := `"int"`

	r := avro.MustParse(reader)
	w := avro.MustParse(writer)
	sc := avro.NewSchemaCompatibility()

	_ = sc.Compatible(r, w)

	err := sc.Compatible(r, w)

	assert.NoError(t, err)
}

func TestSchemaCompatibility_CompatibleUsesCacheWithError(t *testing.T) {
	reader := `"int"`
	writer := `"string"`

	r := avro.MustParse(reader)
	w := avro.MustParse(writer)
	sc := avro.NewSchemaCompatibility()

	_ = sc.Compatible(r, w)

	err := sc.Compatible(r, w)

	assert.Error(t, err)
}

func TestSchemaCompatibility_CacheDistinguishesCompatibilityMetadata(t *testing.T) {
	tests := []struct {
		name         string
		compatible   string
		incompatible string
		writer       string
	}{
		{
			name:         "field default",
			compatible:   `{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":0}]}`,
			incompatible: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
			writer:       `{"type":"record","name":"R","fields":[]}`,
		},
		{
			name:         "enum default",
			compatible:   `{"type":"enum","name":"E","symbols":["A"],"default":"A"}`,
			incompatible: `{"type":"enum","name":"E","symbols":["A"]}`,
			writer:       `{"type":"enum","name":"E","symbols":["A","B"]}`,
		},
		{
			name:         "field alias",
			compatible:   `{"type":"record","name":"R","fields":[{"name":"new","aliases":["old"],"type":"int"}]}`,
			incompatible: `{"type":"record","name":"R","fields":[{"name":"new","type":"int"}]}`,
			writer:       `{"type":"record","name":"R","fields":[{"name":"old","type":"int"}]}`,
		},
		{
			name:         "record alias",
			compatible:   `{"type":"record","name":"New","aliases":["Old"],"fields":[]}`,
			incompatible: `{"type":"record","name":"New","fields":[]}`,
			writer:       `{"type":"record","name":"Old","fields":[]}`,
		},
		{
			name:         "nested field default",
			compatible:   `{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"a","type":"int","default":0}]}}]}`,
			incompatible: `{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"a","type":"int"}]}}]}`,
			writer:       `{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[]}}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parse := func(raw string) avro.Schema {
				schema, err := avro.ParseWithCache(raw, "", &avro.SchemaCache{})
				require.NoError(t, err)
				return schema
			}

			compatible := parse(test.compatible)
			incompatible := parse(test.incompatible)
			writer := parse(test.writer)
			require.Equal(t, compatible.Fingerprint(), incompatible.Fingerprint())

			compatibility := avro.NewSchemaCompatibility()
			require.NoError(t, compatibility.Compatible(compatible, writer))
			require.Error(t, compatibility.Compatible(incompatible, writer))

			compatibility = avro.NewSchemaCompatibility()
			require.Error(t, compatibility.Compatible(incompatible, writer))
			require.NoError(t, compatibility.Compatible(compatible, writer))
		})
	}
}

func TestSchemaCompatibility_DecimalRequiresMatchingScaleAndPrecision(t *testing.T) {
	tests := []struct {
		name         string
		compatible   string
		incompatible string
		writer       string
	}{
		{
			name:         "bytes scale",
			compatible:   `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			incompatible: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":3}`,
			writer:       `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			name:         "bytes precision",
			compatible:   `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			incompatible: `{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}`,
			writer:       `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			name:         "fixed scale",
			compatible:   `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":4,"scale":2}`,
			incompatible: `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":4,"scale":3}`,
			writer:       `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":4,"scale":2}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parse := func(raw string) avro.Schema {
				schema, err := avro.ParseWithCache(raw, "", &avro.SchemaCache{})
				require.NoError(t, err)
				return schema
			}

			compatible := parse(test.compatible)
			incompatible := parse(test.incompatible)
			writer := parse(test.writer)
			require.Equal(t, compatible.Fingerprint(), incompatible.Fingerprint())

			compatibility := avro.NewSchemaCompatibility()
			require.NoError(t, compatibility.Compatible(compatible, writer))
			require.Error(t, compatibility.Compatible(incompatible, writer))

			compatibility = avro.NewSchemaCompatibility()
			require.Error(t, compatibility.Compatible(incompatible, writer))
			require.NoError(t, compatibility.Compatible(compatible, writer))
		})
	}
}

func TestSchemaCompatibility_ConcurrentCallsWaitForFinalResult(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	reader := &blockingCompatibilitySchema{
		typ:         avro.Int,
		fingerprint: [32]byte{1},
		entered:     entered,
		release:     release,
	}
	writer := &blockingCompatibilitySchema{typ: avro.String, fingerprint: [32]byte{2}}
	compatibility := avro.NewSchemaCompatibility()

	first := make(chan error, 1)
	go func() {
		first <- compatibility.Compatible(reader, writer)
	}()
	<-entered

	second := make(chan error, 1)
	go func() {
		second <- compatibility.Compatible(reader, writer)
	}()

	var secondErr error
	early := false
	select {
	case secondErr = <-second:
		early = true
	case <-time.After(250 * time.Millisecond):
	}
	close(release)

	assert.Error(t, <-first)
	assert.False(t, early, "concurrent call returned an in-progress recursion marker")
	if !early {
		secondErr = <-second
	}
	assert.Error(t, secondErr)
}

func TestSchemaCompatibility_Resolve(t *testing.T) {
	tests := []struct {
		name   string
		reader string
		writer string
		value  any
		want   any
	}{
		{
			name:   "Int Promote Long",
			reader: `"long"`,
			writer: `"int"`,
			value:  10,
			want:   int64(10),
		},
		{
			name:   "Int Promote Long Time millis",
			reader: `{"type":"long","logicalType":"timestamp-millis"}`,
			writer: `"int"`,
			value:  5000,
			want:   time.UnixMilli(5000).UTC(),
		},
		{
			name:   "Int Promote Long Time micros",
			reader: `{"type":"long","logicalType":"timestamp-micros"}`,
			writer: `"int"`,
			value:  5000,
			want:   time.UnixMicro(5000).UTC(),
		},
		{
			name:   "Int Promote Long Time micros",
			reader: `{"type":"long","logicalType":"time-micros"}`,
			writer: `"int"`,
			value:  5000,
			want:   5000 * time.Microsecond,
		},
		{
			name:   "Int Promote Float",
			reader: `"float"`,
			writer: `"int"`,
			value:  10,
			want:   float32(10),
		},
		{
			name:   "Int Promote Double",
			reader: `"double"`,
			writer: `"int"`,
			value:  10,
			want:   float64(10),
		},
		{
			name:   "Long Promote Float",
			reader: `"float"`,
			writer: `"long"`,
			value:  int64(10),
			want:   float32(10),
		},
		{
			name:   "Long Promote Double",
			reader: `"double"`,
			writer: `"long"`,
			value:  int64(10),
			want:   float64(10),
		},
		{
			name:   "Float Promote Double",
			reader: `"double"`,
			writer: `"float"`,
			value:  float32(10.5),
			want:   float64(10.5),
		},
		{
			name:   "String Promote Bytes",
			reader: `"bytes"`,
			writer: `"string"`,
			value:  "foo",
			want:   []byte("foo"),
		},
		{
			// I'm not sure about this edge cases;
			// I took the reverse path and tried to find a Decimal that can be encoded to
			// a binary that is a valid UTF-8 sequence.
			name:   "String Promote Bytes With Logical Decimal",
			reader: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			writer: `"string"`,
			value:  "d",
			want:   big.NewRat(1, 1),
		},
		{
			name:   "Bytes Promote String",
			reader: `"string"`,
			writer: `"bytes"`,
			value:  []byte("foo"),
			want:   "foo",
		},
		{
			name:   "Array With Items Promotion",
			reader: `{"type":"array", "items": "long"}`,
			writer: `{"type":"array", "items": "int"}`,
			value:  []any{int32(10), int32(15)},
			want:   []any{int64(10), int64(15)},
		},
		{
			name:   "Map With Items Promotion",
			reader: `{"type":"map", "values": "bytes"}`,
			writer: `{"type":"map", "values": "string"}`,
			value:  map[string]any{"foo": "bar"},
			want:   map[string]any{"foo": []byte("bar")},
		},
		{
			name: "Enum Reader Missing Symbols With Default",
			reader: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo"],
				"default": "foo"
			}`,
			writer: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo", "bar"]
			}`,
			value: "bar",
			want:  "foo",
		},
		{
			name: "Enum Writer Missing Symbols",
			reader: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo", "bar"]
			}`,
			writer: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo"]
			}`,
			value: "foo",
			want:  "foo",
		},
		{
			name: "Enum Writer Missing Symbols and Unused Reader Default",
			reader: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo", "bar"],
				"default": "bar"
			}`,
			writer: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo"]
			}`,
			value: "foo",
			want:  "foo",
		},
		{
			name: "Enum With Alias",
			reader: `{
				"type": "enum",
				"name": "test.enum2",
				"aliases": ["test.enum"],
				"symbols": ["foo", "bar"]
			}`,
			writer: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo", "bar"]
			}`,
			value: "foo",
			want:  "foo",
		},
		{
			name: "Fixed With Alias",
			reader: `{
				"type": "fixed",
				"name": "test.fixed2",
				"aliases": ["test.fixed"],
				"size": 3
			}`,
			writer: `{
				"type": "fixed",
				"name": "test.fixed",
				"size": 3
			}`,
			value: [3]byte{'f', 'o', 'o'},
			want:  [3]byte{'f', 'o', 'o'},
		},
		{
			name:   "Union Match",
			reader: `["int", "long", "string"]`,
			writer: `["string", "int", "long"]`,
			value:  "foo",
			want:   "foo",
		},
		{
			name:   "Union Writer Missing Schema",
			reader: `["int", "long", "string"]`,
			writer: `["string", "int"]`,
			value:  "foo",
			want:   "foo",
		},
		{
			name:   "Union Writer Not Union",
			reader: `["int", "long", "string"]`,
			writer: `"int"`,
			value:  10,
			want:   10,
		},
		{
			name:   "Union Reader Not Union",
			reader: `"int"`,
			writer: `["int"]`,
			value:  10,
			want:   10,
		},
		{
			name:   "Record Reader With Alias",
			reader: `{"type":"record", "name":"test2", "aliases": ["test"], "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10},
		},
		{
			name:   "Record Reader Field Missing",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10, "b": "foo"},
			want:   map[string]any{"a": 10},
		},
		{
			name:   "Record Writer Field Missing With Default",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string", "default": "test"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": "test"},
		},
		{
			name:   "Record Reader Field With Alias",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "aa", "type": "int", "aliases": ["a"]}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"aa": 10},
		},
		{
			name:   "Record Reader Field With Alias And Promotion",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "aa", "type": "double", "aliases": ["a"]}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"aa": float64(10)},
		},
		{
			name:   "Record Writer Field Missing With Bytes Default",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "bytes", "default":"\u0066\u006f\u006f"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": []byte("foo")},
		},
		{
			name:   "Record Writer Field Missing With Bytes Default",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "bytes", "default":"\u0066\u006f\u006f"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": []byte("foo")},
		},
		{
			name: "Record Writer Field Missing With Record Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type": {
									"type": "record",
									"name": "test.record",
									"fields" : [
										{"name": "a", "type": "string"},
										{"name": "b", "type": "string"}
									]
								},
								"default":{"a":"foo", "b": "bar"}
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": map[string]any{"a": "foo", "b": "bar"}},
		},
		{
			// assert that we are not mistakenly using the wrong cached decoder.
			// decoder cache must be aware of fields defaults.
			name: "Record Writer Field Missing With Record Default 2",
			reader: `{
						"type":"record", 
						"name":"test", 
						"namespace": "org.hamba.avro",
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type": {
									"type": "record",
									"name": "test.record",
									"fields" : [
										{"name": "a", "type": "string"},
										{"name": "b", "type": "string"}
									]
								},
								"default":{"a":"foo 2", "b": "bar 2"}
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": map[string]any{"a": "foo 2", "b": "bar 2"}},
		},
		{
			name: "Record Writer Field Missing With Map Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type": {
									"type": "map", "values": "string"
								},
								"default":{"foo":"bar"}
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": map[string]any{"foo": "bar"}},
		},
		{
			name: "Record Writer Field Missing With Array Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type": {
									"type": "array", "items": "int"
								},
								"default":[1, 2, 3, 4]
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": []any{1, 2, 3, 4}},
		},
		{
			name: "Record Writer Field Missing With Union Null Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type":["null", "long"],
								"default": null
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": nil},
		},
		{
			name: "Record Writer Field Missing With Union Non-null Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type":["string", "long"],
								"default": "bar"
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": "bar"},
		},
		{
			name: "Record Writer Field Missing With Fixed Duration Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type": {
									"type": "fixed",
									"name": "test.fixed",
									"logicalType":"duration",
									"size":12
								}, 
								"default": "\u000c\u0000\u0000\u0000\u0022\u0000\u0000\u0000\u0052\u00aa\u0008\u0000"
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want: map[string]any{
				"a": 10,
				"b": avro.LogicalDuration{
					Months:       uint32(12),
					Days:         uint32(34),
					Milliseconds: uint32(567890),
				},
			},
		},
		{
			name: "Record Writer Field Missing With Fixed Logical Decimal Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type": {
									"type": "fixed",
									"name": "test.fixed",
									"size": 6,
									"logicalType":"decimal",
									"precision":5,
									"scale":2
								},
								"default": "\u0000\u0000\u0000\u0000\u0087\u0078"
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want: map[string]any{
				"a": 10,
				"b": big.NewRat(1734, 5),
			},
		},
		{
			name: "Record Writer Field Missing With Enum Duration Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type": {
									"type": "enum",
									"name": "test.enum",
									"symbols": ["foo", "bar"]
								},
								"default": "bar"
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want: map[string]any{
				"a": 10,
				"b": "bar",
			},
		},
		{
			name: "Record Writer Field Missing With Ref Default",
			reader: `{
				"type": "record",
				"name": "parent",
				"namespace": "org.hamba.avro",
				"fields": [
					{
						"name": "a",
						"type": {
							"type": "record",
							"name": "embed",
							"namespace": "org.hamba.avro",
							"fields": [{
								"name": "a",
								"type": "long"
							}]
						}
					},
					{
						"name": "b",
						"type": "embed",
						"default": {"a": 20}
					}
				]
			}`,
			writer: `{
				"type": "record",
				"name": "parent",
				"namespace": "org.hamba.avro",
				"fields": [
					{
						"name": "a",
						"type": {
							"type": "record",
							"name": "embed",
							"namespace": "org.hamba.avro",
							"fields": [{
								"name": "a",
								"type": "long"
							}]
						}
					}
				]
			}`,
			value: map[string]any{"a": map[string]any{"a": int64(10)}},
			want: map[string]any{
				"a": map[string]any{"a": int64(10)},
				"b": map[string]any{"a": int64(20)},
			},
		},
		{
			name: "Record Writer Field Missing With Long timestamp-millis Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "string"},
							{
								"name": "b",
								"type": {
									"type": "long", 
									"logicalType": "timestamp-millis"
								}, 
								"default": ` + strconv.FormatInt(1725616800000, 10) + `
							}
						]
					}`,
			writer: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "string"}
						]
					}`,
			value: map[string]any{"a": "foo"},
			want: map[string]any{
				"a": "foo",
				"b": time.UnixMilli(1725616800000).UTC(), // 2024-09-06 10:00:00
			},
		},
		{
			name: "Record Writer Field Missing With Long timestamp-micros Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "string"},
							{
								"name": "b",
								"type": {
									"type": "long", 
									"logicalType": "timestamp-micros"
								}, 
								"default": ` + strconv.FormatInt(1725616800000000, 10) + `
							}
						]
					}`,
			writer: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "string"}
						]
					}`,
			value: map[string]any{"a": "foo"},
			want: map[string]any{
				"a": "foo",
				"b": time.UnixMicro(1725616800000000).UTC(), // 2024-09-06 10:00:00
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			r, err := avro.ParseWithCache(test.reader, "", &avro.SchemaCache{})
			require.NoError(t, err)
			w, err := avro.ParseWithCache(test.writer, "", &avro.SchemaCache{})
			require.NoError(t, err)
			sc := avro.NewSchemaCompatibility()

			b, err := avro.Marshal(w, test.value)
			assert.NoError(t, err)

			sch, err := sc.Resolve(r, w)
			assert.NoError(t, err)

			var result any
			err = avro.Unmarshal(sch, b, &result)
			assert.NoError(t, err)

			assert.Equal(t, test.want, result)
		})
	}
}

func TestSchemaCompatibility_ResolveWithRefs(t *testing.T) {
	sch1 := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"}
		]
	}`)
	sch2 := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "bytes"}
		]
	}`)

	r := avro.NewRefSchema(sch1.(*avro.RecordSchema))
	w := avro.NewRefSchema(sch2.(*avro.RecordSchema))

	sc := avro.NewSchemaCompatibility()

	value := map[string]any{"a": []byte("foo")}
	b, err := avro.Marshal(w, value)
	assert.NoError(t, err)

	sch, err := sc.Resolve(r, w)
	assert.NoError(t, err)

	var result any
	err = avro.Unmarshal(sch, b, &result)
	assert.NoError(t, err)

	want := map[string]any{"a": "foo"}
	assert.Equal(t, want, result)
}

func TestSchemaCompatibility_ResolvePreservesErrorRecord(t *testing.T) {
	reader := avro.MustParse(`{"type":"error","name":"Failure","fields":[]}`)
	writer := avro.MustParse(`{"type":"error","name":"Failure","fields":[]}`)

	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)
	record, ok := resolved.(*avro.RecordSchema)
	require.True(t, ok)
	assert.True(t, record.IsError())

	data, err := json.Marshal(resolved)
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"error","name":"Failure","fields":[]}`, string(data))
}

func TestSchemaCompatibility_ResolveWithComplexUnion(t *testing.T) {
	r := avro.MustParse(`[
				{
					"type":"record",
					"name":"testA",
					"aliases": ["test1"], 
					"namespace": "org.hamba.avro", 
					"fields":[{"name": "a", "type": "long"}]
				},
				{
					"type":"record",
					"name":"testB",
					"aliases": ["test2"], 
					"namespace": "org.hamba.avro", 
					"fields":[{"name": "b", "type": "bytes"}]
				}
			]`)

	w := avro.MustParse(`{
				"type":"record",
				"name":"test2",
				"namespace": "org.hamba.avro", 
				"fields":[{"name": "b", "type": "string"}]
			}`)

	value := map[string]any{"b": "foo"}
	b, err := avro.Marshal(w, value)
	assert.NoError(t, err)

	sc := avro.NewSchemaCompatibility()
	sch, err := sc.Resolve(r, w)
	assert.NoError(t, err)

	var result any
	err = avro.Unmarshal(sch, b, &result)
	assert.NoError(t, err)

	want := map[string]any{"b": []byte("foo")}
	assert.Equal(t, want, result)
}

func TestSchemaCompatibility_ResolveWithFieldMissingInWriterAndReaderStruct(t *testing.T) {
	w := avro.MustParse(`{
				"type":"record", "name":"test", "namespace": "org.hamba.avro", 
				"fields":[
					{"name": "a", "type": "string"}
				]
			}`)

	r := avro.MustParse(`{
				"type":"record", "name":"test", "namespace": "org.hamba.avro", 
				"fields":[
					{"name": "a", "type": "string"},
					{"name": "b", "type": "int", "default": 10},
					{"name": "c", "type": "string", "default": "foo"}
				]
			}`)

	type TestW struct {
		A string `avro:"a"`
	}
	value := TestW{A: "abc"}

	b, err := avro.Marshal(w, value)
	assert.NoError(t, err)

	schema, err := avro.NewSchemaCompatibility().Resolve(r, w)
	assert.NoError(t, err)

	type TestR struct {
		A string `avro:"a"`
		C string `avro:"c"`
	}
	want := TestR{A: "abc", C: "foo"}

	var result TestR
	err = avro.Unmarshal(schema, b, &result)
	assert.NoError(t, err)

	assert.Equal(t, want, result)
}

func TestSchemaCompatibility_ResolveWithEnums(t *testing.T) {
	reader := avro.MustParse(`{"type": "enum", "name": "Enum", "symbols": ["A", "B", "C", "D"]}`)
	writer := avro.MustParse(`{"type": "enum", "name": "Enum", "symbols": ["B", "C", "D"]}`)

	data, err := avro.Marshal(writer, "D")
	assert.NoError(t, err)

	sc := avro.NewSchemaCompatibility()
	rs, err := sc.Resolve(reader, writer)
	assert.NoError(t, err)

	var got string
	err = avro.Unmarshal(rs, data, &got)
	assert.NoError(t, err)

	assert.Equal(t, "D", got)
}

func TestSchemaCompatibilityResolvePreservesMetadata(t *testing.T) {
	t.Run("record and field", func(t *testing.T) {
		reader := avro.MustParse(`{
			"type":"record", "name":"test", "doc":"record doc", "record-meta":"record value",
			"fields":[{
				"name":"value", "type":"int", "doc":"field doc", "field-meta":"field value"
			}]
		}`)
		writer := avro.MustParse(`{
			"type":"record", "name":"test",
			"fields":[{"name":"value", "type":"int"}]
		}`)

		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		require.NoError(t, err)
		record := resolved.(*avro.RecordSchema)

		assert.Equal(t, "record doc", record.Doc())
		assert.Equal(t, "record value", record.Prop("record-meta"))
		assert.Equal(t, "field doc", record.Fields()[0].Doc())
		assert.Equal(t, "field value", record.Fields()[0].Prop("field-meta"))
	})

	t.Run("primitive", func(t *testing.T) {
		reader := avro.MustParse(`{"type":"long", "meta":"primitive value"}`)
		writer := avro.MustParse(`"int"`)

		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		require.NoError(t, err)
		primitive := resolved.(*avro.PrimitiveSchema)

		assert.Equal(t, "primitive value", primitive.Prop("meta"))
	})

	t.Run("enum", func(t *testing.T) {
		reader := avro.MustParse(`{
			"type":"enum", "name":"test", "symbols":["A", "B"], "default":"A",
			"doc":"enum doc", "meta":"enum value"
		}`)
		writer := avro.MustParse(`{
			"type":"enum", "name":"test", "symbols":["B", "C"]
		}`)

		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		require.NoError(t, err)
		enum := resolved.(*avro.EnumSchema)

		assert.Equal(t, "enum doc", enum.Doc())
		assert.Equal(t, "enum value", enum.Prop("meta"))
	})

	t.Run("array", func(t *testing.T) {
		reader := avro.MustParse(`{"type":"array", "items":"long", "meta":"array value"}`)
		writer := avro.MustParse(`{"type":"array", "items":"int"}`)

		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		require.NoError(t, err)
		array := resolved.(*avro.ArraySchema)

		assert.Equal(t, "array value", array.Prop("meta"))
	})

	t.Run("map", func(t *testing.T) {
		reader := avro.MustParse(`{"type":"map", "values":"double", "meta":"map value"}`)
		writer := avro.MustParse(`{"type":"map", "values":"float"}`)

		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		require.NoError(t, err)
		avroMap := resolved.(*avro.MapSchema)

		assert.Equal(t, "map value", avroMap.Prop("meta"))
	})
}
