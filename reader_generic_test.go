package avro_test

import (
	"bytes"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadNextTimeOfDayBounds(t *testing.T) {
	for _, schema := range []avro.Schema{
		avro.MustParse(`{"type":"int","logicalType":"time-millis"}`),
		avro.MustParse(`{"type":"long","logicalType":"time-micros"}`),
	} {
		r := avro.NewReader(nil, 0).Reset([]byte{1}) // Zig-zag encoding of -1.
		value := r.ReadNext(schema)
		assert.Error(t, r.Error)
		assert.Nil(t, value)
	}
}

func TestReadNextLocalTimestampCivil(t *testing.T) {
	old := time.Local
	rome, err := time.LoadLocation("Europe/Rome")
	require.NoError(t, err)
	time.Local = rome
	t.Cleanup(func() { time.Local = old })
	for _, unit := range []time.Duration{time.Millisecond, time.Microsecond} {
		name := "local-timestamp-millis"
		if unit == time.Microsecond {
			name = "local-timestamp-micros"
		}
		schema := avro.MustParse(`{"type":"long","logicalType":"` + name + `"}`)
		for _, hour := range []int{1, 2, 3} {
			want := time.Date(2020, 3, 29, hour, 30, 0, 0, time.UTC)
			units := want.Unix() * int64(time.Second/unit)
			wire, err := avro.Marshal(avro.MustParse(`"long"`), units)
			require.NoError(t, err)
			r := avro.NewReader(nil, 0).Reset(wire)
			assert.Equal(t, want, r.ReadNext(schema))
			assert.NoError(t, r.Error)
		}
	}
}

func TestReader_ReadNext(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		schema  string
		want    any
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "Null",
			data:    []byte{},
			schema:  "null",
			want:    nil,
			wantErr: require.NoError,
		},
		{
			name:    "Bool",
			data:    []byte{0x01},
			schema:  "boolean",
			want:    true,
			wantErr: require.NoError,
		},
		{
			name:    "Int",
			data:    []byte{0x36},
			schema:  "int",
			want:    27,
			wantErr: require.NoError,
		},
		{
			name:    "Int Date",
			data:    []byte{0xAE, 0x9D, 0x02},
			schema:  `{"type":"int","logicalType":"date"}`,
			want:    time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
			wantErr: require.NoError,
		},
		{
			name:    "Int Time-Millis",
			data:    []byte{0x9C, 0x85, 0xE3, 0x0B},
			schema:  `{"type":"int","logicalType":"time-millis"}`,
			want:    12345678 * time.Millisecond,
			wantErr: require.NoError,
		},
		{
			name:    "Long",
			data:    []byte{0x36},
			schema:  "long",
			want:    int64(27),
			wantErr: require.NoError,
		},
		{
			name:    "Long Time-Micros",
			data:    []byte{0xD6, 0xE4, 0xE0, 0xFD, 0x5B},
			schema:  `{"type":"long","logicalType":"time-micros"}`,
			want:    12345678123 * time.Microsecond,
			wantErr: require.NoError,
		},
		{
			name:    "Long Timestamp-Millis",
			data:    []byte{0x90, 0xB2, 0xAE, 0xC3, 0xEC, 0x5B},
			schema:  `{"type":"long","logicalType":"timestamp-millis"}`,
			want:    time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
			wantErr: require.NoError,
		},
		{
			name:    "Long Timestamp-Micros",
			data:    []byte{0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05},
			schema:  `{"type":"long","logicalType":"timestamp-micros"}`,
			want:    time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
			wantErr: require.NoError,
		},
		{
			name:    "Long Local-Timestamp-Millis",
			data:    []byte{0x90, 0xB2, 0xAE, 0xC3, 0xEC, 0x5B},
			schema:  `{"type":"long","logicalType":"local-timestamp-millis"}`,
			want:    time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
			wantErr: require.NoError,
		},
		{
			name:    "Long Local-Timestamp-Micros",
			data:    []byte{0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05},
			schema:  `{"type":"long","logicalType":"local-timestamp-micros"}`,
			want:    time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
			wantErr: require.NoError,
		},
		{
			name:    "Float",
			data:    []byte{0x33, 0x33, 0x93, 0x3F},
			schema:  "float",
			want:    float32(1.15),
			wantErr: require.NoError,
		},
		{
			name:    "Double",
			data:    []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F},
			schema:  "double",
			want:    float64(1.15),
			wantErr: require.NoError,
		},
		{
			name:    "String",
			data:    []byte{0x06, 0x66, 0x6F, 0x6F},
			schema:  "string",
			want:    "foo",
			wantErr: require.NoError,
		},
		{
			name:    "Bytes",
			data:    []byte{0x08, 0xEC, 0xAB, 0x44, 0x00},
			schema:  "bytes",
			want:    []byte{0xEC, 0xAB, 0x44, 0x00},
			wantErr: require.NoError,
		},
		{
			name:    "Bytes Decimal",
			data:    []byte{0x6, 0x00, 0x87, 0x78},
			schema:  `{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}`,
			want:    big.NewRat(1734, 5),
			wantErr: require.NoError,
		},
		{
			name:    "Record",
			data:    []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema:  `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			want:    map[string]any{"a": int64(27), "b": "foo"},
			wantErr: require.NoError,
		},
		{
			name:    "Record Null Field",
			data:    []byte{0x36},
			schema:  `{"type":"record","name":"test","fields":[{"name":"a","type":"null"},{"name":"b","type":"int"}]}`,
			want:    map[string]any{"a": nil, "b": 27},
			wantErr: require.NoError,
		},
		{
			name:    "Ref",
			data:    []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema:  `{"type":"record","name":"parent","fields":[{"name":"a","type":{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}},{"name":"b","type":"test"}]}`,
			want:    map[string]any{"a": map[string]any{"a": int64(27), "b": "foo"}, "b": map[string]any{"a": int64(27), "b": "foo"}},
			wantErr: require.NoError,
		},
		{
			name:    "Array",
			data:    []byte{0x04, 0x36, 0x38, 0x0},
			schema:  `{"type":"array", "items": "int"}`,
			want:    []any{27, 28},
			wantErr: require.NoError,
		},
		{
			name:    "Map",
			data:    []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00},
			schema:  `{"type":"map", "values": "string"}`,
			want:    map[string]any{"foo": "foo"},
			wantErr: require.NoError,
		},
		{
			name:    "Enum",
			data:    []byte{0x02},
			schema:  `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`,
			want:    "bar",
			wantErr: require.NoError,
		},
		{
			name:    "Enum Invalid Symbol",
			data:    []byte{0x04},
			schema:  `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`,
			want:    nil,
			wantErr: require.Error,
		},
		{
			name:    "Union",
			data:    []byte{0x02, 0x06, 0x66, 0x6F, 0x6F},
			schema:  `["null", "string"]`,
			want:    map[string]any{"string": "foo"},
			wantErr: require.NoError,
		},
		{
			name:    "Union Nil",
			data:    []byte{0x00},
			schema:  `["null", "string"]`,
			want:    nil,
			wantErr: require.NoError,
		},
		{
			name:    "Union Named",
			data:    []byte{0x02, 0x02},
			schema:  `["null", {"type":"enum", "name": "test", "symbols": ["foo", "bar"]}]`,
			want:    map[string]any{"test": "bar"},
			wantErr: require.NoError,
		},
		{
			name:    "Union Invalid Schema",
			data:    []byte{0x04},
			schema:  `["null", "string"]`,
			want:    nil,
			wantErr: require.Error,
		},
		{
			name:    "Fixed",
			data:    []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F},
			schema:  `{"type":"fixed", "name": "test", "size": 6}`,
			want:    [6]byte{'f', 'o', 'o', 'f', 'o', 'o'},
			wantErr: require.NoError,
		},
		{
			name:    "Fixed Decimal",
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78},
			schema:  `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":5,"scale":2}`,
			want:    big.NewRat(1734, 5),
			wantErr: require.NoError,
		},
		{
			name:    "Fixed Duration",
			data:    []byte{0x0c, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x52, 0xaa, 0x08, 0x00},
			schema:  `{"type":"fixed","name":"test","size":12,"logicalType":"duration"}`,
			want:    avro.LogicalDuration{Months: 12, Days: 34, Milliseconds: 567890},
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			schema := avro.MustParse(test.schema)
			r := avro.NewReader(bytes.NewReader(test.data), 10)

			got := r.ReadNext(schema)

			test.wantErr(t, r.Error)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestReader_ReadNextResolvedEnum(t *testing.T) {
	tests := []struct {
		name   string
		reader string
		writer string
		data   []byte
		want   string
	}{
		{
			name:   "writer symbol order",
			reader: `{"type":"enum","name":"test","symbols":["bar","foo"]}`,
			writer: `{"type":"enum","name":"test","symbols":["foo","bar"]}`,
			data:   []byte{0},
			want:   "foo",
		},
		{
			name:   "reader default",
			reader: `{"type":"enum","name":"test","symbols":["foo"],"default":"foo"}`,
			writer: `{"type":"enum","name":"test","symbols":["foo","bar"]}`,
			data:   []byte{2},
			want:   "foo",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := avro.NewSchemaCompatibility().Resolve(
				avro.MustParse(test.reader),
				avro.MustParse(test.writer),
			)
			require.NoError(t, err)

			r := avro.NewReader(bytes.NewReader(test.data), 10)
			got := r.ReadNext(resolved)

			require.NoError(t, r.Error)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestReader_ReadNextResolvedPrimitive(t *testing.T) {
	tests := []struct {
		name   string
		reader string
		writer string
		value  any
		want   any
	}{
		{name: "int to float", reader: `"float"`, writer: `"int"`, value: int32(27), want: float32(27)},
		{name: "long to double", reader: `"double"`, writer: `"long"`, value: int64(27), want: float64(27)},
		{name: "float to double", reader: `"double"`, writer: `"float"`, value: float32(1.5), want: float64(1.5)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := avro.MustParse(test.writer)
			resolved, err := avro.NewSchemaCompatibility().Resolve(avro.MustParse(test.reader), writer)
			require.NoError(t, err)
			data, err := avro.Marshal(writer, test.value)
			require.NoError(t, err)

			r := avro.NewReader(bytes.NewReader(data), 10)
			got := r.ReadNext(resolved)

			require.NoError(t, r.Error)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestReader_ReadNextResolvedRecord(t *testing.T) {
	tests := []struct {
		name   string
		reader string
		writer string
		value  map[string]any
		want   map[string]any
	}{
		{
			name:   "ignore writer field",
			reader: `{"type":"record","name":"test","fields":[{"name":"keep","type":"int"}]}`,
			writer: `{"type":"record","name":"test","fields":[{"name":"keep","type":"int"},{"name":"extra","type":"string"}]}`,
			value:  map[string]any{"keep": 7, "extra": "discard"},
			want:   map[string]any{"keep": 7},
		},
		{
			name:   "apply reader default",
			reader: `{"type":"record","name":"test","fields":[{"name":"keep","type":"int"},{"name":"added","type":"string","default":"fallback"}]}`,
			writer: `{"type":"record","name":"test","fields":[{"name":"keep","type":"int"}]}`,
			value:  map[string]any{"keep": 7},
			want:   map[string]any{"keep": 7, "added": "fallback"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := avro.MustParse(test.writer)
			resolved, err := avro.NewSchemaCompatibility().Resolve(avro.MustParse(test.reader), writer)
			require.NoError(t, err)
			data, err := avro.Marshal(writer, test.value)
			require.NoError(t, err)

			r := avro.NewReader(bytes.NewReader(data), 10)
			got := r.ReadNext(resolved)

			require.NoError(t, r.Error)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestReader_ReadNextUnsupportedType(t *testing.T) {
	schema := avro.NewPrimitiveSchema(avro.Type("test"), nil)
	r := avro.NewReader(bytes.NewReader([]byte{0x01}), 10)

	_ = r.ReadNext(schema)

	assert.Error(t, r.Error)
}

func TestReaderReadNextProgrammaticInvalidDuration(t *testing.T) {
	schema, err := avro.NewFixedSchema("test", "", 1, avro.NewPrimitiveLogicalSchema(avro.Duration))
	require.NoError(t, err)
	r := avro.NewReader(bytes.NewReader([]byte{0x2a}), 1)
	var got any

	assert.NotPanics(t, func() {
		got = r.ReadNext(schema)
	})

	require.NoError(t, r.Error)
	assert.Equal(t, [1]byte{0x2a}, got)
}

func TestReaderReadNextProgrammaticInvalidDecimal(t *testing.T) {
	fixedInvalid, err := avro.NewFixedSchema("invalid", "", 1, avro.NewDecimalLogicalSchema(3, 0))
	require.NoError(t, err)
	fixedCustom, err := avro.NewFixedSchema("custom", "", 1, customDecimalLogicalSchema{})
	require.NoError(t, err)
	tests := []struct {
		name   string
		schema avro.Schema
		data   []byte
		want   any
	}{
		{
			name:   "bytes invalid parameters",
			schema: avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(0, 0)),
			data:   []byte{0x02, 0x2a},
			want:   []byte{0x2a},
		},
		{
			name:   "bytes custom implementation",
			schema: avro.NewPrimitiveSchema(avro.Bytes, customDecimalLogicalSchema{}),
			data:   []byte{0x02, 0x2a},
			want:   []byte{0x2a},
		},
		{name: "fixed invalid parameters", schema: fixedInvalid, data: []byte{0x2a}, want: [1]byte{0x2a}},
		{name: "fixed custom implementation", schema: fixedCustom, data: []byte{0x2a}, want: [1]byte{0x2a}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := avro.NewReader(bytes.NewReader(test.data), 1)
			var got any

			assert.NotPanics(t, func() {
				got = r.ReadNext(test.schema)
			})

			require.NoError(t, r.Error)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestReader_ReadNextNilSchema(t *testing.T) {
	tests := []struct {
		name   string
		schema avro.Schema
	}{
		{name: "nil"},
		{name: "typed nil", schema: (*avro.PrimitiveSchema)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := avro.NewReader(bytes.NewReader(nil), 10)
			var got any

			assert.NotPanics(t, func() {
				got = r.ReadNext(test.schema)
			})

			assert.Nil(t, got)
			assert.ErrorContains(t, r.Error, "schema cannot be nil")
		})
	}
}

func TestReader_ReadArrayCallbackCanStop(t *testing.T) {
	var data bytes.Buffer
	writer := avro.NewWriter(&data, 16)
	writer.WriteLong(3)
	require.NoError(t, writer.Flush())

	reader := avro.NewReader(bytes.NewReader(data.Bytes()), 16)
	calls := 0
	reader.ReadArrayCB(func(*avro.Reader) bool {
		calls++
		return false
	})

	require.NoError(t, reader.Error)
	assert.Equal(t, 1, calls)
}

func TestReader_ReadArrayCallbackEnforcesBudget(t *testing.T) {
	var data bytes.Buffer
	writer := avro.NewWriter(&data, 16)
	writer.WriteLong(3)
	require.NoError(t, writer.Flush())

	config := avro.Config{MaxSliceAllocSize: 2}.Freeze()
	reader := avro.NewReader(bytes.NewReader(data.Bytes()), 16, avro.WithReaderConfig(config))
	calls := 0
	reader.ReadArrayCB(func(*avro.Reader) bool {
		calls++
		return true
	})

	assert.Zero(t, calls)
	assert.Error(t, reader.Error)
}

func TestReader_ReadNextRejectsNarrowedUnionIndex(t *testing.T) {
	var data bytes.Buffer
	writer := avro.NewWriter(&data, 16)
	writer.WriteLong(math.MaxInt64)
	require.NoError(t, writer.Flush())

	reader := avro.NewReader(bytes.NewReader(data.Bytes()), 16)
	value := reader.ReadNext(avro.MustParse(`["null","string"]`))

	assert.Nil(t, value)
	assert.Error(t, reader.Error)
}

func TestReader_ReadNextRejectsUnallocatableFixedSize(t *testing.T) {
	schema, err := avro.NewFixedSchema("huge", "", math.MaxInt, nil)
	require.NoError(t, err)
	reader := avro.NewReader(bytes.NewReader(nil), 16)
	var got any

	assert.NotPanics(t, func() {
		got = reader.ReadNext(schema)
	})
	assert.Nil(t, got)
	assert.Error(t, reader.Error)
}
