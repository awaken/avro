package avro

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestName_NameAndNamespace(t *testing.T) {
	n, err := newName("bar", "foo", nil)
	require.NoError(t, err)

	assert.Equal(t, "bar", n.Name())
	assert.Equal(t, "foo", n.Namespace())
	assert.Equal(t, "foo.bar", n.FullName())
}

func TestName_QualifiedName(t *testing.T) {
	n, err := newName("foo.bar", "test", nil)
	require.NoError(t, err)

	assert.Equal(t, "bar", n.Name())
	assert.Equal(t, "foo", n.Namespace())
	assert.Equal(t, "foo.bar", n.FullName())
}

func TestName_NameAndNamespaceAndAlias(t *testing.T) {
	n, err := newName("bar", "foo", []string{"baz", "test.bat"})
	require.NoError(t, err)

	assert.Equal(t, "bar", n.Name())
	assert.Equal(t, "foo", n.Namespace())
	assert.Equal(t, "foo.bar", n.FullName())
	assert.Equal(t, []string{"foo.baz", "test.bat"}, n.Aliases())
}

func TestName_EmpryName(t *testing.T) {
	_, err := newName("", "foo", nil)

	assert.Error(t, err)
}

func TestName_InvalidNameFirstChar(t *testing.T) {
	_, err := newName("+bar", "foo", nil)

	assert.Error(t, err)
}

func TestName_InvalidNameOtherChar(t *testing.T) {
	_, err := newName("bar+", "foo", nil)

	assert.Error(t, err)
}

func TestName_InvalidNamespaceFirstChar(t *testing.T) {
	_, err := newName("bar", "+foo", nil)

	assert.EqualError(t, err, `avro: invalid name part "+foo" in name "+foo.bar": invalid name +foo`)
}

func TestName_InvalidNamespaceOtherChar(t *testing.T) {
	_, err := newName("bar", "foo+", nil)

	assert.Error(t, err)
}

func TestName_HistoricalAliasFirstChar(t *testing.T) {
	name, err := newName("bar", "foo", []string{"+bar"})

	require.NoError(t, err)
	assert.Equal(t, []string{"foo.+bar"}, name.Aliases())
}

func TestName_HistoricalAliasOtherChar(t *testing.T) {
	name, err := newName("bar", "foo", []string{"bar+"})

	require.NoError(t, err)
	assert.Equal(t, []string{"foo.bar+"}, name.Aliases())
}

func TestName_HistoricalAliasFQNFirstChar(t *testing.T) {
	name, err := newName("bar", "foo", []string{"test.+bar"})

	require.NoError(t, err)
	assert.Equal(t, []string{"test.+bar"}, name.Aliases())
}

func TestName_HistoricalAliasFQNOtherChar(t *testing.T) {
	name, err := newName("bar", "foo", []string{"test.bar+"})

	require.NoError(t, err)
	assert.Equal(t, []string{"test.bar+"}, name.Aliases())
}

func TestProperties_PropGetsFromEmptySet(t *testing.T) {
	p := properties{}

	assert.Nil(t, p.Prop("test"))
}

func TestName_InvalidNameFirstCharButValidationSkipped(t *testing.T) {
	SkipNameValidation = true
	t.Cleanup(func() {
		SkipNameValidation = false
	})

	_, err := newName("+bar", "foo", nil)
	assert.NoError(t, err)
}

func TestIsValidDefault(t *testing.T) {
	tests := []struct {
		name     string
		schemaFn func() Schema
		def      any
		want     any
		wantOk   bool
	}{
		{
			name: "Null",
			schemaFn: func() Schema {
				return &NullSchema{}
			},
			def:    nil,
			want:   nullDefault,
			wantOk: true,
		},
		{
			name: "Null Invalid Type",
			schemaFn: func() Schema {
				return &NullSchema{}
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "String",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(String, nil)
			},
			def:    "test",
			want:   "test",
			wantOk: true,
		},
		{
			name: "String Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(String, nil)
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Bytes",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Bytes, nil)
			},
			def:    "test",
			want:   []byte("test"),
			wantOk: true,
		},
		{
			name: "Bytes Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Bytes, nil)
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Enum",
			schemaFn: func() Schema {
				s, _ := NewEnumSchema("foo", "", []string{"BAR"})
				return s
			},
			def:    "BAR",
			want:   "BAR",
			wantOk: true,
		},
		{
			name: "Enum Invalid Default",
			schemaFn: func() Schema {
				s, _ := NewEnumSchema("foo", "", []string{"BAR"})
				return s
			},
			def:    "BUP",
			wantOk: false,
		},
		{
			name: "Enum Empty string",
			schemaFn: func() Schema {
				s, _ := NewEnumSchema("foo", "", []string{"BAR"})
				return s
			},
			def:    "",
			wantOk: false,
		},
		{
			name: "Enum Invalid Type",
			schemaFn: func() Schema {
				s, _ := NewEnumSchema("foo", "", []string{"BAR"})
				return s
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Fixed",
			schemaFn: func() Schema {
				s, _ := NewFixedSchema("foo", "", 4, nil)
				return s
			},
			def:    "test",
			want:   [4]byte{'t', 'e', 's', 't'},
			wantOk: true,
		},
		{
			name: "Fixed Invalid Type",
			schemaFn: func() Schema {
				s, _ := NewFixedSchema("foo", "", 1, nil)
				return s
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Fixed Too Short",
			schemaFn: func() Schema {
				s, _ := NewFixedSchema("foo", "", 4, nil)
				return s
			},
			def:    "abc",
			wantOk: false,
		},
		{
			name: "Fixed Too Long",
			schemaFn: func() Schema {
				s, _ := NewFixedSchema("foo", "", 4, nil)
				return s
			},
			def:    "abcde",
			wantOk: false,
		},
		{
			name: "Boolean",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Boolean, nil)
			},
			def:    true,
			want:   true,
			wantOk: true,
		},
		{
			name: "Boolean Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Boolean, nil)
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Int",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    1,
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Int8",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    int8(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Int16",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    int16(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Int32",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    int32(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Float64",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    float64(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "Long",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Long, nil)
			},
			def:    int64(1),
			want:   int64(1),
			wantOk: true,
		},
		{
			name: "Long Float64",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Long, nil)
			},
			def:    float64(1),
			want:   int64(1),
			wantOk: true,
		},
		{
			name: "Long Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Long, nil)
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "Float",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Float, nil)
			},
			def:    float32(1),
			want:   float32(1),
			wantOk: true,
		},
		{
			name: "Float Float64",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Float, nil)
			},
			def:    float64(1),
			want:   float32(1),
			wantOk: true,
		},
		{
			name: "Float Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Float, nil)
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "Double",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Double, nil)
			},
			def:    float64(1),
			want:   float64(1),
			wantOk: true,
		},
		{
			name: "Double Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Double, nil)
			},
			def:    "test",
			wantOk: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, ok := isValidDefault(test.schemaFn(), test.def)

			assert.Equal(t, test.wantOk, ok)
			if ok {
				assert.Equal(t, test.want, got)
			}
		})
	}
}

func TestValidateDefault_ErrorReportsOriginalValue(t *testing.T) {
	union, err := NewUnionSchema([]Schema{NewNullSchema(), NewPrimitiveSchema(String, nil)})
	require.NoError(t, err)

	_, err = validateDefault("v", union, true)
	assert.EqualError(t, err, "avro: invalid default for field v. true not a union")
}

func TestSchema_FingerprintUsingCaches(t *testing.T) {
	schema := NewPrimitiveSchema(String, nil)

	want, _ := schema.FingerprintUsing(CRC64Avro)

	got, _ := schema.FingerprintUsing(CRC64Avro)

	value, ok := schema.fingerprinter.cache.Load(CRC64Avro)
	require.True(t, ok)
	assert.Equal(t, want, value)
	assert.Equal(t, want, got)
}

func TestSchema_FingerprintUsingReturnsCopy(t *testing.T) {
	schema := NewPrimitiveSchema(String, nil)

	fingerprint, err := schema.FingerprintUsing(CRC64Avro)
	require.NoError(t, err)
	want := append([]byte(nil), fingerprint...)
	fingerprint[0] ^= 0xff

	got, err := schema.FingerprintUsing(CRC64Avro)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestRecordSchema_CacheFingerprintIncludesDefaultPositions(t *testing.T) {
	first, err := ParseWithCache(`{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":1},{"name":"b","type":"int"}]}`, "", &SchemaCache{})
	require.NoError(t, err)
	second, err := ParseWithCache(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int","default":1}]}`, "", &SchemaCache{})
	require.NoError(t, err)

	assert.NotEqual(t, first.CacheFingerprint(), second.CacheFingerprint())

	type onlyB struct {
		B int `avro:"b"`
	}
	api := Config{}.Freeze()
	_, err = api.Marshal(first, onlyB{B: 2})
	require.NoError(t, err)
	_, err = api.Marshal(second, onlyB{B: 2})
	assert.EqualError(t, err, `avro: record R is missing required field "a"`)
}

func TestNewRecordSchema_RejectsInvalidFields(t *testing.T) {
	field, err := NewField("a", NewPrimitiveSchema(Int, nil))
	require.NoError(t, err)

	_, err = NewRecordSchema("R", "", []*Field{field, field})
	assert.Error(t, err)

	_, err = NewRecordSchema("R", "", []*Field{nil})
	assert.Error(t, err)
}

func TestSchemaConstructorsRejectNilMembers(t *testing.T) {
	var typedNil *PrimitiveSchema
	for _, schema := range []Schema{nil, typedNil} {
		field, err := NewField("value", schema)
		assert.Nil(t, field)
		assert.EqualError(t, err, "avro: field type cannot be nil")

		var union *UnionSchema
		assert.NotPanics(t, func() {
			union, err = NewUnionSchema([]Schema{schema})
		})
		assert.Nil(t, union)
		assert.EqualError(t, err, "avro: union type cannot contain nil")
	}
}

func TestSchemaConstructorsTreatTypedNilLogicalAsAbsent(t *testing.T) {
	var logical *PrimitiveLogicalSchema

	t.Run("primitive", func(t *testing.T) {
		var primitive *PrimitiveSchema
		assert.NotPanics(t, func() {
			primitive = NewPrimitiveSchema(Int, logical)
		})
		require.NotNil(t, primitive)
		assert.Nil(t, primitive.Logical())
	})

	t.Run("fixed", func(t *testing.T) {
		var fixed *FixedSchema
		var err error
		assert.NotPanics(t, func() {
			fixed, err = NewFixedSchema("F", "", 1, logical)
		})
		require.NoError(t, err)
		require.NotNil(t, fixed)
		assert.Nil(t, fixed.Logical())
	})
}

func TestSchema_CollectionsAndDefaultsAreSnapshots(t *testing.T) {
	aliases := []string{"OldR"}
	fieldAliases := []string{"old_a"}
	defaultValue := map[string]any{"x": float64(1)}
	field, err := NewField("a", NewMapSchema(NewPrimitiveSchema(Int, nil)), WithAliases(fieldAliases), WithDefault(defaultValue))
	require.NoError(t, err)
	fields := []*Field{field}
	record, err := NewRecordSchema("R", "", fields, WithAliases(aliases))
	require.NoError(t, err)

	aliases[0] = "Changed"
	fieldAliases[0] = "changed"
	defaultValue["x"] = float64(2)
	fields[0], err = NewField("b", NewPrimitiveSchema(Int, nil))
	require.NoError(t, err)
	assert.Equal(t, []string{"OldR"}, record.Aliases())
	assert.Equal(t, []string{"old_a"}, record.Fields()[0].Aliases())
	assert.Equal(t, map[string]any{"x": 1}, record.Fields()[0].Default())
	assert.Equal(t, "a", record.Fields()[0].Name())

	record.Aliases()[0] = "Mutated"
	record.Fields()[0] = fields[0]
	record.Fields()[0].Aliases()[0] = "mutated"
	returnedDefault := record.Fields()[0].Default().(map[string]any)
	returnedDefault["x"] = 3
	assert.Equal(t, []string{"OldR"}, record.Aliases())
	assert.Equal(t, []string{"old_a"}, record.Fields()[0].Aliases())
	assert.Equal(t, map[string]any{"x": 1}, record.Fields()[0].Default())
	assert.Equal(t, "a", record.Fields()[0].Name())

	symbols := []string{"A", "B"}
	enum, err := NewEnumSchema("E", "", symbols)
	require.NoError(t, err)
	symbols[0] = "C"
	enum.Symbols()[0] = "D"
	assert.Equal(t, []string{"A", "B"}, enum.Symbols())

	types := []Schema{NewNullSchema(), NewPrimitiveSchema(String, nil)}
	union, err := NewUnionSchema(types)
	require.NoError(t, err)
	types[0] = NewPrimitiveSchema(Int, nil)
	union.Types()[0] = NewPrimitiveSchema(Long, nil)
	assert.Equal(t, Null, union.Types()[0].Type())
}

func TestSchema_IsPromotable(t *testing.T) {
	tests := []struct {
		writerTyp  Type
		readerType Type
		want       bool
	}{
		{
			writerTyp:  Int,
			readerType: Long,
			want:       true,
		},
		{
			writerTyp:  Int,
			readerType: Float,
			want:       true,
		},
		{
			writerTyp:  Int,
			readerType: Double,
			want:       true,
		},
		{
			writerTyp:  Long,
			readerType: Float,
			want:       true,
		},
		{
			writerTyp:  Long,
			readerType: Double,
			want:       true,
		},
		{
			writerTyp:  Float,
			readerType: Double,
			want:       true,
		},
		{
			writerTyp:  String,
			readerType: Bytes,
			want:       true,
		},
		{
			writerTyp:  Bytes,
			readerType: String,
			want:       true,
		},
		{
			writerTyp:  Double,
			readerType: Int,
			want:       false,
		},
		{
			writerTyp:  Boolean,
			readerType: Int,
			want:       false,
		},
		{
			writerTyp:  Null,
			readerType: Null,
			want:       false,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			ok := isPromotable(test.writerTyp, test.readerType)

			assert.Equal(t, test.want, ok)
		})
	}
}

func TestSchema_IsNative(t *testing.T) {
	tests := []struct {
		typ    Type
		wantOk bool
	}{
		{
			typ:    Null,
			wantOk: true,
		},
		{
			typ:    Boolean,
			wantOk: true,
		},
		{
			typ:    Int,
			wantOk: true,
		},
		{
			typ:    Long,
			wantOk: true,
		},

		{
			typ:    Float,
			wantOk: true,
		},
		{
			typ:    Double,
			wantOk: true,
		},

		{
			typ:    Bytes,
			wantOk: true,
		},
		{
			typ:    String,
			wantOk: true,
		},
		{
			typ:    Record,
			wantOk: false,
		},
		{
			typ:    Array,
			wantOk: false,
		},
		{
			typ:    Map,
			wantOk: false,
		},
		{
			typ:    Fixed,
			wantOk: false,
		},
		{
			typ:    Enum,
			wantOk: false,
		},
		{
			typ:    Union,
			wantOk: false,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			ok := isNative(test.typ)
			assert.Equal(t, test.wantOk, ok)
		})
	}
}

func TestSchema_FieldEncodeDefault(t *testing.T) {
	schema := MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string", "default": "bar"},
			{"name": "b", "type": "boolean"}
		]
	}`).(*RecordSchema)

	fooEncoder := func(a any) ([]byte, error) {
		return []byte("foo"), nil
	}
	barEncoder := func(a any) ([]byte, error) {
		return []byte("bar"), nil
	}

	assert.Equal(t, nil, schema.fields[0].encodedDef.Load())

	_, err := schema.fields[0].encodeDefault(nil)
	assert.Error(t, err)

	_, err = schema.fields[1].encodeDefault(fooEncoder)
	assert.Error(t, err)

	def, err := schema.fields[0].encodeDefault(fooEncoder)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), def)

	def, err = schema.fields[0].encodeDefault(barEncoder)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), def)
}

func TestEnumSchema_GetSymbol(t *testing.T) {
	tests := []struct {
		schemaFn func() *EnumSchema
		idx      int
		want     any
		wantOk   bool
	}{
		{
			schemaFn: func() *EnumSchema {
				enum, _ := NewEnumSchema("foo", "", []string{"BAR"})
				return enum
			},
			idx:    0,
			wantOk: true,
			want:   "BAR",
		},
		{
			schemaFn: func() *EnumSchema {
				enum, _ := NewEnumSchema("foo", "", []string{"BAR"})
				return enum
			},
			idx:    1,
			wantOk: false,
		},
		{
			schemaFn: func() *EnumSchema {
				enum, _ := NewEnumSchema("foo", "", []string{"FOO"}, WithDefault("FOO"))
				return enum
			},
			idx:    1,
			wantOk: false,
		},
		{
			schemaFn: func() *EnumSchema {
				enum, _ := NewEnumSchema("foo", "", []string{"FOO"})
				enum.encodedSymbols = []string{"FOO", "BAR"}
				return enum
			},
			idx:    1,
			wantOk: false,
		},
		{
			schemaFn: func() *EnumSchema {
				enum, _ := NewEnumSchema("foo", "", []string{"FOO"}, WithDefault("FOO"))
				enum.encodedSymbols = []string{"FOO", "BAR"}
				return enum
			},
			idx:    1,
			wantOk: true,
			want:   "FOO",
		},
		{
			schemaFn: func() *EnumSchema {
				enum, _ := NewEnumSchema("foo", "", []string{"FOO", "BAR"})
				enum.encodedSymbols = []string{"FOO"}
				return enum
			},
			idx:    0,
			wantOk: true,
			want:   "FOO",
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			got, ok := test.schemaFn().Symbol(test.idx)
			assert.Equal(t, test.wantOk, ok)
			if ok {
				assert.Equal(t, test.want, got)
			}
		})
	}
}

func TestEnumSchema_RejectsInvalidDefaultOptions(t *testing.T) {
	_, err := NewEnumSchema("foo", "", []string{"BAR"}, WithDefault(""))
	assert.Error(t, err)

	_, err = NewEnumSchema("foo", "", []string{"BAR"}, WithDefault(1))
	assert.Error(t, err)
}
