package avro

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/modern-go/reflect2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_UnmarshalExact(t *testing.T) {
	api := Config{MaxByteSliceSize: 2}.Freeze()
	exact, ok := api.(ExactUnmarshaler)
	require.True(t, ok)
	schema := MustParse(`"int"`)
	var got int
	require.NoError(t, api.Unmarshal(schema, []byte{2, 4}, &got), "ordinary Unmarshal retains its compatibility contract")
	require.Equal(t, 1, got)
	require.ErrorContains(t, exact.UnmarshalExact(schema, []byte{2, 4}, &got), "trailing bytes")
	err := exact.UnmarshalExact(schema, []byte{0x80}, &got)
	require.ErrorIs(t, err, io.EOF)
	require.NotEqual(t, io.EOF, err, "the wrapped decode error must not be discarded")
	require.Error(t, exact.UnmarshalExact(schema, []byte{2}, nil))
	require.NoError(t, exact.UnmarshalExact(schema, []byte{6}, &got))
	require.Equal(t, 3, got)

	var value []byte
	require.Error(t, exact.UnmarshalExact(MustParse(`"bytes"`), []byte{6, 1, 2, 3}, &value), "exact decoding must retain configured resource limits")
	var record map[string]any
	empty := MustParse(`{"type":"record","name":"EmptyExact","fields":[]}`)
	require.NoError(t, exact.UnmarshalExact(empty, nil, &record))
	require.Error(t, exact.UnmarshalExact(empty, []byte{0}, &record))
}

func TestConfig_Freeze(t *testing.T) {
	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	assert.Equal(t, "test", cfg.getTagKey())
	assert.Equal(t, 2, cfg.getBlockLength())
}

func TestConfig_ReturnReaderReleasesInput(t *testing.T) {
	cfg := Config{}.Freeze().(*frozenConfig).snapshot()
	reader := cfg.borrowReader(make([]byte, 1<<20))
	reader.head = 1
	reader.pendingErr = errors.New("pending")
	reader.Error = errors.New("read")

	cfg.returnReader(reader)

	assert.Zero(t, cap(reader.buf))
	assert.Zero(t, reader.head)
	assert.Zero(t, reader.tail)
	assert.NoError(t, reader.pendingErr)
	assert.NoError(t, reader.Error)
}

func TestConfig_ReusesDecoders(t *testing.T) {
	type testObj struct {
		A int64 `avro:"a"`
	}

	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"}
	]
}`)
	typ := reflect2.TypeOfPtr(&testObj{})

	dec1 := cfg.DecoderOf(schema, typ)
	dec2 := cfg.DecoderOf(schema, typ)

	assert.Same(t, dec1, dec2)
}

func TestConfig_ReusesDecoders_WithWriterFingerprint(t *testing.T) {
	type testObj struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	sch := `{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string", "default": "foo"}
		]
	}`
	typ := reflect2.TypeOfPtr(&testObj{})

	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema1 := MustParse(sch)
	schema2 := MustParse(sch)
	fp := [32]byte{1, 2, 3}
	schema2.(*RecordSchema).writerFingerprint = &fp

	dec1 := cfg.DecoderOf(schema1, typ)
	dec2 := cfg.DecoderOf(schema2, typ)

	assert.NotSame(t, dec1, dec2)
}

func TestConfig_ReusesDecoders_WithEnum(t *testing.T) {
	sch := `{
		"type": "enum",
		"name": "test.enum",
		"symbols": ["foo"],
		"default": "foo"
	}`
	typ := reflect2.TypeOfPtr(new(string))

	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema1 := MustParse(sch)
	schema2 := MustParse(sch)
	schema2.(*EnumSchema).encodedSymbols = []string{"foo", "bar"}
	fp := schema1.Fingerprint()
	schema2.(*EnumSchema).writerFingerprint = &fp

	dec1 := cfg.DecoderOf(schema1, typ)
	dec2 := cfg.DecoderOf(schema2, typ)

	assert.NotSame(t, dec1, dec2)
}

func TestConfig_DecoderCacheIncludesFieldAliases(t *testing.T) {
	first, err := ParseWithCache(`{
		"type": "record",
		"name": "test",
		"fields": [{"name": "current", "aliases": ["a"], "type": "int"}]
	}`, "", &SchemaCache{})
	require.NoError(t, err)
	second, err := ParseWithCache(`{
		"type": "record",
		"name": "test",
		"fields": [{"name": "current", "aliases": ["b"], "type": "int"}]
	}`, "", &SchemaCache{})
	require.NoError(t, err)

	type target struct {
		A int32 `avro:"a"`
		B int32 `avro:"b"`
	}
	api := Config{}.Freeze()
	var got target
	require.NoError(t, api.Unmarshal(first, []byte{0x02}, &got))
	assert.Equal(t, target{A: 1}, got)

	got = target{}
	require.NoError(t, api.Unmarshal(second, []byte{0x02}, &got))
	assert.Equal(t, target{B: 1}, got)
}

func TestConfig_CodecCacheIsolatesCustomProperties(t *testing.T) {
	first, err := ParseWithCache(`{
		"type": "record",
		"name": "test",
		"fields": [{"name": "v", "type": {"type": "int", "offset": 1}}]
	}`, "", &SchemaCache{})
	require.NoError(t, err)
	second, err := ParseWithCache(`{
		"type": "record",
		"name": "test",
		"fields": [{"name": "v", "type": {"type": "int", "offset": 2}}]
	}`, "", &SchemaCache{})
	require.NoError(t, err)

	offset := func(schema Schema) int {
		return int(schema.(PropertySchema).Prop("offset").(float64))
	}
	api := Config{}.Freeze()
	api.RegisterTypeConverters(TypeConversionFuncs{
		AvroType: Int,
		EncoderTypeConversion: func(in any, schema Schema) (any, error) {
			return int(in.(float64)) + offset(schema), nil
		},
		DecoderTypeConversion: func(in any, schema Schema) (any, error) {
			return in.(int) + offset(schema), nil
		},
	})

	encoded, err := api.Marshal(first, map[string]any{"v": float64(1)})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x04}, encoded)
	encoded, err = api.Marshal(second, map[string]any{"v": float64(1)})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x06}, encoded)

	var decoded map[string]any
	require.NoError(t, api.Unmarshal(first, []byte{0x02}, &decoded))
	assert.Equal(t, map[string]any{"v": 2}, decoded)
	decoded = nil
	require.NoError(t, api.Unmarshal(second, []byte{0x02}, &decoded))
	assert.Equal(t, map[string]any{"v": 3}, decoded)
}

func TestConfig_DisableCache_DoesNotReuseDecoders(t *testing.T) {
	type testObj struct {
		A int64 `avro:"a"`
	}

	api := Config{
		TagKey:         "test",
		BlockLength:    2,
		DisableCaching: true,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"}
	]
}`)
	typ := reflect2.TypeOfPtr(&testObj{})

	dec1 := cfg.DecoderOf(schema, typ)
	dec2 := cfg.DecoderOf(schema, typ)

	assert.NotSame(t, dec1, dec2)
}

func TestConfig_ReusesEncoders(t *testing.T) {
	type testObj struct {
		A int64 `avro:"a"`
	}

	api := Config{
		TagKey:      "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"}
	]
}`)
	typ := reflect2.TypeOfPtr(testObj{})

	enc1 := cfg.EncoderOf(schema, typ)
	enc2 := cfg.EncoderOf(schema, typ)

	assert.Same(t, enc1, enc2)
}

func TestConfig_DisableCache_DoesNotReuseEncoders(t *testing.T) {
	type testObj struct {
		A int64 `avro:"a"`
	}

	api := Config{
		TagKey:         "test",
		BlockLength:    2,
		DisableCaching: true,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"}
	]
}`)
	typ := reflect2.TypeOfPtr(testObj{})

	enc1 := cfg.EncoderOf(schema, typ)
	enc2 := cfg.EncoderOf(schema, typ)

	assert.NotSame(t, enc1, enc2)
}

func TestConfig_CodecLookupRejectsNilSchema(t *testing.T) {
	cfg := Config{}.Freeze()
	typ := reflect2.TypeOfPtr(new(int))
	tests := []struct {
		name   string
		schema Schema
	}{
		{name: "nil"},
		{name: "typed nil", schema: (*PrimitiveSchema)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name+" decoder", func(t *testing.T) {
			var decoder ValDecoder
			assert.NotPanics(t, func() {
				decoder = cfg.DecoderOf(test.schema, typ)
			})
			require.NotNil(t, decoder)

			reader := &Reader{}
			decoder.Decode(nil, reader)
			assert.ErrorContains(t, reader.Error, "schema cannot be nil")
		})

		t.Run(test.name+" encoder", func(t *testing.T) {
			var encoder ValEncoder
			assert.NotPanics(t, func() {
				encoder = cfg.EncoderOf(test.schema, typ)
			})
			require.NotNil(t, encoder)

			writer := &Writer{}
			encoder.Encode(nil, writer)
			assert.ErrorContains(t, writer.Error, "schema cannot be nil")
		})
	}
}

func TestConfig_DecoderOfRejectsInvalidType(t *testing.T) {
	cfg := Config{}.Freeze()
	schema := MustParse(`"int"`)
	tests := []struct {
		name string
		typ  reflect2.Type
	}{
		{name: "nil"},
		{name: "typed nil", typ: (*reflect2.UnsafePtrType)(nil)},
		{name: "non-pointer", typ: reflect2.TypeOf(0)},
		{name: "safe pointer", typ: reflect2.ConfigSafe.TypeOf(new(int))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var decoder ValDecoder
			assert.NotPanics(t, func() {
				decoder = cfg.DecoderOf(schema, test.typ)
			})
			require.NotNil(t, decoder)

			reader := &Reader{}
			decoder.Decode(nil, reader)
			assert.ErrorContains(t, reader.Error, "decoder type must")
		})
	}
}

func TestConfig_EncoderOfAcceptsTypedNilType(t *testing.T) {
	cfg := Config{}.Freeze()
	schema := MustParse(`"null"`)
	var typ *reflect2.UnsafePtrType
	var encoder ValEncoder

	assert.NotPanics(t, func() {
		encoder = cfg.EncoderOf(schema, typ)
	})
	require.NotNil(t, encoder)

	writer := &Writer{}
	encoder.Encode(nil, writer)
	assert.NoError(t, writer.Error)
}

func TestConfig_EncoderOfRejectsSafeType(t *testing.T) {
	cfg := Config{}.Freeze()
	schema := MustParse(`"int"`)
	var encoder ValEncoder

	assert.NotPanics(t, func() {
		encoder = cfg.EncoderOf(schema, reflect2.ConfigSafe.TypeOf(0))
	})
	require.NotNil(t, encoder)

	writer := &Writer{}
	encoder.Encode(nil, writer)
	assert.ErrorContains(t, writer.Error, "encoder type must support unsafe operations")
}

func TestTypeResolver_NameReturnsCopy(t *testing.T) {
	resolver := NewTypeResolver()
	typ := reflect2.TypeOf(int(0))

	names, err := resolver.Name(typ)
	require.NoError(t, err)
	names[0] = "corrupted"

	names, err = resolver.Name(typ)
	require.NoError(t, err)
	assert.Equal(t, []string{"int", "long"}, names)
}

func TestTypeResolver_LocalTimestampUnions(t *testing.T) {
	tests := []struct {
		logical LogicalType
		value   time.Time
	}{
		{
			logical: LocalTimestampMillis,
			value:   time.Date(1970, 1, 1, 0, 0, 0, int(time.Millisecond), time.Local),
		},
		{
			logical: LocalTimestampMicros,
			value:   time.Date(1970, 1, 1, 0, 0, 0, int(time.Microsecond), time.Local),
		},
	}

	for _, test := range tests {
		t.Run(string(test.logical), func(t *testing.T) {
			api := Config{}.Freeze()
			schema := MustParse(fmt.Sprintf(
				`["null", {"type":"long", "logicalType":%q}]`,
				test.logical,
			))

			data, err := api.Marshal(schema, test.value)
			require.NoError(t, err)
			assert.Equal(t, []byte{0x02, 0x02}, data)

			var got any
			err = api.Unmarshal(schema, data, &got)
			require.NoError(t, err)
			want := time.Date(1970, 1, 1, 0, 0, 0, test.value.Nanosecond(), time.UTC)
			assert.Equal(t, want, got)
		})
	}
}

func TestTypeResolver_RegisterIsIdempotent(t *testing.T) {
	type value struct{}

	resolver := NewTypeResolver()
	resolver.Register("value", value{})
	resolver.Register("value", value{})

	names, err := resolver.Name(reflect2.TypeOf(value{}))
	require.NoError(t, err)
	assert.Equal(t, []string{"value"}, names)
}

func TestTypeResolver_RegisterIgnoresNil(t *testing.T) {
	resolver := NewTypeResolver()

	assert.NotPanics(t, func() {
		resolver.Register("value", nil)
	})
	_, err := resolver.Type("value")
	assert.Error(t, err)
}

func TestTypeResolver_NameRejectsNil(t *testing.T) {
	resolver := NewTypeResolver()
	var names []string
	var err error

	assert.NotPanics(t, func() {
		names, err = resolver.Name(nil)
	})
	assert.Error(t, err)
	assert.Nil(t, names)
}

func TestTypeResolver_ConcurrentRegister(t *testing.T) {
	type value struct{}

	resolver := NewTypeResolver()
	const count = 128
	want := make([]string, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range count {
		want[i] = fmt.Sprintf("name-%03d", i)
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			<-start
			resolver.Register(name, value{})
		}(want[i])
	}
	close(start)
	wg.Wait()

	names, err := resolver.Name(reflect2.TypeOf(value{}))
	require.NoError(t, err)
	assert.ElementsMatch(t, want, names)
}

func TestRegistrationRefreshesCodecs(t *testing.T) {
	type item struct {
		Value int64 `avro:"value" json:"value" yaml:"value" xml:"value"`
	}
	schema := MustParse(`["null",{"type":"record","name":"Late","fields":[{"name":"value","type":"long"}]}]`)
	api := Config{}.Freeze()
	data, err := api.Marshal(schema, map[string]any{"Late": map[string]any{"value": int64(2)}})
	require.NoError(t, err)
	var before any
	require.NoError(t, api.Unmarshal(schema, data, &before))
	require.IsType(t, map[string]any{}, before)
	api.Register("Late", item{})
	var after any
	require.NoError(t, api.Unmarshal(schema, data, &after))
	require.Equal(t, item{2}, after)
}

func TestConvertersRefreshCodecs(t *testing.T) {
	api := Config{}.Freeze()
	schema := MustParse(`"string"`)
	data, err := api.Marshal(schema, "initial")
	require.NoError(t, err)
	var got any
	require.NoError(t, api.Unmarshal(schema, data, &got))
	api.RegisterTypeConverters(TypeConversionFuncs{
		AvroType:              String,
		DecoderTypeConversion: func(in any, schema Schema) (any, error) { return "converted", nil },
	})
	require.NoError(t, api.Unmarshal(schema, data, &got))
	require.Equal(t, "converted", got)
}

func TestRegistrationRefreshesStreams(t *testing.T) {
	type item struct {
		Value int64 `avro:"value" json:"value" yaml:"value" xml:"value"`
	}
	schema := MustParse(`["null",{"type":"record","name":"StreamItem","fields":[{"name":"value","type":"long"}]}]`)
	api := Config{}.Freeze()
	data, err := api.Marshal(schema, map[string]any{"StreamItem": map[string]any{"value": int64(2)}})
	require.NoError(t, err)
	dec := api.NewDecoder(schema, bytes.NewReader(append(append([]byte(nil), data...), data...)))
	var got any
	require.NoError(t, dec.Decode(&got))
	require.IsType(t, map[string]any{}, got)
	old := api.(*frozenConfig).snapshot()
	api.Register("StreamItem", item{})
	// Complete an older cache build after registration; it cannot replace the new codec.
	old.DecoderOf(schema, reflect2.TypeOf(&got))
	require.NoError(t, dec.Decode(&got))
	require.Equal(t, item{2}, got)
	require.NoError(t, api.Unmarshal(schema, data, &got))
	require.Equal(t, item{2}, got)
}

type reentrantConverter struct {
	TypeConversionFuncs
	metadata func()
}

func (c reentrantConverter) Type() Type {
	c.metadata()
	return c.TypeConversionFuncs.Type()
}

func TestConverterMetadataCanRegister(t *testing.T) {
	api := Config{}.Freeze()
	done := make(chan struct{})
	go func() {
		api.RegisterTypeConverters(reentrantConverter{
			TypeConversionFuncs: TypeConversionFuncs{AvroType: String},
			metadata:            func() { api.Register("FromConverter", int64(0)) },
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("converter metadata blocked registration")
	}
	_, err := api.TypeOf("FromConverter")
	require.NoError(t, err)
}

func TestConcurrentRegistrations(t *testing.T) {
	api := Config{}.Freeze()
	schema := MustParse(`"long"`)
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			api.Register(fmt.Sprintf("Concurrent%d", i), int64(0))
			data, err := api.Marshal(schema, int64(i))
			if err != nil {
				errs <- err
				return
			}
			var got int64
			if err = api.Unmarshal(schema, data, &got); err != nil {
				errs <- err
				return
			}
			if got != int64(i) {
				errs <- fmt.Errorf("decoded %d, want %d", got, i)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for i := range 16 {
		_, err := api.TypeOf(fmt.Sprintf("Concurrent%d", i))
		require.NoError(t, err)
	}
}

func TestRegistrationRefreshesCacheModes(t *testing.T) {
	type item struct {
		Value int64 `avro:"value" json:"value" yaml:"value" xml:"value"`
	}
	type replacement struct {
		Value int64 `avro:"value" json:"value" yaml:"value" xml:"value"`
	}
	schema := MustParse(`["null",{"type":"record","name":"LateMode","fields":[{"name":"value","type":"long"}]}]`)
	for _, disabled := range []bool{false, true} {
		for _, partial := range []bool{false, true} {
			for _, strict := range []bool{false, true} {
				t.Run(fmt.Sprintf("disabled=%v/partial=%v/strict=%v", disabled, partial, strict), func(t *testing.T) {
					api := Config{DisableCaching: disabled, PartialUnionTypeResolution: partial, UnionResolutionError: strict}.Freeze()
					_, err := api.Marshal(schema, item{4})
					require.Error(t, err)
					var got any
					_ = api.Unmarshal(schema, []byte{2, 8}, &got)
					api.Register("LateMode", item{})
					data, err := api.Marshal(schema, item{4})
					require.NoError(t, err)
					require.Equal(t, []byte{2, 8}, data)
					require.NoError(t, api.Unmarshal(schema, data, &got))
					require.Equal(t, item{4}, got)
					api.Register("LateMode", replacement{})
					require.NoError(t, api.Unmarshal(schema, data, &got))
					require.Equal(t, replacement{4}, got)
				})
			}
		}
	}
}
