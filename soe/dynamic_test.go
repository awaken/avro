package soe_test

import (
	"context"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/awaken/avro/v2/soe"
	"github.com/awaken/avro/v2/soe/internal/testdata"
	"github.com/awaken/avro/v2/soe/resolvers"
	"github.com/stretchr/testify/require"
)

type nilResolver struct {
	soe.SchemaResolver
}

func TestDynamicDecoder_CustomExactAPI(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	store := resolvers.NewMemorySchemaStore()
	require.NoError(t, store.AddSchema(schema))
	codec, err := soe.NewCodec(schema)
	require.NoError(t, err)
	frame, err := codec.Encode(12)
	require.NoError(t, err)
	legacy := soe.NewDynamicDecoderWithAPI(store, &nilAPI{API: avro.DefaultConfig})
	require.ErrorIs(t, legacy.Decode(context.Background(), frame, new(int)), soe.ErrExactAPI)
	api := &exactSOEWrapper{API: avro.DefaultConfig}
	decoder := soe.NewDynamicDecoderWithAPI(store, api)
	var got int
	require.NoError(t, decoder.Decode(context.Background(), frame, &got))
	require.Equal(t, 12, got)
	require.Equal(t, 1, api.calls)
	require.Error(t, decoder.Decode(context.Background(), append(frame, 0), &got))
}

func TestDynamicDecoder_RejectsTrailingObject(t *testing.T) {
	store := resolvers.NewMemorySchemaStore()
	for _, schema := range []avro.Schema{avro.MustParse(`"int"`), avro.MustParse(`"null"`)} {
		require.NoError(t, store.AddSchema(schema))
		var value any
		if schema.Type() == avro.Int {
			value = 23
		}
		codec, err := soe.NewCodec(schema)
		require.NoError(t, err)
		frame, err := codec.Encode(value)
		require.NoError(t, err)
		decoder := soe.NewDynamicDecoder(store)
		t.Run(string(schema.Type()), func(t *testing.T) {
			var got any = new(any)
			if schema.Type() == avro.Int {
				got = new(int)
			}
			require.NoError(t, decoder.Decode(context.Background(), frame, got))
			if schema.Type() == avro.Int {
				require.Equal(t, 23, *got.(*int))
			} else {
				require.Nil(t, *got.(*any))
			}
			require.Error(t, decoder.Decode(context.Background(), append(frame, 0), got))
		})
	}
}

func TestDynamicDecoder_DecodeRejectsNilResolver(t *testing.T) {
	data := encode(t, avro.MustParse("null"), nil)
	tests := []struct {
		name     string
		resolver soe.SchemaResolver
	}{
		{name: "nil", resolver: nil},
		{name: "typed nil", resolver: (*nilResolver)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := soe.NewDynamicDecoder(test.resolver)

			var err error
			require.NotPanics(t, func() {
				err = decoder.Decode(context.Background(), data, new(any))
			})
			require.ErrorContains(t, err, "resolver cannot be nil")
		})
	}
}

func TestDynamicDecoder_DecodeRejectsNilAPI(t *testing.T) {
	schema := avro.MustParse("null")
	data := encode(t, schema, nil)
	store := resolvers.NewMemorySchemaStore()
	require.NoError(t, store.AddSchema(schema))
	tests := []struct {
		name string
		api  avro.API
	}{
		{name: "nil", api: nil},
		{name: "typed nil", api: (*nilAPI)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := soe.NewDynamicDecoderWithAPI(store, test.api)

			var err error
			require.NotPanics(t, func() {
				err = decoder.Decode(context.Background(), data, new(any))
			})
			require.ErrorContains(t, err, "API cannot be nil")
		})
	}
}

// Helper function to marshal v into an SOE-framed binary Avro encoding.
func encode(t *testing.T, schema avro.Schema, v any) []byte {
	t.Helper()

	// Use a basic codec to marshal value.
	codec, err := soe.NewCodec(schema)
	require.NoError(t, err)

	data, err := codec.Encode(&v)
	require.NoError(t, err)
	return data
}

// Helper function to create a new DynamicDecoder with a registry over zero or
// more schemas.
func newDynamicDecoder(t *testing.T, schemas ...avro.Schema) *soe.DynamicDecoder {
	t.Helper()

	// Set up a store with provided schemas.
	store := resolvers.NewMemorySchemaStore()
	for _, schema := range schemas {
		err := store.AddSchema(schema)
		require.NoError(t, err)
	}
	return soe.NewDynamicDecoder(store)
}

func TestDynamicDecoder_DecodeUnknownSchema(t *testing.T) {
	// Marshal a value
	data := encode(t, testdata.Dynamic1Schema, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})

	// Set up a decoder with an empty registry.
	decoder := newDynamicDecoder(t)

	var v1 testdata.Dynamic1
	err := decoder.Decode(context.Background(), data, &v1)

	// Decode should fail with unknown schema error.
	require.ErrorIs(t, err, soe.ErrUnknownSchema)
}

func TestDynamicDecoder_DecodeWithOneSchema(t *testing.T) {
	schema := testdata.Dynamic1Schema

	// Marshal a value
	v0 := testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	}
	data := encode(t, schema, v0)

	// Set up a decoder with a registry containing the same schema.
	decoder := newDynamicDecoder(t, schema)

	var v1 testdata.Dynamic1
	err := decoder.Decode(context.Background(), data, &v1)

	require.NoError(t, err)
	require.Equal(t, v0, v1)
}

func TestDynamicDecoder_DecodeWithTwoSchemas(t *testing.T) {
	s1 := testdata.Dynamic1Schema
	s2 := testdata.Dynamic2Schema

	// Marshal two values of the different schemas
	data1 := encode(t, s1, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})
	data2 := encode(t, s2, testdata.Dynamic2{
		Key:     "ABC",
		Enabled: true,
	})

	// Set up a decoder with a registry containing the same two schemas.
	decoder := newDynamicDecoder(t, s1, s2)

	// Decode the payloads
	var v1 testdata.Dynamic1
	err1 := decoder.Decode(context.Background(), data1, &v1)

	var v2 testdata.Dynamic2
	err2 := decoder.Decode(context.Background(), data2, &v2)

	// Both payloads should be decodable since the registry knows about both
	// schemas.
	require.NoError(t, err1)
	require.Equal(t, "Bob", v1.Name)
	require.Equal(t, 16, v1.Age)

	require.NoError(t, err2)
	require.Equal(t, "ABC", v2.Key)
	require.Equal(t, true, v2.Enabled)
}

func TestDynamicDecoder_ValueMismatch(t *testing.T) {
	schema := testdata.Dynamic1Schema

	// Marshal a Dynamic1 value
	data := encode(t, schema, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})

	// Set up a decoder with a registry containing the schema.
	decoder := newDynamicDecoder(t, schema)

	// Attempt to decode into Dynamic2 type. Avro Unmarshal does best-effort
	// matching of fields to annotations, so this won't fail...
	var v2 testdata.Dynamic2
	err := decoder.Decode(context.Background(), data, &v2)

	// ... but we'll get an empty value.
	require.NoError(t, err)
	require.Equal(t, "", v2.Key)
	require.Equal(t, false, v2.Enabled)
}

func TestDynamicDecoder_DecodeBasePayloadWithExtendedType(t *testing.T) {
	// Marshal a Dynamic1 value
	data := encode(t, testdata.Dynamic1Schema, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})

	// Set up a decoder with a registry containing the schema of base type
	// Dynamic1.
	decoder := newDynamicDecoder(t, testdata.Dynamic1Schema)

	// Attempt to decode into Dynamic3 type.
	var v3 testdata.Dynamic3
	err := decoder.Decode(context.Background(), data, &v3)

	// The fields compatible with Dynamic1 should be populated.
	require.NoError(t, err)
	require.Equal(t, "Bob", v3.Name)
	require.Equal(t, 16, v3.Age)
	require.Equal(t, "", v3.Hobby)
}

func TestDynamicDecoder_DecodeExtendedPayloadWithBaseType(t *testing.T) {
	require.Equal(t, testdata.Dynamic1Schema.(avro.NamedSchema).FullName(), testdata.Dynamic3Schema.(avro.NamedSchema).FullName())

	// Marshal a Dynamic3 value
	data := encode(t, testdata.Dynamic3Schema, testdata.Dynamic3{
		Name:  "Bob",
		Age:   16,
		Hobby: "Dancing",
	})

	// Set up a decoder with a registry containing the schema of extended
	// type Dynamic3.
	decoder := newDynamicDecoder(t, testdata.Dynamic3Schema)

	// Attempt to decode into Dynamic1 type.
	var v1 testdata.Dynamic1
	err := decoder.Decode(context.Background(), data, &v1)

	// The fields compatible with Dynamic1 should be populated.
	require.NoError(t, err)
	require.Equal(t, "Bob", v1.Name)
	require.Equal(t, 16, v1.Age)
}

func TestDynamicDecoder_DecodeDynamicallyIntoMap(t *testing.T) {
	s1 := testdata.Dynamic1Schema
	s2 := testdata.Dynamic2Schema

	// Marshal two values of the different schemas
	data1 := encode(t, s1, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})
	data2 := encode(t, s2, testdata.Dynamic2{
		Key:     "ABC",
		Enabled: true,
	})

	// Set up a decoder with a registry containing the same two schemas.
	decoder := newDynamicDecoder(t, s1, s2)

	// Decode both payloads using different schemas into two values of the
	// same type map[string]any. This is an example of fully dynamic
	// decoding where distinct schemas/payloads are decoded into the same
	// Go type.
	var v1 map[string]any
	err1 := decoder.Decode(context.Background(), data1, &v1)

	var v2 map[string]any
	err2 := decoder.Decode(context.Background(), data2, &v2)

	// Check that the decoding succeeds and the expected values come out.
	require.NoError(t, err1)
	require.Equal(t, map[string]any{
		"name": "Bob",
		"age":  16,
	}, v1)

	require.NoError(t, err2)
	require.Equal(t, map[string]any{
		"key":     "ABC",
		"enabled": true,
	}, v2)
}
