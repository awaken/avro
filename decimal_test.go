package avro_test

import (
	"math/big"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecimalExactScale(t *testing.T) {
	for _, text := range []string{
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		`{"type":"fixed","name":"Decimal","size":2,"logicalType":"decimal","precision":4,"scale":2}`,
	} {
		schema := avro.MustParse(text)
		for _, value := range []*big.Rat{big.NewRat(1, 3), big.NewRat(-1, 3), big.NewRat(101, 1000), big.NewRat(-101, 1000)} {
			before := new(big.Rat).Set(value)
			_, err := avro.Marshal(schema, value)
			assert.Error(t, err)
			assert.Equal(t, before, value)
			if schema.Type() == avro.Bytes {
				_, err = avro.Marshal(schema, *value)
				assert.Error(t, err)
				assert.Equal(t, before, value)
			}
		}
		for _, value := range []*big.Rat{big.NewRat(0, 1), big.NewRat(9999, 100), big.NewRat(-9999, 100), big.NewRat(1, 4), big.NewRat(-1, 4)} {
			before := new(big.Rat).Set(value)
			wire, err := avro.Marshal(schema, value)
			require.NoError(t, err)
			var got *big.Rat
			require.NoError(t, avro.Unmarshal(schema, wire, &got))
			assert.Equal(t, value, got)
			assert.Equal(t, before, value)
			if schema.Type() == avro.Bytes {
				byValue, err := avro.Marshal(schema, *value)
				require.NoError(t, err)
				assert.Equal(t, wire, byValue)
			}
		}
	}
}

func TestDecimalDecodePrecision(t *testing.T) {
	for _, text := range []string{
		`{"type":"bytes","logicalType":"decimal","precision":2,"scale":1}`,
		`{"type":"fixed","name":"Decimal","size":2,"logicalType":"decimal","precision":2,"scale":1}`,
	} {
		schema := avro.MustParse(text)
		for _, payload := range [][]byte{{0, 100}, {0xff, 0x9c}, {0, 123}} {
			wire := payload
			if schema.Type() == avro.Bytes {
				var err error
				wire, err = avro.Marshal(avro.MustParse(`"bytes"`), payload)
				require.NoError(t, err)
			}
			value := big.NewRat(7, 1)
			got := value
			assert.Error(t, avro.Unmarshal(schema, wire, &got))
			assert.Same(t, value, got)
			assert.Equal(t, big.NewRat(7, 1), got)
			if schema.Type() == avro.Bytes {
				var byValue big.Rat
				byValue.SetInt64(7)
				assert.Error(t, avro.Unmarshal(schema, wire, &byValue))
				assert.Equal(t, big.NewRat(7, 1), &byValue)
			}
			var generic any
			assert.Error(t, avro.Unmarshal(schema, wire, &generic))
			assert.Nil(t, generic)
			r := avro.NewReader(nil, 0).Reset(wire)
			assert.Nil(t, r.ReadNext(schema))
			assert.Error(t, r.Error)
		}
		for _, value := range []*big.Rat{big.NewRat(0, 1), big.NewRat(99, 10), big.NewRat(-99, 10)} {
			wire, err := avro.Marshal(schema, value)
			require.NoError(t, err)
			var got *big.Rat
			require.NoError(t, avro.Unmarshal(schema, wire, &got))
			assert.Equal(t, value, got)
		}
	}
}

func TestDecimalScalingLimit(t *testing.T) {
	schema := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":1001,"scale":1000}`)
	api := avro.Config{MaxByteSliceSize: 2}.Freeze()
	_, err := api.Marshal(schema, big.NewRat(1, 1))
	assert.ErrorContains(t, err, "MaxByteSliceSize")
	value := big.NewRat(7, 1)
	got := value
	assert.ErrorContains(t, api.Unmarshal(schema, []byte{2, 1}, &got), "MaxByteSliceSize")
	assert.Same(t, value, got)
	r := avro.NewReader(nil, 0, avro.WithReaderConfig(api)).Reset([]byte{2, 1})
	assert.Nil(t, r.ReadNext(schema))
	assert.ErrorContains(t, r.Error, "MaxByteSliceSize")

	// Explicit opt-out remains available for trusted larger scales.
	api = avro.Config{MaxByteSliceSize: -1}.Freeze()
	wire, err := api.Marshal(schema, big.NewRat(1, 1))
	require.NoError(t, err)
	var decoded *big.Rat
	require.NoError(t, api.Unmarshal(schema, wire, &decoded))
	assert.Equal(t, big.NewRat(1, 1), decoded)
}

func TestDecimalReadFailurePreservesValue(t *testing.T) {
	for _, text := range []string{
		`{"type":"bytes","logicalType":"decimal","precision":2}`,
		`{"type":"fixed","name":"Decimal","size":2,"logicalType":"decimal","precision":2}`,
	} {
		schema := avro.MustParse(text)
		wire := []byte{0}
		if schema.Type() == avro.Bytes {
			wire = []byte{4, 0} // Declares two bytes but supplies one.
		}
		value := big.NewRat(7, 1)
		got := value
		assert.Error(t, avro.Unmarshal(schema, wire, &got))
		assert.Same(t, value, got)
	}
}

func TestDecimalRawPrecision(t *testing.T) {
	bytesSchema := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":2}`)
	fixedSchema := avro.MustParse(`{"type":"fixed","name":"Decimal","size":2,"logicalType":"decimal","precision":2}`)
	uintSchema := avro.MustParse(`{"type":"fixed","name":"Decimal64","size":8,"logicalType":"decimal","precision":2}`)
	_, err := avro.Marshal(bytesSchema, []byte{100})
	assert.Error(t, err)
	_, err = avro.Marshal(fixedSchema, [2]byte{0, 100})
	assert.Error(t, err)
	_, err = avro.Marshal(uintSchema, uint64(100))
	assert.Error(t, err)

	gotBytes := []byte{7}
	assert.Error(t, avro.Unmarshal(bytesSchema, []byte{2, 100}, &gotBytes))
	assert.Equal(t, []byte{7}, gotBytes)
	gotFixed := [2]byte{7, 7}
	assert.Error(t, avro.Unmarshal(fixedSchema, []byte{0, 100}, &gotFixed))
	assert.Equal(t, [2]byte{7, 7}, gotFixed)
	gotUint := uint64(7)
	assert.Error(t, avro.Unmarshal(uintSchema, []byte{0, 0, 0, 0, 0, 0, 0, 100}, &gotUint))
	assert.Equal(t, uint64(7), gotUint)

	for _, schema := range []avro.Schema{bytesSchema, fixedSchema, uintSchema} {
		wire, err := avro.Marshal(schema, big.NewRat(99, 1))
		require.NoError(t, err)
		switch schema {
		case bytesSchema:
			require.NoError(t, avro.Unmarshal(schema, wire, &gotBytes))
			assert.Equal(t, []byte{99}, gotBytes)
		case fixedSchema:
			require.NoError(t, avro.Unmarshal(schema, wire, &gotFixed))
			assert.Equal(t, [2]byte{0, 99}, gotFixed)
		case uintSchema:
			require.NoError(t, avro.Unmarshal(schema, wire, &gotUint))
			assert.Equal(t, uint64(99), gotUint)
		}
	}
}

func TestDecimalSignedByteBoundary(t *testing.T) {
	schema := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":3}`)
	api := avro.Config{MaxByteSliceSize: 1}.Freeze()
	for _, value := range []int64{-128, -127, -1, 0, 127} {
		wire, err := api.Marshal(schema, big.NewRat(value, 1))
		require.NoError(t, err)
		assert.Len(t, wire, 2) // One byte of length and one byte of signed data.
		var got *big.Rat
		require.NoError(t, api.Unmarshal(schema, wire, &got))
		assert.Equal(t, big.NewRat(value, 1), got)
	}
	_, err := api.Marshal(schema, big.NewRat(128, 1))
	assert.ErrorContains(t, err, "MaxByteSliceSize")
}
