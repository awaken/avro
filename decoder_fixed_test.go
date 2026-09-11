package avro_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"sync"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_FixedInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F}
	schema := `{"type":"fixed", "name": "test", "size": 6}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i [6]int
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Fixed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F}
	schema := `{"type":"fixed", "name": "test", "size": 6}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got [6]byte
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, [6]byte{'f', 'o', 'o', 'f', 'o', 'o'}, got)
}

func TestDecoder_FixedRat_Positive(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}
	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":5,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, big.NewRat(1734, 5), got)
}

func TestDecoder_FixedRat_Negative(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x78, 0x88}
	schema := `{"type":"fixed", "name": "test", "size": 6, "logicalType":"decimal","precision":5,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, big.NewRat(-1734, 5), got)
}

func TestDecoder_FixedRat_Zero(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":5,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, big.NewRat(0, 1), got)
}

func TestDecoder_FixedRatInvalidLogicalSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	schema := `{"type":"fixed", "name": "test", "size": 6}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_FixedLogicalDuration(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xc, 0x0, 0x0, 0x0, 0x22, 0x0, 0x0, 0x0, 0x52, 0xaa, 0x8, 0x0}
	schema := `{"name":"foo","type":"fixed","logicalType":"duration","size":12}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := avro.LogicalDuration{}
	err = dec.Decode(&got)
	require.NoError(t, err)

	assert.Equal(t, uint32(12), got.Months)
	assert.Equal(t, uint32(34), got.Days)
	assert.Equal(t, uint32(567890), got.Milliseconds)
}

func TestDecoder_FixedLogicalDurationSizeNot12(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xc, 0x0, 0x0, 0x0, 0x22, 0x0, 0x0, 0x0, 0x52, 0xaa, 0x8}
	schema := `{"name":"foo","type":"fixed","logicalType":"duration","size":11}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := avro.LogicalDuration{}
	err = dec.Decode(&got)
	assert.Error(t, err)
	assert.Equal(t, fmt.Errorf("avro: avro.LogicalDuration is unsupported for Avro fixed, size=11"), err)
}

func TestDecoderFixedProgrammaticInvalidDuration(t *testing.T) {
	schema, err := avro.NewFixedSchema("test", "", 1, avro.NewPrimitiveLogicalSchema(avro.Duration))
	require.NoError(t, err)

	t.Run("typed", func(t *testing.T) {
		dec := avro.NewDecoderForSchema(schema, bytes.NewReader(make([]byte, 12)))
		var got avro.LogicalDuration

		err := dec.Decode(&got)

		require.Error(t, err)
	})

	t.Run("interface", func(t *testing.T) {
		dec := avro.NewDecoderForSchema(schema, bytes.NewReader([]byte{0x2a}))
		var got any

		err := dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, [1]byte{0x2a}, got)
	})
}

func TestDecoder_FixedUint64_Full(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	schema := `{"type":"fixed", "name": "test", "size": 8}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got uint64
	err = dec.Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), got)
}

func TestDecoder_FixedUint64_Simple(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}
	schema := `{"type":"fixed", "name": "test", "size": 8}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got uint64
	err = dec.Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, uint64(256), got)
}

func TestDecoder_FixedUint64_Concurrent(t *testing.T) {
	schema := avro.MustParse(`{"type":"fixed", "name":"test", "size":8}`)
	api := avro.Config{}.Freeze()

	var warmup uint64
	err := api.Unmarshal(schema, make([]byte, 8), &warmup)
	require.NoError(t, err)

	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			for j := range 100 {
				expected := uint64(i+1)<<56 | uint64(j)
				data := make([]byte, 8)
				binary.BigEndian.PutUint64(data, expected)

				var got uint64
				err := api.Unmarshal(schema, data, &got)
				if !assert.NoError(t, err) {
					return
				}
				assert.Equal(t, expected, got)
			}
		}()
	}

	close(start)
	wg.Wait()
}

func TestDecoder_FixedInterfaceRejectsUnallocatableSize(t *testing.T) {
	schema, err := avro.NewFixedSchema("huge", "", math.MaxInt, nil)
	require.NoError(t, err)
	dec := avro.NewDecoderForSchema(schema, bytes.NewReader([]byte{0}))
	var got any

	assert.NotPanics(t, func() {
		err = dec.Decode(&got)
	})
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestDecoder_FixedDecimalRejectsUnallocatableSize(t *testing.T) {
	schema, err := avro.NewFixedSchema("huge", "", math.MaxInt, avro.NewDecimalLogicalSchema(1, 0))
	require.NoError(t, err)
	dec := avro.NewDecoderForSchema(schema, bytes.NewReader([]byte{0}))
	var got *big.Rat

	assert.NotPanics(t, func() {
		err = dec.Decode(&got)
	})
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestDecoderFixedProgrammaticInvalidDecimal(t *testing.T) {
	tests := []struct {
		name    string
		logical avro.LogicalSchema
	}{
		{name: "invalid parameters", logical: avro.NewDecimalLogicalSchema(3, 0)},
		{name: "custom implementation", logical: customDecimalLogicalSchema{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, err := avro.NewFixedSchema("test", "", 1, test.logical)
			require.NoError(t, err)

			var generic any
			assert.NotPanics(t, func() {
				err = avro.NewDecoderForSchema(schema, bytes.NewReader([]byte{0x2a})).Decode(&generic)
				require.NoError(t, err)
			})
			assert.Equal(t, [1]byte{0x2a}, generic)

			var typed *big.Rat
			assert.NotPanics(t, func() {
				err = avro.NewDecoderForSchema(schema, bytes.NewReader([]byte{0x2a})).Decode(&typed)
				require.Error(t, err)
			})
		})
	}
}
