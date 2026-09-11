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

func TestEncoder_FixedInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([6]int{})

	assert.Error(t, err)
}

func TestEncoder_Fixed(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([6]byte{'f', 'o', 'o', 'f', 'o', 'o'})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_FixedRat_Positive(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":5,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoder_FixedRat_Negative(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6, "logicalType":"decimal","precision":5,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(-1734, 5))

	require.NoError(t, err)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x78, 0x88}, buf.Bytes())
}

func TestEncoder_FixedRat_Zero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":5,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(0, 1))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, buf.Bytes())
}

func TestEncoder_FixedRat_Nil(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":5,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var rat *big.Rat
	assert.NotPanics(t, func() {
		err = enc.Encode(rat)
	})

	assert.ErrorContains(t, err, "cannot encode nil pointer")
	assert.Empty(t, buf.Bytes())
}

func TestEncoder_FixedRat_TooManyDigits(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":3,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	assert.ErrorContains(t, err, "avro: decimal exceeds precision=3, has 5 significant digits")
	assert.Empty(t, buf.Bytes())
}

func TestEncoder_FixedRatInvalidLogicalSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	assert.Error(t, err)
}

func TestEncoder_FixedLogicalDuration(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"name":"foo","type":"fixed","logicalType":"duration","size":12}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	duration := avro.LogicalDuration{Months: 12, Days: 34, Milliseconds: 567890}
	err = enc.Encode(duration)

	require.NoError(t, err)
	assert.Equal(t, []byte{0xc, 0x0, 0x0, 0x0, 0x22, 0x0, 0x0, 0x0, 0x52, 0xaa, 0x8, 0x0}, buf.Bytes())
}

func TestEncoder_FixedLogicalDurationSizeNot12(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"name":"foo","type":"fixed","logicalType":"duration","size":11}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	duration := avro.LogicalDuration{}
	err = enc.Encode(duration)
	assert.Error(t, err)
	assert.Equal(t, fmt.Errorf("avro: avro.LogicalDuration is unsupported for Avro fixed, size=11"), err)
}

func TestEncoderFixedProgrammaticInvalidDuration(t *testing.T) {
	schema, err := avro.NewFixedSchema("test", "", 1, avro.NewPrimitiveLogicalSchema(avro.Duration))
	require.NoError(t, err)
	var data bytes.Buffer

	err = avro.NewEncoderForSchema(schema, &data).Encode(avro.LogicalDuration{})

	require.Error(t, err)
	assert.Empty(t, data.Bytes())
}

func TestEncoder_FixedUint64_Full(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 8}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(uint64(math.MaxUint64))

	require.NoError(t, err)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, buf.Bytes())
}

func TestEncoder_FixedUint64_Small(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 8}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(uint64(256))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}, buf.Bytes())
}

func TestEncoder_FixedUint64_Concurrent(t *testing.T) {
	schema := avro.MustParse(`{"type":"fixed", "name":"test", "size":8}`)
	api := avro.Config{}.Freeze()

	_, err := api.Marshal(schema, uint64(0))
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
				value := uint64(i+1)<<56 | uint64(j)
				got, err := api.Marshal(schema, value)
				if !assert.NoError(t, err) {
					return
				}

				expected := make([]byte, 8)
				binary.BigEndian.PutUint64(expected, value)
				assert.Equal(t, expected, got)
			}
		}()
	}

	close(start)
	wg.Wait()
}

func TestEncoder_FixedDecimalRejectsUnallocatableSize(t *testing.T) {
	schema, err := avro.NewFixedSchema("huge", "", math.MaxInt, avro.NewDecimalLogicalSchema(1, 0))
	require.NoError(t, err)
	enc := avro.NewEncoderForSchema(schema, bytes.NewBuffer(nil))

	assert.NotPanics(t, func() {
		err = enc.Encode(big.NewRat(0, 1))
	})
	assert.Error(t, err)
}

func TestEncoderFixedProgrammaticInvalidDecimal(t *testing.T) {
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
			var data bytes.Buffer

			assert.NotPanics(t, func() {
				err = avro.NewEncoderForSchema(schema, &data).Encode(big.NewRat(1, 1))
			})

			require.Error(t, err)
			assert.Empty(t, data.Bytes())
		})
	}
}
