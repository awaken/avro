package avro_test

import (
	"io"
	"math/big"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixedLimitReader struct {
	reads int
}

func (r *fixedLimitReader) Read([]byte) (int, error) {
	r.reads++
	return 0, io.EOF
}

func TestFixedConfiguredSizeLimit(t *testing.T) {
	for _, logical := range []bool{false, true} {
		text := `{"type":"fixed","name":"Limit","size":2}`
		var value any = [2]byte{0, 1}
		if logical {
			text = `{"type":"fixed","name":"Limit","size":2,"logicalType":"decimal","precision":2}`
			value = big.NewRat(1, 1)
		}
		schema := avro.MustParse(text)
		strict := avro.Config{MaxByteSliceSize: 1}.Freeze()
		_, err := strict.Marshal(schema, value)
		assert.ErrorContains(t, err, "MaxByteSliceSize")
		var generic any
		assert.ErrorContains(t, strict.Unmarshal(schema, []byte{0, 1}, &generic), "MaxByteSliceSize")
		assert.Nil(t, generic)
		r := avro.NewReader(nil, 0, avro.WithReaderConfig(strict)).Reset([]byte{0, 1})
		assert.Nil(t, r.ReadNext(schema))
		assert.ErrorContains(t, r.Error, "MaxByteSliceSize")

		union, err := avro.NewUnionSchema([]avro.Schema{avro.NewNullSchema(), schema})
		require.NoError(t, err)
		_, err = strict.Marshal(union, map[string]any{"Limit": value})
		assert.ErrorContains(t, err, "MaxByteSliceSize")
		assert.ErrorContains(t, strict.Unmarshal(union, []byte{2, 0, 1}, &generic), "MaxByteSliceSize")

		// A different frozen API must not inherit the stricter API's cached error.
		for _, limit := range []int{0, 2, -1} {
			api := avro.Config{MaxByteSliceSize: limit}.Freeze()
			wire, err := api.Marshal(schema, value)
			require.NoError(t, err)
			assert.Equal(t, []byte{0, 1}, wire)
			generic = nil
			require.NoError(t, api.Unmarshal(schema, wire, &generic))
			assert.Equal(t, value, generic)
		}
	}
}

func TestFixedSizeLimitBeforeRead(t *testing.T) {
	for _, size := range []int{2, 1_048_577} {
		limit := 1
		if size > 2 {
			limit = 0 // Default limit is 1 MiB.
		}
		schema, err := avro.NewFixedSchema("Limit", "", size, nil)
		require.NoError(t, err)
		api := avro.Config{MaxByteSliceSize: limit}.Freeze()
		source := &fixedLimitReader{}
		var got any
		// ReadVal bypasses the stream Decoder's deliberate EOF prefetch.
		r := avro.NewReader(source, 0, avro.WithReaderConfig(api))
		r.ReadVal(schema, &got)
		assert.ErrorContains(t, r.Error, "MaxByteSliceSize")
		assert.Equal(t, 0, source.reads)
		assert.Nil(t, got)
	}
}

func TestFixedSizeLimitWhileSkipping(t *testing.T) {
	writer := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"skip","type":{"type":"fixed","name":"Limit","size":2}}]}`)
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[]}`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)
	api := avro.Config{MaxByteSliceSize: 1}.Freeze()
	var dst struct{}
	assert.ErrorContains(t, api.Unmarshal(resolved, []byte{0, 1}, &dst), "MaxByteSliceSize")
	r := avro.NewReader(nil, 0, avro.WithReaderConfig(api)).Reset([]byte{0, 1})
	r.ReadNext(resolved)
	assert.ErrorContains(t, r.Error, "MaxByteSliceSize")
}
