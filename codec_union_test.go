package avro_test

import (
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/require"
)

func TestNullUnionRejectsPayload(t *testing.T) {
	schema := avro.MustParse(`["null","string"]`)
	for _, value := range []any{"lost", 42, []byte{1}} {
		writer := avro.NewWriter(nil, 0)
		writer.WriteVal(schema, map[string]any{"null": value})
		require.Error(t, writer.Error)
		require.Empty(t, writer.Buffer(), "invalid branch must not write its index")
	}
	data, err := avro.Marshal(schema, map[string]any{"null": nil})
	require.NoError(t, err)
	require.Equal(t, []byte{0}, data)
}

func TestNullUnionAllowsExplicitConversion(t *testing.T) {
	for _, kind := range []avro.Type{avro.Union, avro.Null} {
		api := avro.Config{}.Freeze()
		api.RegisterTypeConverters(avro.TypeConversionFuncs{
			AvroType:              kind,
			EncoderTypeConversion: func(in any, schema avro.Schema) (any, error) { return nil, nil },
		})
		data, err := api.Marshal(avro.MustParse(`["null","string"]`), map[string]any{"null": "converted"})
		require.NoError(t, err)
		require.Equal(t, []byte{0}, data)
	}
}
