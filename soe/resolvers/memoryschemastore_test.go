package resolvers_test

import (
	"context"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/awaken/avro/v2/soe"
	"github.com/awaken/avro/v2/soe/resolvers"
	"github.com/stretchr/testify/require"
)

type fingerprintSchema struct {
	avro.Schema
	fingerprint []byte
}

func (s fingerprintSchema) FingerprintUsing(avro.FingerprintType) ([]byte, error) {
	return s.fingerprint, nil
}

func TestMemorySchemaStore_AddSchemaRejectsNilSchema(t *testing.T) {
	for _, schema := range []avro.Schema{nil, (*avro.RecordSchema)(nil)} {
		store := resolvers.NewMemorySchemaStore()

		var err error
		require.NotPanics(t, func() {
			err = store.AddSchema(schema)
		})
		require.ErrorContains(t, err, "schema cannot be nil")
	}
}

func TestMemorySchemaStore_AddSchemaValidatesFingerprintLength(t *testing.T) {
	for _, length := range []int{0, 7, 9} {
		t.Run(string(rune('0'+length)), func(t *testing.T) {
			store := resolvers.NewMemorySchemaStore()
			schema := fingerprintSchema{
				Schema:      avro.MustParse("null"),
				fingerprint: make([]byte, length),
			}

			var err error
			require.NotPanics(t, func() {
				err = store.AddSchema(schema)
			})
			require.ErrorContains(t, err, "bad fingerprint length")
		})
	}

	store := resolvers.NewMemorySchemaStore()
	schema := fingerprintSchema{
		Schema:      avro.MustParse("null"),
		fingerprint: make([]byte, 8),
	}
	require.NoError(t, store.AddSchema(schema))

	got, err := store.GetSchema(context.Background(), schema.fingerprint)
	require.NoError(t, err)
	require.Equal(t, schema, got)
}

func TestMemorySchemaStore_GetSchemaValidatesFingerprintLength(t *testing.T) {
	store := resolvers.NewMemorySchemaStore()
	require.NoError(t, store.AddSchema(avro.MustParse("string")))

	for _, length := range []int{0, 7, 9} {
		t.Run(string(rune('0'+length)), func(t *testing.T) {
			_, err := store.GetSchema(context.Background(), make([]byte, length))
			require.ErrorIs(t, err, soe.ErrUnknownSchema)
			require.ErrorContains(t, err, "invalid fingerprint length")
		})
	}

	_, err := store.GetSchema(context.Background(), make([]byte, 8))
	require.ErrorIs(t, err, soe.ErrUnknownSchema)
}
