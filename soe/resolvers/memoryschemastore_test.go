package resolvers_test

import (
	"context"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/awaken/avro/v2/soe"
	"github.com/awaken/avro/v2/soe/resolvers"
	"github.com/stretchr/testify/require"
)

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
