package resolvers

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/awaken/avro/v2"
	"github.com/awaken/avro/v2/soe"
)

const fingerprintSize = 8

// MemorySchemaStore is a basic in-memory schema store and resolver
// implementation.
type MemorySchemaStore struct {
	schemas sync.Map // map[uint64]avro.Schema
}

// NewMemorySchemaStore creates a new MemorySchemaStore.
func NewMemorySchemaStore() *MemorySchemaStore {
	return &MemorySchemaStore{}
}

// AddSchema puts a schema into the registry.
func (s *MemorySchemaStore) AddSchema(schema avro.Schema) error {
	fp, err := soe.ComputeFingerprint(schema)
	if err != nil {
		return err
	}
	if len(fp) != fingerprintSize {
		return fmt.Errorf("bad fingerprint length: %d", len(fp))
	}

	key := keyFromFingerprint(fp)
	s.schemas.Store(key, schema)
	return nil
}

// GetSchema implements SchemaResolver.
func (s *MemorySchemaStore) GetSchema(_ context.Context, fingerprint []byte) (avro.Schema, error) {
	if len(fingerprint) != fingerprintSize {
		return nil, fmt.Errorf("%w: invalid fingerprint length %d", soe.ErrUnknownSchema, len(fingerprint))
	}
	key := keyFromFingerprint(fingerprint)
	schema, ok := s.schemas.Load(key)
	if !ok {
		return nil, fmt.Errorf("%w: %x", soe.ErrUnknownSchema, fingerprint)
	}
	return schema.(avro.Schema), nil
}

// keyFromFingerprint creates an arbitrary uint64 encoding of a schema
// fingerprint, suitable as a map key.
func keyFromFingerprint(fingerprint []byte) uint64 {
	return binary.LittleEndian.Uint64(fingerprint)
}
