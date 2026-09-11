package avro

import (
	"crypto/sha256"
	"errors"
	"slices"
)

// ErrResolvedSchemaEncoding reports an attempt to encode using a decode plan.
var ErrResolvedSchemaEncoding = errors.New("avro: resolved writer unions are decode-only; encode with the declared schema")

type resolutionPair struct {
	reader *RecordSchema
	writer *RecordSchema
}

type resolvedRecord struct {
	schema  *RecordSchema
	changed bool
	done    bool
}

// schemaResolution owns all incomplete schemas until one Resolve call succeeds.
type schemaResolution struct {
	*SchemaCompatibility
	records map[resolutionPair]*resolvedRecord
	order   []resolutionPair
	caches  []*cacheFingerprinter
}

func newSchemaResolution(c *SchemaCompatibility) *schemaResolution {
	return &schemaResolution{SchemaCompatibility: c, records: make(map[resolutionPair]*resolvedRecord)}
}

func (c *schemaResolution) remember(cache *cacheFingerprinter) {
	c.caches = append(c.caches, cache)
}

func (c *schemaResolution) union(reader, writer Schema, plans []Schema) (*UnionSchema, error) {
	types := Schemas{reader}
	if union, ok := reader.(*UnionSchema); ok {
		types = union.Types()
	}
	union, err := NewUnionSchema(types, withWriterFingerprint(writer.Fingerprint()))
	if err != nil {
		return nil, err
	}
	union.encodedTypes = slices.Clone(plans)
	c.remember(&union.cacheFingerprinter)
	return union, nil
}

// finish binds caches to the complete input graph, including metadata behind
// recursive references. No partially built result has entered a codec cache.
func (c *schemaResolution) finish(reader, writer Schema) {
	var data []byte
	add := func(fp [32]byte) { data = append(data, fp[:]...) }
	add(compatibilityFingerprint(reader))
	add(compatibilityFingerprint(writer))
	add(reader.CacheFingerprint())
	add(writer.CacheFingerprint())
	for _, pair := range c.order {
		add(pair.reader.CacheFingerprint())
		add(pair.writer.CacheFingerprint())
	}
	graph := sha256.Sum256(data)
	for _, cache := range c.caches {
		data = append(data[:0], graph[:]...)
		if cache.writerFingerprint != nil {
			data = append(data, cache.writerFingerprint[:]...)
		}
		fp := sha256.Sum256(data)
		cache.writerFingerprint = &fp
	}
}
