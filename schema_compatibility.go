package avro

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
)

type compatKey struct {
	reader [32]byte
	writer [32]byte
}

type compatibilityFingerprintWriter struct {
	data []byte
	seen map[Schema]uint64
}

func compatibilityFingerprint(schema Schema) [32]byte {
	w := compatibilityFingerprintWriter{seen: map[Schema]uint64{}}
	w.writeSchema(schema)
	return sha256.Sum256(w.data)
}

func (w *compatibilityFingerprintWriter) writeUint(value uint64) {
	w.data = binary.AppendUvarint(w.data, value)
}

func (w *compatibilityFingerprintWriter) writeString(value string) {
	w.writeUint(uint64(len(value)))
	w.data = append(w.data, value...)
}

func (w *compatibilityFingerprintWriter) writeStrings(values []string) {
	w.writeUint(uint64(len(values)))
	for _, value := range values {
		w.writeString(value)
	}
}

func (w *compatibilityFingerprintWriter) writeDefinition(schema Schema) bool {
	if id, ok := w.seen[schema]; ok {
		w.writeString("reference")
		w.writeUint(id)
		return false
	}

	id := uint64(len(w.seen) + 1)
	w.seen[schema] = id
	w.writeString("definition")
	w.writeUint(id)
	return true
}

func (w *compatibilityFingerprintWriter) writeSchema(schema Schema) {
	switch s := schema.(type) {
	case *RefSchema:
		w.writeSchema(s.Schema())
	case *PrimitiveSchema:
		w.writeString("primitive")
		w.writeString(string(s.Type()))
		w.writeDecimal(s)
	case *NullSchema:
		w.writeString("null")
	case *RecordSchema:
		if !w.writeDefinition(s) {
			return
		}
		w.writeString("record")
		w.writeString(s.FullName())
		w.writeStrings(s.Aliases())
		fields := s.Fields()
		w.writeUint(uint64(len(fields)))
		for _, field := range fields {
			w.writeString(field.Name())
			w.writeStrings(field.Aliases())
			if field.HasDefault() {
				w.writeUint(1)
			} else {
				w.writeUint(0)
			}
			w.writeSchema(field.Type())
		}
	case *EnumSchema:
		if !w.writeDefinition(s) {
			return
		}
		w.writeString("enum")
		w.writeString(s.FullName())
		w.writeStrings(s.Aliases())
		w.writeStrings(s.Symbols())
		if s.HasDefault() {
			w.writeUint(1)
		} else {
			w.writeUint(0)
		}
	case *FixedSchema:
		if !w.writeDefinition(s) {
			return
		}
		w.writeString("fixed")
		w.writeString(s.FullName())
		w.writeStrings(s.Aliases())
		w.writeUint(uint64(s.Size()))
		w.writeDecimal(s)
	case *ArraySchema:
		w.writeString("array")
		w.writeSchema(s.Items())
	case *MapSchema:
		w.writeString("map")
		w.writeSchema(s.Values())
	case *UnionSchema:
		w.writeString("union")
		types := s.Types()
		w.writeUint(uint64(len(types)))
		for _, typ := range types {
			w.writeSchema(typ)
		}
	default:
		w.writeString("schema")
		fingerprint := schema.CacheFingerprint()
		w.data = append(w.data, fingerprint[:]...)
	}
}

func (w *compatibilityFingerprintWriter) writeDecimal(schema LogicalTypeSchema) {
	decimal, ok := schema.Logical().(*DecimalLogicalSchema)
	if !ok {
		w.writeUint(0)
		return
	}

	w.writeUint(1)
	w.writeUint(uint64(decimal.Precision()))
	w.writeUint(uint64(decimal.Scale()))
}

// SchemaCompatibility determines the compatibility of schemas.
type SchemaCompatibility struct {
	cache sync.Map // map[compatKey]error
}

// NewSchemaCompatibility creates a new schema compatibility instance.
func NewSchemaCompatibility() *SchemaCompatibility {
	return &SchemaCompatibility{}
}

// Compatible determines whether the reader can read the writer schema.
// Each reader field must match at most one writer name, through its name or aliases.
// Ambiguous matches are rejected, including an exact match plus an alias match.
func (c *SchemaCompatibility) Compatible(reader, writer Schema) error {
	if isNilSchema(reader) {
		return errors.New("avro: reader schema is nil")
	}
	if isNilSchema(writer) {
		return errors.New("avro: writer schema is nil")
	}

	return c.compatible(reader, writer, map[compatKey]struct{}{})
}

func (c *SchemaCompatibility) compatible(reader, writer Schema, active map[compatKey]struct{}) error {
	key := compatKey{reader: compatibilityFingerprint(reader), writer: compatibilityFingerprint(writer)}
	if err, ok := c.cache.Load(key); ok {
		if err == nil {
			return nil
		}

		return err.(error)
	}
	if _, ok := active[key]; ok {
		return nil
	}

	active[key] = struct{}{}
	err := c.match(reader, writer, active)
	delete(active, key)
	if err != nil {
		// We dont want to pay the cost of fmt.Errorf every time
		err = errors.New(err.Error())
	}
	c.cache.Store(key, err)
	return err
}

func (c *SchemaCompatibility) match(reader, writer Schema, active map[compatKey]struct{}) error {
	// If the schema is a reference, get the actual schema
	if reader.Type() == Ref {
		reader = reader.(*RefSchema).Schema()
	}
	if writer.Type() == Ref {
		writer = writer.(*RefSchema).Schema()
	}
	if err := checkDecimalCompatibility(reader, writer); err != nil {
		return err
	}

	if reader.Type() != writer.Type() {
		if writer.Type() == Union {
			// Reader must be compatible with all types in writer
			for _, schema := range writer.(*UnionSchema).Types() {
				if err := c.compatible(reader, schema, active); err != nil {
					return err
				}
			}

			return nil
		}

		if reader.Type() == Union {
			// Writer must be compatible with at least one reader schema
			var err error
			for _, schema := range reader.(*UnionSchema).Types() {
				err = c.compatible(schema, writer, active)
				if err == nil {
					return nil
				}
			}

			return fmt.Errorf("reader union lacking writer schema %s", writer.Type())
		}

		switch writer.Type() {
		case Int:
			if reader.Type() == Long || reader.Type() == Float || reader.Type() == Double {
				return nil
			}

		case Long:
			if reader.Type() == Float || reader.Type() == Double {
				return nil
			}

		case Float:
			if reader.Type() == Double {
				return nil
			}

		case String:
			if reader.Type() == Bytes {
				return nil
			}

		case Bytes:
			if reader.Type() == String {
				return nil
			}
		}

		return fmt.Errorf("reader schema %s not compatible with writer schema %s", reader.Type(), writer.Type())
	}

	switch reader.Type() {
	case Array:
		return c.compatible(reader.(*ArraySchema).Items(), writer.(*ArraySchema).Items(), active)

	case Map:
		return c.compatible(reader.(*MapSchema).Values(), writer.(*MapSchema).Values(), active)

	case Fixed:
		r := reader.(*FixedSchema)
		w := writer.(*FixedSchema)

		if err := c.checkSchemaName(r, w); err != nil {
			return err
		}

		if err := c.checkFixedSize(r, w); err != nil {
			return err
		}

	case Enum:
		r := reader.(*EnumSchema)
		w := writer.(*EnumSchema)

		if err := c.checkSchemaName(r, w); err != nil {
			return err
		}

		if err := c.checkEnumSymbols(r, w); err != nil {
			if r.HasDefault() {
				return nil
			}
			return err
		}

	case Record:
		r := reader.(*RecordSchema)
		w := writer.(*RecordSchema)

		if err := c.checkSchemaName(r, w); err != nil {
			return err
		}

		if err := c.checkRecordFields(r, w, active); err != nil {
			return err
		}

	case Union:
		for _, schema := range writer.(*UnionSchema).Types() {
			if err := c.compatible(reader, schema, active); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkDecimalCompatibility(reader, writer Schema) error {
	r, rOK := decimalLogicalSchema(reader)
	w, wOK := decimalLogicalSchema(writer)
	if !rOK || !wOK {
		return nil
	}
	if r.Precision() != w.Precision() || r.Scale() != w.Scale() {
		return fmt.Errorf(
			"reader decimal precision %d and scale %d do not match writer decimal precision %d and scale %d",
			r.Precision(), r.Scale(), w.Precision(), w.Scale(),
		)
	}

	return nil
}

func decimalLogicalSchema(schema Schema) (*DecimalLogicalSchema, bool) {
	return validDecimalLogicalSchema(schema)
}

func (c *SchemaCompatibility) checkSchemaName(reader, writer NamedSchema) error {
	if reader.Name() != writer.Name() {
		if slices.Contains(reader.Aliases(), writer.FullName()) {
			return nil
		}
		return fmt.Errorf("reader schema %s and writer schema %s  names do not match", reader.FullName(), writer.FullName())
	}

	return nil
}

func (c *SchemaCompatibility) checkFixedSize(reader, writer *FixedSchema) error {
	if reader.Size() != writer.Size() {
		return fmt.Errorf("%s reader and writer fixed sizes do not match", reader.FullName())
	}

	return nil
}

func (c *SchemaCompatibility) checkEnumSymbols(reader, writer *EnumSchema) error {
	for _, symbol := range writer.Symbols() {
		if !slices.Contains(reader.Symbols(), symbol) {
			return fmt.Errorf("reader %s is missing symbol %s", reader.FullName(), symbol)
		}
	}

	return nil
}

func (c *SchemaCompatibility) identicalEnumSymbols(reader, writer *EnumSchema) bool {
	if len(reader.symbols) != len(writer.symbols) {
		return false
	}
	for i := range len(reader.symbols) {
		if reader.symbols[i] != writer.symbols[i] {
			return false
		}
	}
	return true
}

func (c *SchemaCompatibility) checkRecordFields(
	reader, writer *RecordSchema,
	active map[compatKey]struct{},
) error {
	matched, err := matchRecordFields(reader, writer)
	if err != nil {
		return err
	}
	for _, field := range reader.fields {
		f := matched[field]
		if f == nil {
			if field.HasDefault() {
				continue
			}

			return fmt.Errorf("reader field %s is missing in writer schema and has no default", field.Name())
		}

		if err := c.compatible(field.Type(), f.Type(), active); err != nil {
			return err
		}
	}

	return nil
}

// Match before type recursion so compatibility and resolution use the same mapping.
// Writer aliases do not rename wire fields; only reader aliases participate.
func matchRecordFields(reader, writer *RecordSchema) (map[*Field]*Field, error) {
	writers := make(map[string]*Field, len(writer.fields))
	for _, field := range writer.fields {
		writers[field.name] = field
	}
	matched := make(map[*Field]*Field, len(reader.fields))
	used := make(map[*Field]*Field, len(reader.fields))
	for _, field := range reader.fields {
		match := writers[field.name]
		for _, alias := range field.aliases {
			if candidate := writers[alias]; candidate != nil {
				if match != nil {
					return nil, fmt.Errorf("reader field %s matches multiple writer fields", field.name)
				}
				match = candidate
			}
		}
		if match == nil {
			continue
		}
		if other := used[match]; other != nil {
			return nil, fmt.Errorf("writer field %s matches multiple reader fields", match.name)
		}
		matched[field] = match
		used[match] = field
	}
	return matched, nil
}

// Resolve returns a composite schema that allows decoding data written by the writer schema,
// and makes necessary adjustments to support the reader schema.
//
// It fails if the writer and reader schemas are not compatible.
// Recursive records retain references to the completed resolution graph.
// Resolved writer unions are decode-only: their public types, JSON and canonical
// fingerprints describe the reader union, while CacheFingerprint identifies the
// writer-indexed decoding plan. Serializing a schema does not preserve that plan;
// retain both declared schemas and call Resolve again to reconstruct it.
func (c *SchemaCompatibility) Resolve(reader, writer Schema) (Schema, error) {
	if err := c.Compatible(reader, writer); err != nil {
		return nil, err
	}

	r := newSchemaResolution(c)
	schema, _, err := r.resolve(reader, writer)
	if err != nil {
		return nil, err
	}
	r.finish(reader, writer)
	return schema, nil
}

// resolve requires the reader's schema to be already compatible with the writer's.
func (c *schemaResolution) resolve(reader, writer Schema) (schema Schema, resolved bool, err error) {
	if reader.Type() == Ref {
		reader = reader.(*RefSchema).Schema()
	}
	if writer.Type() == Ref {
		writer = writer.(*RefSchema).Schema()
	}

	if writer.Type() != reader.Type() {
		if reader.Type() == Union {
			for _, schema := range reader.(*UnionSchema).Types() {
				// Compatibility is not guaranteed for every Union reader schema.
				// Therefore, we need to check compatibility in every iteration.
				if err := c.Compatible(schema, writer); err != nil {
					continue
				}
				sch, _, err := c.resolve(schema, writer)
				if err != nil {
					return nil, false, err
				}
				return sch, true, nil
			}

			return nil, false, fmt.Errorf("reader union lacking writer schema %s", writer.Type())
		}

		if writer.Type() == Union {
			schemas := make([]Schema, 0)
			for _, schema := range writer.(*UnionSchema).Types() {
				sch, _, err := c.resolve(reader, schema)
				if err != nil {
					return nil, false, err
				}
				schemas = append(schemas, sch)
			}
			s, err := c.union(reader, writer, schemas)
			return s, true, err
		}

		if isPromotable(writer.Type(), reader.Type()) {
			r := NewPrimitiveSchema(reader.Type(), reader.(*PrimitiveSchema).Logical(),
				WithProps(reader.(*PrimitiveSchema).Props()),
				withWriterFingerprint(writer.Fingerprint()),
			)
			r.encodedType = writer.Type()
			c.remember(&r.cacheFingerprinter)
			return r, true, nil
		}

		return nil, false, fmt.Errorf("failed to resolve composite schema for %s and %s", reader.Type(), writer.Type())
	}

	if isNative(writer.Type()) {
		return reader, false, nil
	}

	if writer.Type() == Enum {
		r := reader.(*EnumSchema)
		w := writer.(*EnumSchema)
		if err = c.checkEnumSymbols(r, w); err != nil {
			if r.HasDefault() {
				enum, _ := NewEnumSchema(r.Name(), r.Namespace(), r.Symbols(),
					WithAliases(r.Aliases()),
					WithDoc(r.Doc()),
					WithDefault(r.Default()),
					WithProps(r.Props()),
					withWriterFingerprint(w.Fingerprint()),
				)
				enum.encodedSymbols = w.Symbols()
				c.remember(&enum.cacheFingerprinter)
				return enum, true, nil
			}
			return nil, false, err
		}
		if !c.identicalEnumSymbols(r, w) {
			opts := []SchemaOption{
				WithAliases(r.Aliases()),
				WithDoc(r.Doc()),
				WithProps(r.Props()),
				withWriterFingerprint(w.Fingerprint()),
			}
			if r.HasDefault() {
				opts = append(opts, WithDefault(r.Default()))
			}
			enum, _ := NewEnumSchema(r.Name(), r.Namespace(), r.Symbols(), opts...)
			enum.encodedSymbols = w.Symbols()
			c.remember(&enum.cacheFingerprinter)
			return enum, true, nil
		}
		return reader, false, nil
	}

	if writer.Type() == Fixed {
		return reader, false, nil
	}

	if writer.Type() == Union {
		schemas := make([]Schema, 0)
		for _, s := range writer.(*UnionSchema).Types() {
			sch, resolv, err := c.resolve(reader, s)
			if err != nil {
				return nil, false, err
			}
			schemas = append(schemas, sch)
			resolved = resolv || resolved
		}
		s, err := c.union(reader, writer, schemas)
		if err != nil {
			return nil, false, err
		}
		return s, resolved, nil
	}

	if writer.Type() == Array {
		schema, resolved, err = c.resolve(reader.(*ArraySchema).Items(), writer.(*ArraySchema).Items())
		if err != nil {
			return nil, false, err
		}
		array := NewArraySchema(schema,
			WithProps(reader.(*ArraySchema).Props()),
			withWriterFingerprintIfResolved(writer.Fingerprint(), resolved),
		)
		c.remember(&array.cacheFingerprinter)
		return array, resolved, nil
	}

	if writer.Type() == Map {
		schema, resolved, err = c.resolve(reader.(*MapSchema).Values(), writer.(*MapSchema).Values())
		if err != nil {
			return nil, false, err
		}
		m := NewMapSchema(schema,
			WithProps(reader.(*MapSchema).Props()),
			withWriterFingerprintIfResolved(writer.Fingerprint(), resolved),
		)
		c.remember(&m.cacheFingerprinter)
		return m, resolved, nil
	}

	if writer.Type() == Record {
		return c.resolveRecord(reader, writer)
	}

	return nil, false, fmt.Errorf("failed to resolve composite schema for %s and %s", reader.Type(), writer.Type())
}

func (c *schemaResolution) resolveRecord(reader, writer Schema) (Schema, bool, error) {
	w := writer.(*RecordSchema)
	r := reader.(*RecordSchema)
	pair := resolutionPair{reader: r, writer: w}
	if previous := c.records[pair]; previous != nil {
		return NewRefSchema(previous.schema), previous.changed || !previous.done, nil
	}
	matched, err := matchRecordFields(r, w)
	if err != nil {
		return nil, false, err
	}
	// Publish only a named placeholder within this call; fields follow below.
	placeholder, err := newRecordSchema(r.Name(), r.Namespace(), nil,
		WithAliases(r.Aliases()), WithDoc(r.Doc()), WithProps(r.Props()),
		withWriterFingerprint(w.Fingerprint()))
	if err != nil {
		return nil, false, err
	}
	placeholder.isError = r.IsError()
	entry := &resolvedRecord{schema: placeholder}
	c.records[pair] = entry
	c.order = append(c.order, pair)
	c.remember(&placeholder.cacheFingerprinter)
	readers := make(map[*Field]*Field, len(matched))
	for reader, writer := range matched {
		readers[writer] = reader
	}

	fields := make([]*Field, 0)

	var resolved bool
	for _, wf := range w.fields {
		rf := readers[wf]
		if rf == nil {
			// The field was not found in the reader schema, it should be ignored.
			// Its writer aliases are irrelevant and may conflict with reader names.
			f, _ := NewField(wf.Name(), wf.Type(),
				WithDoc(wf.Doc()),
				WithOrder(wf.Order()),
				WithProps(wf.Props()),
			)
			f.def = wf.def
			f.jsonDef = cloneDefault(wf.jsonDef)
			f.hasDef = wf.hasDef
			f.action = FieldIgnore
			fields = append(fields, f)

			resolved = true
			continue
		}

		ft, resolv, err := c.resolve(rf.Type(), wf.Type())
		if err != nil {
			return nil, false, err
		}
		f, _ := NewField(rf.Name(), ft,
			WithAliases(rf.Aliases()),
			WithDoc(rf.Doc()),
			WithOrder(rf.Order()),
			WithProps(rf.Props()),
		)
		f.def = rf.def
		f.jsonDef = cloneDefault(rf.jsonDef)
		f.hasDef = rf.hasDef
		fields = append(fields, f)
		resolved = resolv || resolved
	}

	for _, rf := range r.fields {
		if matched[rf] != nil {
			// This field has already been seen.
			continue
		}

		// The schemas are already known to be compatible, so there must be a default on
		// the field in the writer. Use the default.

		f, _ := NewField(rf.Name(), rf.Type(),
			WithAliases(rf.Aliases()),
			WithDoc(rf.Doc()),
			WithOrder(rf.Order()),
			WithProps(rf.Props()),
		)
		f.def = rf.def
		f.jsonDef = cloneDefault(rf.jsonDef)
		f.hasDef = rf.hasDef
		f.action = FieldSetDefault
		fields = append(fields, f)

		resolved = true
	}

	if err := validateRecordFields(fields); err != nil {
		return nil, false, err
	}
	placeholder.fields = fields
	entry.changed, entry.done = resolved, true
	return placeholder, resolved, nil
}

func isNative(typ Type) bool {
	switch typ {
	case Null, Boolean, Int, Long, Float, Double, Bytes, String:
		return true
	default:
		return false
	}
}

func isPromotable(writerTyp, readerType Type) bool {
	switch writerTyp {
	case Int:
		return readerType == Long || readerType == Float || readerType == Double
	case Long:
		return readerType == Float || readerType == Double
	case Float:
		return readerType == Double
	case String:
		return readerType == Bytes
	case Bytes:
		return readerType == String
	default:
		return false
	}
}
