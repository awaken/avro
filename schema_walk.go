package avro

import "slices"

func walkSchema(schema Schema, fn func(Schema) Schema) Schema {
	schema = fn(schema)

	switch s := schema.(type) {
	case *RecordSchema:
		for _, f := range s.Fields() {
			f.typ = walkSchema(f.typ, fn)
		}
	case *ArraySchema:
		s.items = walkSchema(s.items, fn)
	case *MapSchema:
		s.values = walkSchema(s.values, fn)
	case *UnionSchema:
		for i, st := range s.types {
			s.types[i] = walkSchema(st, fn)
		}
	}
	return schema
}

func cloneSchemaGraph(schema Schema) Schema {
	return cloneSchema(schema, map[Schema]Schema{})
}

func cloneSchema(schema Schema, cloned map[Schema]Schema) Schema {
	switch s := schema.(type) {
	case *PrimitiveSchema:
		if clone, ok := cloned[s]; ok {
			return clone
		}
		clone := &PrimitiveSchema{
			properties:         cloneProperties(s.properties),
			cacheFingerprinter: cloneCacheFingerprinter(s.cacheFingerprinter),
			typ:                s.typ,
			logical:            s.logical,
			encodedType:        s.encodedType,
		}
		cloned[s] = clone
		return clone
	case *RecordSchema:
		if clone, ok := cloned[s]; ok {
			return clone
		}
		clone := &RecordSchema{
			name:               cloneName(s.name),
			properties:         cloneProperties(s.properties),
			cacheFingerprinter: cloneCacheFingerprinter(s.cacheFingerprinter),
			isError:            s.isError,
			fields:             make([]*Field, len(s.fields)),
			doc:                s.doc,
		}
		cloned[s] = clone
		for i, field := range s.fields {
			clone.fields[i] = cloneField(field, cloned)
		}
		return clone
	case *EnumSchema:
		if clone, ok := cloned[s]; ok {
			return clone
		}
		clone := &EnumSchema{
			name:               cloneName(s.name),
			properties:         cloneProperties(s.properties),
			cacheFingerprinter: cloneCacheFingerprinter(s.cacheFingerprinter),
			symbols:            slices.Clone(s.symbols),
			def:                s.def,
			doc:                s.doc,
			encodedSymbols:     slices.Clone(s.encodedSymbols),
		}
		cloned[s] = clone
		return clone
	case *ArraySchema:
		if clone, ok := cloned[s]; ok {
			return clone
		}
		clone := &ArraySchema{
			properties:         cloneProperties(s.properties),
			cacheFingerprinter: cloneCacheFingerprinter(s.cacheFingerprinter),
		}
		cloned[s] = clone
		clone.items = cloneSchema(s.items, cloned)
		return clone
	case *MapSchema:
		if clone, ok := cloned[s]; ok {
			return clone
		}
		clone := &MapSchema{
			properties:         cloneProperties(s.properties),
			cacheFingerprinter: cloneCacheFingerprinter(s.cacheFingerprinter),
		}
		cloned[s] = clone
		clone.values = cloneSchema(s.values, cloned)
		return clone
	case *UnionSchema:
		if clone, ok := cloned[s]; ok {
			return clone
		}
		clone := &UnionSchema{
			cacheFingerprinter: cloneCacheFingerprinter(s.cacheFingerprinter),
			types:              make(Schemas, len(s.types)),
		}
		cloned[s] = clone
		for i, typ := range s.types {
			clone.types[i] = cloneSchema(typ, cloned)
		}
		return clone
	case *FixedSchema:
		if clone, ok := cloned[s]; ok {
			return clone
		}
		clone := &FixedSchema{
			name:               cloneName(s.name),
			properties:         cloneProperties(s.properties),
			cacheFingerprinter: cloneCacheFingerprinter(s.cacheFingerprinter),
			size:               s.size,
			logical:            s.logical,
		}
		cloned[s] = clone
		return clone
	case *NullSchema:
		if clone, ok := cloned[s]; ok {
			return clone
		}
		clone := &NullSchema{properties: cloneProperties(s.properties)}
		cloned[s] = clone
		return clone
	case *RefSchema:
		if clone, ok := cloned[s]; ok {
			return clone
		}
		clone := &RefSchema{}
		cloned[s] = clone
		clone.actual = cloneSchema(s.actual, cloned).(NamedSchema)
		return clone
	default:
		return schema
	}
}

func cloneField(field *Field, cloned map[Schema]Schema) *Field {
	if field == nil {
		return nil
	}
	clone := &Field{
		properties: cloneProperties(field.properties),
		name:       field.name,
		aliases:    slices.Clone(field.aliases),
		doc:        field.doc,
		hasDef:     field.hasDef,
		def:        cloneDefault(field.def),
		order:      field.order,
		action:     field.action,
	}
	clone.typ = cloneSchema(field.typ, cloned)
	if encoded := field.encodedDef.Load(); encoded != nil {
		clone.encodedDef.Store(slices.Clone(encoded.([]byte)))
	}
	return clone
}

func cloneName(value name) name {
	value.aliases = slices.Clone(value.aliases)
	return value
}

func cloneProperties(value properties) properties {
	if value.props == nil {
		return properties{}
	}
	props := make(map[string]any, len(value.props))
	for key, item := range value.props {
		props[key] = cloneDefault(item)
	}
	return properties{props: props}
}

func cloneCacheFingerprinter(value cacheFingerprinter) cacheFingerprinter {
	if value.writerFingerprint == nil {
		return cacheFingerprinter{}
	}
	fingerprint := *value.writerFingerprint
	return cacheFingerprinter{writerFingerprint: &fingerprint}
}
