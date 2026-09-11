package avro

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	jsoniter "github.com/json-iterator/go"
)

var schemaJSONAPI = jsoniter.Config{UseNumber: true}.Froze()

// DefaultSchemaCache is the default cache for schemas.
var DefaultSchemaCache = &SchemaCache{}

// SkipNameValidation sets whether to skip name validation.
// Avro spec incurs a strict naming convention for names and aliases, however official Avro tools do not follow that
// More info:
// https://lists.apache.org/thread/39v98os6wdpyr6w31xdkz0yzol51fsrr
// https://github.com/apache/avro/pull/1995
var SkipNameValidation = false

// Parse parses a schema string.
func Parse(schema string) (Schema, error) {
	return ParseBytes([]byte(schema))
}

// ParseWithCache parses a schema string using the given namespace and schema cache.
// A nil cache parses without retaining named schemas.
func ParseWithCache(schema, namespace string, cache *SchemaCache) (Schema, error) {
	return ParseBytesWithCache([]byte(schema), namespace, cache)
}

// MustParse parses a schema string, panicking if there is an error.
func MustParse(schema string) Schema {
	parsed, err := Parse(schema)
	if err != nil {
		panic(err)
	}

	return parsed
}

// ParseFiles parses the schemas in the files, in the order they appear, returning the last schema.
//
// This is useful when your schemas rely on other schemas.
func ParseFiles(paths ...string) (Schema, error) {
	var schema Schema
	cache := &SchemaCache{}
	for _, path := range paths {
		s, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil, err
		}

		schema, err = ParseBytesWithCache(s, "", cache)
		if err != nil {
			return nil, err
		}
	}

	return schema, nil
}

// ParseBytes parses a schema byte slice.
func ParseBytes(schema []byte) (Schema, error) {
	return ParseBytesWithCache(schema, "", DefaultSchemaCache)
}

// ParseBytesWithCache parses a schema byte slice using the given namespace and schema cache.
// A nil cache parses without retaining named schemas.
func ParseBytesWithCache(schema []byte, namespace string, cache *SchemaCache) (Schema, error) {
	var json any
	if err := schemaJSONAPI.Unmarshal(schema, &json); err != nil {
		bareName := strings.TrimSpace(string(schema))
		if !isBareSchemaName(bareName) {
			return nil, fmt.Errorf("avro: invalid schema JSON: %w", err)
		}
		json = bareName
	} else if json == nil && strings.TrimSpace(string(schema)) == "null" {
		json = "null"
	}

	internalCache := &SchemaCache{}
	internalCache.AddAll(cache)

	seen := seenCache{}
	s, err := parseType(namespace, json, seen, internalCache)
	if err != nil {
		return nil, err
	}

	if cache != nil {
		cache.AddAll(internalCache)
	}

	return derefSchema(cloneSchemaGraph(s)), nil
}

func isBareSchemaName(schema string) bool {
	if schema == "" {
		return false
	}
	for part := range strings.SplitSeq(schema, ".") {
		if part == "" || validateNameStrict(part) != nil {
			return false
		}
	}
	return true
}

func parseType(namespace string, v any, seen seenCache, cache *SchemaCache) (Schema, error) {
	switch val := v.(type) {
	case string:
		return parsePrimitiveType(namespace, val, cache)

	case map[string]any:
		return parseComplexType(namespace, val, seen, cache)

	case []any:
		return parseUnion(namespace, val, seen, cache)
	}

	return nil, fmt.Errorf("avro: unknown type: %v", v)
}

func parsePrimitiveType(namespace, s string, cache *SchemaCache) (Schema, error) {
	typ := Type(s)
	switch typ {
	case Null:
		return &NullSchema{}, nil

	case String, Bytes, Int, Long, Float, Double, Boolean:
		return parsePrimitive(typ, nil)

	default:
		schema := cache.Get(fullName(namespace, s))
		if schema != nil {
			return schema, nil
		}

		return nil, fmt.Errorf("avro: unknown type: %s", s)
	}
}

func parseComplexType(namespace string, m map[string]any, seen seenCache, cache *SchemaCache) (Schema, error) {
	str, ok := m["type"].(string)
	if !ok {
		return nil, fmt.Errorf("avro: unknown type: %+v", m)
	}
	typ := Type(str)

	switch typ {
	case String, Bytes, Int, Long, Float, Double, Boolean, Null:
		return parsePrimitive(typ, m)

	case Record, Error:
		return parseRecord(typ, namespace, m, seen, cache)

	case Enum:
		return parseEnum(namespace, m, seen, cache)

	case Array:
		return parseArray(namespace, m, seen, cache)

	case Map:
		return parseMap(namespace, m, seen, cache)

	case Fixed:
		return parseFixed(namespace, m, seen, cache)

	default:
		return parseType(namespace, string(typ), seen, cache)
	}
}

type primitiveSchema struct {
	Type  string         `mapstructure:"type"`
	Props map[string]any `mapstructure:",remain"`
}

func parsePrimitive(typ Type, m map[string]any) (Schema, error) {
	if len(m) == 0 {
		if typ == Null {
			return &NullSchema{}, nil
		}
		return NewPrimitiveSchema(typ, nil), nil
	}

	var (
		p    primitiveSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &p, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding primitive: %w", err)
	}

	var logical LogicalSchema
	if logicalType := logicalTypeProperty(p.Props); logicalType != "" {
		logical = parsePrimitiveLogicalType(typ, logicalType, p.Props)
		if logical != nil {
			delete(p.Props, "logicalType")
		}
	}
	if err := normalizeProperties(p.Props); err != nil {
		return nil, err
	}

	if typ == Null {
		return NewNullSchema(WithProps(p.Props)), nil
	}
	return NewPrimitiveSchema(typ, logical, WithProps(p.Props)), nil
}

func parsePrimitiveLogicalType(typ Type, lt string, props map[string]any) LogicalSchema {
	ltyp := LogicalType(lt)
	if (typ == String && ltyp == UUID) ||
		(typ == Int && ltyp == Date) ||
		(typ == Int && ltyp == TimeMillis) ||
		(typ == Long && ltyp == TimeMicros) ||
		(typ == Long && ltyp == TimestampMillis) ||
		(typ == Long && ltyp == TimestampMicros) ||
		(typ == Long && ltyp == LocalTimestampMillis) ||
		(typ == Long && ltyp == LocalTimestampMicros) {
		return NewPrimitiveLogicalSchema(ltyp)
	}

	if typ == Bytes && ltyp == Decimal {
		return parseDecimalLogicalType(-1, props)
	}

	return nil // otherwise, not a recognized logical type
}

type recordSchema struct {
	Type      string           `mapstructure:"type"`
	Name      string           `mapstructure:"name"`
	Namespace string           `mapstructure:"namespace"`
	Aliases   []string         `mapstructure:"aliases"`
	Doc       string           `mapstructure:"doc"`
	Fields    []map[string]any `mapstructure:"fields"`
	Props     map[string]any   `mapstructure:",remain"`
}

func parseRecord(typ Type, namespace string, m map[string]any, seen seenCache, cache *SchemaCache) (Schema, error) {
	var (
		r    recordSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &r, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding record: %w", err)
	}

	if err := checkParsedName(r.Name); err != nil {
		return nil, err
	}
	if !slices.Contains(meta.Keys, "namespace") {
		r.Namespace = namespace
	}

	if !slices.Contains(meta.Keys, "fields") {
		return nil, errors.New("avro: record must have an array of fields")
	}
	if err := normalizeProperties(r.Props); err != nil {
		return nil, err
	}
	fields := make([]*Field, len(r.Fields))

	var (
		rec *RecordSchema
		err error
	)
	switch typ {
	case Record:
		rec, err = newRecordSchema(r.Name, r.Namespace, fields,
			WithAliases(r.Aliases), WithDoc(r.Doc), WithProps(r.Props),
		)
	case Error:
		rec, err = newErrorRecordSchema(r.Name, r.Namespace, fields,
			WithAliases(r.Aliases), WithDoc(r.Doc), WithProps(r.Props),
		)
	}
	if err != nil {
		return nil, err
	}

	if err = seen.AddSchema(rec); err != nil {
		return nil, err
	}

	ref := NewRefSchema(rec)
	cache.Add(rec.FullName(), ref)
	for _, alias := range rec.Aliases() {
		cache.Add(alias, ref)
	}

	for i, f := range r.Fields {
		field, err := parseField(rec.namespace, f, seen, cache)
		if err != nil {
			return nil, err
		}

		fields[i] = field
	}
	if err = validateRecordFields(fields); err != nil {
		return nil, err
	}

	return rec, nil
}

type fieldSchema struct {
	Name    string         `mapstructure:"name"`
	Aliases []string       `mapstructure:"aliases"`
	Type    any            `mapstructure:"type"`
	Doc     string         `mapstructure:"doc"`
	Default any            `mapstructure:"default"`
	Order   Order          `mapstructure:"order"`
	Props   map[string]any `mapstructure:",remain"`
}

func parseField(namespace string, m map[string]any, seen seenCache, cache *SchemaCache) (*Field, error) {
	var (
		f    fieldSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &f, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding field: %w", err)
	}

	if err := checkParsedName(f.Name); err != nil {
		return nil, err
	}

	if !slices.Contains(meta.Keys, "type") {
		return nil, errors.New("avro: field requires a type")
	}
	typ, err := parseType(namespace, f.Type, seen, cache)
	if err != nil {
		return nil, err
	}

	if !slices.Contains(meta.Keys, "default") {
		f.Default = NoDefault
	}
	if err := normalizeProperties(f.Props); err != nil {
		return nil, err
	}

	field, err := NewField(f.Name, typ,
		WithDefault(f.Default), WithAliases(f.Aliases), WithDoc(f.Doc), WithOrder(f.Order), WithProps(f.Props),
	)
	if err != nil {
		return nil, err
	}

	return field, nil
}

type enumSchema struct {
	Name      string         `mapstructure:"name"`
	Namespace string         `mapstructure:"namespace"`
	Aliases   []string       `mapstructure:"aliases"`
	Type      string         `mapstructure:"type"`
	Doc       string         `mapstructure:"doc"`
	Symbols   []string       `mapstructure:"symbols"`
	Default   string         `mapstructure:"default"`
	Props     map[string]any `mapstructure:",remain"`
}

func parseEnum(namespace string, m map[string]any, seen seenCache, cache *SchemaCache) (Schema, error) {
	var (
		e    enumSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &e, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding enum: %w", err)
	}

	if err := checkParsedName(e.Name); err != nil {
		return nil, err
	}
	if !slices.Contains(meta.Keys, "namespace") {
		e.Namespace = namespace
	}
	if err := normalizeProperties(e.Props); err != nil {
		return nil, err
	}

	opts := []SchemaOption{WithAliases(e.Aliases), WithDoc(e.Doc), WithProps(e.Props)}
	if slices.Contains(meta.Keys, "default") {
		opts = append(opts, WithDefault(e.Default))
	}
	enum, err := NewEnumSchema(e.Name, e.Namespace, e.Symbols, opts...)
	if err != nil {
		return nil, err
	}

	if err = seen.AddSchema(enum); err != nil {
		return nil, err
	}

	ref := NewRefSchema(enum)
	cache.Add(enum.FullName(), ref)
	for _, alias := range enum.Aliases() {
		cache.Add(alias, enum)
	}

	return enum, nil
}

type arraySchema struct {
	Type  string         `mapstructure:"type"`
	Items any            `mapstructure:"items"`
	Props map[string]any `mapstructure:",remain"`
}

func parseArray(namespace string, m map[string]any, seen seenCache, cache *SchemaCache) (Schema, error) {
	var (
		a    arraySchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &a, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding array: %w", err)
	}

	if !slices.Contains(meta.Keys, "items") {
		return nil, errors.New("avro: array must have an items key")
	}
	schema, err := parseType(namespace, a.Items, seen, cache)
	if err != nil {
		return nil, err
	}
	if err := normalizeProperties(a.Props); err != nil {
		return nil, err
	}

	return NewArraySchema(schema, WithProps(a.Props)), nil
}

type mapSchema struct {
	Type   string         `mapstructure:"type"`
	Values any            `mapstructure:"values"`
	Props  map[string]any `mapstructure:",remain"`
}

func parseMap(namespace string, m map[string]any, seen seenCache, cache *SchemaCache) (Schema, error) {
	var (
		ms   mapSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &ms, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding map: %w", err)
	}

	if !slices.Contains(meta.Keys, "values") {
		return nil, errors.New("avro: map must have a values key")
	}
	schema, err := parseType(namespace, ms.Values, seen, cache)
	if err != nil {
		return nil, err
	}
	if err := normalizeProperties(ms.Props); err != nil {
		return nil, err
	}

	return NewMapSchema(schema, WithProps(ms.Props)), nil
}

func parseUnion(namespace string, v []any, seen seenCache, cache *SchemaCache) (Schema, error) {
	var err error
	types := make([]Schema, len(v))
	for i := range v {
		types[i], err = parseType(namespace, v[i], seen, cache)
		if err != nil {
			return nil, err
		}
	}

	return NewUnionSchema(types)
}

type fixedSchema struct {
	Name      string         `mapstructure:"name"`
	Namespace string         `mapstructure:"namespace"`
	Aliases   []string       `mapstructure:"aliases"`
	Type      string         `mapstructure:"type"`
	Size      int            `mapstructure:"size"`
	Props     map[string]any `mapstructure:",remain"`
}

func parseFixed(namespace string, m map[string]any, seen seenCache, cache *SchemaCache) (Schema, error) {
	var (
		f    fixedSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &f, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding fixed: %w", err)
	}

	if err := checkParsedName(f.Name); err != nil {
		return nil, err
	}
	if !slices.Contains(meta.Keys, "namespace") {
		f.Namespace = namespace
	}

	if !slices.Contains(meta.Keys, "size") {
		return nil, errors.New("avro: fixed must have a size")
	}

	var logical LogicalSchema
	if logicalType := logicalTypeProperty(f.Props); logicalType != "" {
		logical = parseFixedLogicalType(f.Size, logicalType, f.Props)
		if logical != nil {
			delete(f.Props, "logicalType")
		}
	}
	if err := normalizeProperties(f.Props); err != nil {
		return nil, err
	}

	fixed, err := NewFixedSchema(f.Name, f.Namespace, f.Size, logical, WithAliases(f.Aliases), WithProps(f.Props))
	if err != nil {
		return nil, err
	}

	if err = seen.AddSchema(fixed); err != nil {
		return nil, err
	}

	ref := NewRefSchema(fixed)
	cache.Add(fixed.FullName(), ref)
	for _, alias := range fixed.Aliases() {
		cache.Add(alias, fixed)
	}

	return fixed, nil
}

func parseFixedLogicalType(size int, lt string, props map[string]any) LogicalSchema {
	ltyp := LogicalType(lt)
	switch {
	case ltyp == Duration && size == 12:
		return NewPrimitiveLogicalSchema(Duration)
	case ltyp == Decimal:
		return parseDecimalLogicalType(size, props)
	}

	return nil
}

type decimalSchema struct {
	Precision int `mapstructure:"precision"`
	Scale     int `mapstructure:"scale"`
}

func parseDecimalLogicalType(size int, props map[string]any) LogicalSchema {
	var (
		d    decimalSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(props, &d, &meta); err != nil {
		return nil
	}
	decType := newDecimalLogicalType(size, d.Precision, d.Scale)
	if decType != nil {
		// Remove the properties that we consumed
		delete(props, "precision")
		delete(props, "scale")
	}
	return decType
}

func newDecimalLogicalType(size, prec, scale int) LogicalSchema {
	if !validDecimalLogicalType(size, prec, scale) {
		return nil
	}

	return NewDecimalLogicalSchema(prec, scale)
}

func fullName(namespace, name string) string {
	if len(namespace) == 0 || strings.ContainsRune(name, '.') {
		return name
	}

	return namespace + "." + name
}

func checkParsedName(name string) error {
	if name == "" {
		return errors.New("avro: non-empty name key required")
	}
	return nil
}

func decodeMap(in, v any, meta *mapstructure.Metadata) error {
	cfg := &mapstructure.DecoderConfig{
		ZeroFields: true,
		Metadata:   meta,
		Result:     v,
		DecodeHook: func(from, to reflect.Type, data any) (any, error) {
			if from == nil || to.Kind() != reflect.Int {
				return data, nil
			}

			switch value := data.(type) {
			case stdjson.Number:
				parsed, err := strconv.ParseInt(value.String(), 10, to.Bits())
				if err != nil {
					return nil, fmt.Errorf("%v is not an integer in range: %w", value, err)
				}
				return parsed, nil
			case float64:
				limit := math.Ldexp(1, to.Bits()-1)
				if math.Trunc(value) != value || value < -limit || value >= limit {
					return nil, fmt.Errorf("%v is not an integer in range", value)
				}
			}
			return data, nil
		},
		MatchName: func(mapKey, fieldName string) bool {
			return mapKey == fieldName
		},
	}

	decoder, _ := mapstructure.NewDecoder(cfg)
	return decoder.Decode(in)
}

func derefSchema(schema Schema) Schema {
	seen := map[string]struct{}{}

	return walkSchema(schema, func(schema Schema) Schema {
		if ns, ok := schema.(NamedSchema); ok {
			if _, hasSeen := seen[ns.FullName()]; hasSeen {
				// This NamedSchema has been seen in this run, it needs
				// to be turned into a reference. It is possible it was
				// dereferenced in a previous run.
				return NewRefSchema(ns)
			}

			seen[ns.FullName()] = struct{}{}
			return schema
		}

		ref, isRef := schema.(*RefSchema)
		if !isRef {
			return schema
		}

		if _, haveSeen := seen[ref.Schema().FullName()]; !haveSeen {
			seen[ref.Schema().FullName()] = struct{}{}
			return ref.Schema()
		}
		return schema
	})
}

type seenCache map[string]struct{}

func (c seenCache) Add(name string) error {
	if _, ok := c[name]; ok {
		return fmt.Errorf("duplicate name %q", name)
	}
	c[name] = struct{}{}
	return nil
}

func (c seenCache) AddSchema(schema NamedSchema) error {
	if err := c.Add(schema.FullName()); err != nil {
		return err
	}

	aliases := map[string]struct{}{}
	for _, alias := range schema.Aliases() {
		if _, ok := aliases[alias]; ok {
			return fmt.Errorf("duplicate alias %q", alias)
		}
		aliases[alias] = struct{}{}

		if alias == schema.FullName() {
			continue
		}
		if err := c.Add(alias); err != nil {
			return err
		}
	}
	return nil
}

func logicalTypeProperty(props map[string]any) string {
	if lt, ok := props["logicalType"].(string); ok {
		return lt
	}
	return ""
}

func normalizeProperties(props map[string]any) error {
	for key, value := range props {
		normalized, err := normalizePropertyNumbers(value)
		if err != nil {
			return fmt.Errorf("avro: invalid numeric property %q: %w", key, err)
		}
		props[key] = normalized
	}
	return nil
}

func normalizePropertyNumbers(value any) (any, error) {
	switch value := value.(type) {
	case stdjson.Number:
		return value.Float64()
	case []any:
		for i, item := range value {
			normalized, err := normalizePropertyNumbers(item)
			if err != nil {
				return nil, err
			}
			value[i] = normalized
		}
	case map[string]any:
		if err := normalizeProperties(value); err != nil {
			return nil, err
		}
	}
	return value, nil
}
