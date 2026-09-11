// Package gen allows generating Go structs from avro schemas.
package gen

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"unicode"
	"unicode/utf8"

	"github.com/awaken/avro/v2"
	"github.com/ettle/strcase"
	"golang.org/x/tools/imports"
)

// Config configures the code generation.
type Config struct {
	PackageName  string
	Tags         map[string]TagStyle
	FullName     bool
	Encoders     bool
	FullSchema   bool
	StrictTypes  bool
	Initialisms  []string
	LogicalTypes []LogicalType
	Metadata     any
}

// TagStyle defines the styling for a tag.
type TagStyle string

const (
	// Original is a style like whAtEVer_IS_InthEInpuT.
	Original TagStyle = "original"
	// Snake is a style like im_written_in_snake_case.
	Snake TagStyle = "snake"
	// Camel is a style like imWrittenInCamelCase.
	Camel TagStyle = "camel"
	// Kebab is a style like im-written-in-kebab-case.
	Kebab TagStyle = "kebab"
	// UpperCamel is a style like ImWrittenInUpperCamel.
	UpperCamel TagStyle = "upper-camel"
)

//go:embed output_template.tmpl
var outputTemplate string

var (
	primitiveMappings = map[avro.Type]string{
		"null":    "any",
		"string":  "string",
		"bytes":   "[]byte",
		"int":     "int",
		"long":    "int64",
		"float":   "float32",
		"double":  "float64",
		"boolean": "bool",
	}
	strictTypeMappings = map[string]string{
		"int": "int32",
	}
)

const avroImport = "github.com/awaken/avro/v2"

// Struct generates Go structs based on the schema and writes them to w.
func Struct(s string, w io.Writer, cfg Config) error {
	schema, err := avro.Parse(s)
	if err != nil {
		return err
	}
	return StructFromSchema(schema, w, cfg)
}

// StructFromSchema generates Go structs based on the schema and writes them to w.
func StructFromSchema(schema avro.Schema, w io.Writer, cfg Config) error {
	rec, ok := schema.(*avro.RecordSchema)
	if !ok || rec == nil {
		return errors.New("can only generate Go code from Record Schemas")
	}

	opts := []OptsFunc{
		WithFullName(cfg.FullName),
		WithEncoders(cfg.Encoders),
		WithInitialisms(cfg.Initialisms),
		WithStrictTypes(cfg.StrictTypes),
		WithFullSchema(cfg.FullSchema),
		WithMetadata(cfg.Metadata),
	}
	for _, opt := range cfg.LogicalTypes {
		opts = append(opts, WithLogicalType(opt))
	}
	g := NewGenerator(strcase.ToSnake(cfg.PackageName), cfg.Tags, opts...)
	g.Parse(rec)

	buf := &bytes.Buffer{}
	if err := g.Write(buf); err != nil {
		return err
	}

	formatted, err := imports.Process("", buf.Bytes(), nil)
	if err != nil {
		return fmt.Errorf("generated code could not be formatted: %w", err)
	}

	_, err = w.Write(formatted)
	return err
}

// OptsFunc is a function that configures a generator.
type OptsFunc func(*Generator)

// WithFullName uses full Avro names for generated record and enum type names.
func WithFullName(b bool) OptsFunc {
	return func(g *Generator) {
		g.fullName = b
	}
}

// WithEncoders configures the generator to generate schema and encoders on
// all objects.
func WithEncoders(b bool) OptsFunc {
	return func(g *Generator) {
		g.encoders = b
	}
}

// WithInitialisms configures the generator to use additional custom initialisms
// when styling struct and field names.
func WithInitialisms(ss []string) OptsFunc {
	return func(g *Generator) {
		g.initialisms = ss
	}
}

// WithTemplate configures the generator to use a custom template provided by the user.
func WithTemplate(template string) OptsFunc {
	return func(g *Generator) {
		if template == "" {
			return
		}
		g.template = template
	}
}

// WithStrictTypes configures the generator to use strict type sizes.
func WithStrictTypes(b bool) OptsFunc {
	return func(g *Generator) {
		g.strictTypes = b
	}
}

// WithPackageDoc configures the generator to output the given text as a package doc comment.
func WithPackageDoc(text string) OptsFunc {
	return func(g *Generator) {
		g.pkgdoc = ensureTrailingPeriod(text)
	}
}

// WithFullSchema configures the generator to store the full schema within the generation context.
func WithFullSchema(b bool) OptsFunc {
	return func(g *Generator) {
		g.fullSchema = b
	}
}

// WithMetadata configures the generator to store the metadata within the generation context.
func WithMetadata(m any) OptsFunc {
	return func(g *Generator) {
		g.metadata = m
	}
}

// WithEnums configures the generator to output the enum symbols.
func WithEnums(b bool) OptsFunc {
	return func(g *Generator) {
		g.genEnums = b
	}
}

// LogicalType used when the name of the "LogicalType" field in the Avro schema matches the Name attribute.
type LogicalType struct {
	// Name of the LogicalType
	Name string
	// Typ returned, has to be a valid Go type
	Typ string
	// Import added as import (if not empty)
	Import string
	// ThirdPartyImport added as import (if not empty)
	ThirdPartyImport string
}

// WithLogicalType registers a LogicalType which takes precedence over the default logical types
// defined by this package.
func WithLogicalType(logicalType LogicalType) OptsFunc {
	return func(g *Generator) {
		if g.logicalTypes == nil {
			g.logicalTypes = map[avro.LogicalType]LogicalType{}
		}
		g.logicalTypes[avro.LogicalType(logicalType.Name)] = logicalType
	}
}

func ensureTrailingPeriod(text string) string {
	if text == "" {
		return text
	}
	if last, _ := utf8.DecodeLastRuneInString(text); last == '.' {
		return text
	}
	return text + "."
}

// Generator generates Go structs from schemas.
type Generator struct {
	template     string
	pkg          string
	pkgdoc       string
	tags         map[string]TagStyle
	fullName     bool
	encoders     bool
	fullSchema   bool
	strictTypes  bool
	genEnums     bool
	initialisms  []string
	logicalTypes map[avro.LogicalType]LogicalType
	metadata     any
	err          error

	imports           []string
	thirdPartyImports []string
	typedefs          []typedef
	typeenums         []typeenum
	nameCaser         *strcase.Caser
	roots             []avro.Schema
	named             map[string]avro.NamedSchema
	checked           map[avro.NamedSchema]bool
	visited           map[avro.Schema]bool
	declared          map[string]bool
	symbols           map[string]string
}

// NewGenerator returns a generator.
func NewGenerator(pkg string, tags map[string]TagStyle, opts ...OptsFunc) *Generator {
	clonedTags := maps.Clone(tags)
	delete(clonedTags, "avro")

	g := &Generator{
		template: outputTemplate,
		pkg:      pkg,
		tags:     clonedTags,
	}
	g.resetGraph()

	for _, opt := range opts {
		opt(g)
	}

	initialisms := map[string]bool{}
	for _, v := range g.initialisms {
		initialisms[v] = true
	}

	g.nameCaser = strcase.NewCaser(
		true, // use standard Golint's initialisms
		initialisms,
		nil, // use default word split function
	)

	return g
}

// Reset reset the generator.
func (g *Generator) Reset() {
	g.imports = g.imports[:0]
	g.thirdPartyImports = g.thirdPartyImports[:0]
	g.typedefs = g.typedefs[:0]
	g.typeenums = g.typeenums[:0]
	g.err = nil
	g.resetGraph()
}

// Parse visits all reachable schema definitions, including references.
// Equivalent repeated definitions are emitted once; the first metadata is kept.
// Generation errors are retained until Reset and returned by Write.
func (g *Generator) Parse(schema avro.Schema) {
	g.ParseWithMetadata(schema, nil)
}

// ParseWithMetadata parses an avro schema into Go types with arbitrary metadata attached.
// The metadata is then passed to the template as `Typedefs[].Metadata`.
func (g *Generator) ParseWithMetadata(schema avro.Schema, metadata any) {
	g.roots = append(g.roots, schema)
	_ = g.generate(schema, metadata)
}

func (g *Generator) generate(schema avro.Schema, metadata any) string {
	if isNilSchema(schema) {
		if g.err == nil {
			g.err = errors.New("cannot generate Go code from a nil schema")
		}
		return ""
	}
	if g.err != nil {
		return ""
	}
	if named, ok := schema.(avro.NamedSchema); ok && !g.registerNamed(named) {
		return ""
	}

	switch s := schema.(type) {
	case *avro.RefSchema:
		return g.resolveRefSchema(s, metadata)
	case *avro.RecordSchema:
		return g.resolveRecordSchema(s, metadata)
	case *avro.NullSchema:
		return "any"
	case *avro.PrimitiveSchema:
		typ := primitiveMappings[s.Type()]
		if g.strictTypes {
			if newTyp, ok := strictTypeMappings[typ]; ok {
				typ = newTyp
			}
		}
		if ls := s.Logical(); ls != nil && validLogicalSchema(s, ls) {
			if logicalType := g.resolveLogicalSchema(ls.Type()); logicalType != "" {
				typ = logicalType
			}
		}
		return typ
	case *avro.ArraySchema:
		return "[]" + g.generate(s.Items(), metadata)
	case *avro.EnumSchema:
		if g.genEnums {
			return g.resolveEnum(s)
		}
		return "string"
	case *avro.FixedSchema:
		typ := fmt.Sprintf("[%d]byte", s.Size())
		if ls := s.Logical(); ls != nil && validLogicalSchema(s, ls) {
			if logicalType := g.resolveLogicalSchema(ls.Type()); logicalType != "" {
				typ = logicalType
			}
		}
		return typ
	case *avro.MapSchema:
		return "map[string]" + g.generate(s.Values(), metadata)
	case *avro.UnionSchema:
		return g.resolveUnionTypes(s, metadata)
	default:
		if g.err == nil {
			g.err = fmt.Errorf("unsupported schema implementation %T", schema)
		}
		return ""
	}
}

func isNilSchema(schema avro.Schema) bool {
	if schema == nil {
		return true
	}

	value := reflect.ValueOf(schema)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func validLogicalSchema(schema avro.Schema, logical avro.LogicalSchema) bool {
	switch logical.Type() {
	case avro.UUID:
		return schema.Type() == avro.String
	case avro.Date, avro.TimeMillis:
		return schema.Type() == avro.Int
	case avro.TimeMicros, avro.TimestampMillis, avro.TimestampMicros,
		avro.LocalTimestampMillis, avro.LocalTimestampMicros:
		return schema.Type() == avro.Long
	case avro.Duration:
		fixed, ok := schema.(*avro.FixedSchema)
		return ok && fixed.Size() == 12
	case avro.Decimal:
		return validDecimalLogicalSchema(schema, logical)
	default:
		return true
	}
}

func validDecimalLogicalSchema(schema avro.Schema, logical avro.LogicalSchema) bool {
	decimal, ok := logical.(*avro.DecimalLogicalSchema)
	if !ok || decimal == nil {
		return false
	}

	size := -1
	switch s := schema.(type) {
	case *avro.PrimitiveSchema:
		if s.Type() != avro.Bytes {
			return false
		}
	case *avro.FixedSchema:
		size = s.Size()
	default:
		return false
	}

	precision, scale := decimal.Precision(), decimal.Scale()
	if precision <= 0 || size == 0 || scale < 0 || scale > precision {
		return false
	}
	if size > 0 {
		maxPrecision := math.Floor(math.Log10(2) * (8*float64(size) - 1))
		if float64(precision) > maxPrecision {
			return false
		}
	}

	return true
}

func sanitizeIdentifier(name string) string {
	var identifier strings.Builder
	for _, char := range name {
		if char == '_' || unicode.IsLetter(char) || unicode.IsDigit(char) {
			identifier.WriteRune(char)
		}
	}

	value := identifier.String()
	if value == "" {
		return "_"
	}
	first, _ := utf8.DecodeRuneInString(value)
	if unicode.IsDigit(first) {
		return "_" + value
	}
	return value
}

func (g *Generator) resolveEnum(s *avro.EnumSchema) string {
	name := g.resolveTypeName(s)
	if !g.declared[s.FullName()] {
		g.claimName(g.symbols, name, "enum "+s.FullName())
		for _, symbol := range s.Symbols() {
			g.claimName(g.symbols, g.enumConstName(name, symbol), "symbol "+s.FullName()+"."+symbol)
		}
		g.typeenums = append(g.typeenums, newTypeEnum(name, s.Symbols()))
		g.declared[s.FullName()] = true
	}
	return name
}

func (g *Generator) resolveTypeName(s avro.NamedSchema) string {
	if g.fullName {
		return sanitizeIdentifier(g.nameCaser.ToPascal(s.FullName()))
	}
	return sanitizeIdentifier(g.nameCaser.ToPascal(s.Name()))
}

func (g *Generator) resolveRecordSchema(schema *avro.RecordSchema, metadata any) string {
	typeName := g.resolveTypeName(schema)
	if g.visited[schema] {
		return typeName
	}
	// Mark the identity before visiting references, including mutual recursion.
	g.visited[schema] = true
	g.claimName(g.symbols, typeName, "record "+schema.FullName())
	names := map[string]string{}
	if g.encoders {
		for _, method := range []string{"Schema", "Marshal", "Unmarshal"} {
			names[method] = "generated method " + method
		}
	}
	fields := make([]field, len(schema.Fields()))
	for i, f := range schema.Fields() {
		typ := g.generate(f.Type(), metadata)
		name := sanitizeIdentifier(g.nameCaser.ToPascal(f.Name()))
		g.claimName(names, name, "field "+schema.FullName()+"."+f.Name())
		fields[i] = g.newField(name, typ, f.Doc(), f.Name(), f.Props())
	}

	if g.err != nil {
		return typeName
	}
	if !g.declared[schema.FullName()] {
		def := newType(typeName, schema.Doc(), fields, g.rawSchema(schema), schema.Props(), metadata)
		def.FullName = schema.FullName()
		g.typedefs = append(g.typedefs, def)
		g.declared[schema.FullName()] = true
	}
	return typeName
}

func (g *Generator) rawSchema(schema *avro.RecordSchema) string {
	if g.fullSchema {
		schemaJSON, err := schema.MarshalJSON()
		if err != nil {
			if g.err == nil {
				g.err = fmt.Errorf("failed to marshal raw schema for '%s': %w", schema.FullName(), err)
			}
			return ""
		}
		return string(schemaJSON)
	}
	return avro.LegacyParsingCanonicalForm(schema)
}

func (g *Generator) resolveRefSchema(s *avro.RefSchema, metadata any) string {
	return g.generate(s.Schema(), metadata)
}

func (g *Generator) resolveUnionTypes(s *avro.UnionSchema, metadata any) string {
	types := make([]string, 0, len(s.Types()))
	for _, elem := range s.Types() {
		if elem.Type() == avro.Null {
			continue
		}
		types = append(types, g.generate(elem, metadata))
	}
	if s.Nullable() {
		return "*" + types[0]
	}
	return "any"
}

func (g *Generator) resolveLogicalSchema(logicalType avro.LogicalType) string {
	if g.logicalTypes != nil {
		if typ, ok := g.logicalTypes[logicalType]; ok {
			if val := typ.Import; val != "" {
				g.addImport(val)
			}
			if val := typ.ThirdPartyImport; val != "" {
				g.addThirdPartyImport(val)
			}

			return typ.Typ
		}
	}

	var typ string
	switch logicalType {
	case "date", "timestamp-millis", "timestamp-micros", "local-timestamp-millis", "local-timestamp-micros":
		typ = "time.Time"
	case "time-millis", "time-micros":
		typ = "time.Duration"
	case "decimal":
		typ = "*big.Rat"
	case "duration":
		typ = "avro.LogicalDuration"
	case "uuid":
		typ = "string"
	}
	if strings.Contains(typ, "time") {
		g.addImport("time")
	}
	if strings.Contains(typ, "big") {
		g.addImport("math/big")
	}
	if strings.Contains(typ, "avro") {
		g.addThirdPartyImport(avroImport)
	}
	return typ
}

func (g *Generator) newField(name, typ, doc, avroFieldName string, props map[string]any) field {
	return field{
		Name:          name,
		Type:          typ,
		AvroFieldName: avroFieldName,
		Doc:           ensureTrailingPeriod(doc),
		Tags:          g.tags,
		Props:         props,
	}
}

func (g *Generator) addImport(pkg string) {
	if slices.Contains(g.imports, pkg) {
		return
	}
	g.imports = append(g.imports, pkg)
}

func (g *Generator) addThirdPartyImport(pkg string) {
	if slices.Contains(g.thirdPartyImports, pkg) {
		return
	}
	g.thirdPartyImports = append(g.thirdPartyImports, pkg)
}

// Write writes Go code from the parsed schemas. Conflicting normalized names,
// inaccessible identifiers and recursive value types fail before output is written.
// WithEncoders initializes reachable schemas in a private cache, without changing
// avro.DefaultSchemaCache. Each Schema method returns its named record schema.
func (g *Generator) Write(w io.Writer) error {
	if err := validateStructTagNames(g.tags); err != nil {
		return err
	}
	if g.err != nil {
		return g.err
	}
	if err := g.checkValueCycles(); err != nil {
		g.err = err
		return err
	}
	schemaDefinitions, err := g.schemaDefinitions()
	if err != nil {
		g.err = err
		return err
	}
	schemaCacheName := ""
	if len(g.typedefs) > 0 {
		schemaCacheName = "avroSchemaCache" + g.typedefs[0].Name
	}

	parsed, err := template.New("out").
		Funcs(template.FuncMap{
			"kebab":         strcase.ToKebab,
			"upperCamel":    strcase.ToPascal,
			"camel":         strcase.ToCamel,
			"snake":         strcase.ToSnake,
			"replace":       strings.Replace,
			"buildTag":      buildTag,
			"enumConstName": g.enumConstName,
		}).
		Parse(g.template)
	if err != nil {
		return err
	}

	thirdPartyImports := slices.Clone(g.thirdPartyImports)
	if g.encoders {
		thirdPartyImports = slices.DeleteFunc(thirdPartyImports, func(pkg string) bool {
			return pkg == avroImport
		})
		thirdPartyImports = append([]string{avroImport}, thirdPartyImports...)
	}
	imports := slices.Concat(g.imports, thirdPartyImports)

	data := struct {
		WithEncoders      bool
		PackageName       string
		PackageDoc        string
		Imports           []string
		ThirdPartyImports []string
		Typedefs          []typedef
		Metadata          any
		Typeenums         []typeenum
		SchemaDefinitions []string
		SchemaCacheName   string
	}{
		WithEncoders:      g.encoders,
		PackageName:       g.pkg,
		PackageDoc:        g.pkgdoc,
		Imports:           imports,
		Typedefs:          g.typedefs,
		Metadata:          g.metadata,
		Typeenums:         g.typeenums,
		SchemaDefinitions: schemaDefinitions,
		SchemaCacheName:   schemaCacheName,
	}
	// Template errors must not publish incomplete Go source.
	var output bytes.Buffer
	if err := parsed.Execute(&output, data); err != nil {
		return err
	}
	_, err = w.Write(output.Bytes())
	return err
}

type typedef struct {
	Name     string
	FullName string
	Doc      string
	Fields   []field
	Schema   string
	Props    map[string]any
	Metadata any
}

func newType(name, doc string, fields []field, schema string, props map[string]any, metadata any) typedef {
	return typedef{
		Name:     name,
		Doc:      ensureTrailingPeriod(doc),
		Fields:   fields,
		Schema:   schema,
		Props:    props,
		Metadata: metadata,
	}
}

type field struct {
	Name          string
	Type          string
	Doc           string
	AvroFieldName string
	Tags          map[string]TagStyle
	Props         map[string]any
}

func buildTag(f field) string {
	names := make([]string, 0, len(f.Tags))
	for name := range f.Tags {
		names = append(names, name)
	}
	sort.Strings(names)

	var tag strings.Builder
	tag.WriteString("avro:")
	tag.WriteString(strconv.Quote(f.AvroFieldName))
	for _, name := range names {
		value := f.AvroFieldName
		switch f.Tags[name] {
		case Kebab:
			value = strcase.ToKebab(value)
		case UpperCamel:
			value = strcase.ToPascal(value)
		case Camel:
			value = strcase.ToCamel(value)
		case Snake:
			value = strcase.ToSnake(value)
		}
		tag.WriteByte(' ')
		tag.WriteString(name)
		tag.WriteByte(':')
		tag.WriteString(strconv.Quote(value))
	}

	value := tag.String()
	if strings.ContainsRune(value, '`') {
		return strconv.Quote(value)
	}
	return "`" + value + "`"
}

func validateStructTagNames(tags map[string]TagStyle) error {
	names := slices.Sorted(maps.Keys(tags))
	for _, name := range names {
		if name == "" || !utf8.ValidString(name) || strings.IndexFunc(name, func(r rune) bool {
			return r <= ' ' || r == ':' || r == '"' || r == 0x7f
		}) >= 0 {
			return fmt.Errorf("invalid struct tag name %q", name)
		}
	}

	return nil
}

func (g *Generator) enumConstName(typeName, symbol string) string {
	return sanitizeIdentifier(typeName + g.nameCaser.ToPascal(symbol))
}

type typeenum struct {
	Name    string
	Symbols []string
}

func newTypeEnum(name string, symbols []string) typeenum {
	return typeenum{
		Name:    name,
		Symbols: symbols,
	}
}
