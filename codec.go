package avro

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"time"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var (
	timeType         = reflect.TypeFor[time.Time]()
	timeDurationType = reflect.TypeFor[time.Duration]()
	ratType          = reflect.TypeFor[big.Rat]()
	durType          = reflect.TypeFor[LogicalDuration]()
)

type null struct{}

// ValDecoder represents an internal value decoder.
//
// You should never use ValDecoder directly.
type ValDecoder interface {
	Decode(ptr unsafe.Pointer, r *Reader)
}

// ValEncoder represents an internal value encoder.
//
// You should never use ValEncoder directly.
type ValEncoder interface {
	Encode(ptr unsafe.Pointer, w *Writer)
}

// ReadVal parses Avro value and stores the result in the value pointed to by obj.
func (r *Reader) ReadVal(schema Schema, obj any) {
	if r.Error != nil {
		return
	}
	if r.owner != nil && r.valDepth == 0 {
		r.cfg = r.owner.snapshot()
	}
	r.valDepth++
	defer func() { r.valDepth-- }()

	if obj == nil {
		r.ReportError("ReadVal", "can not read into nil pointer")
		return
	}
	if isNilSchema(schema) {
		r.ReportError("ReadVal", "schema cannot be nil")
		return
	}

	decoder := r.cfg.getDecoderFromCache(schema, reflect2.RTypeOf(obj))
	if decoder == nil {
		typ := reflect2.TypeOf(obj)
		if typ.Kind() != reflect.Ptr {
			r.ReportError("ReadVal", "can only unmarshal into pointer")
			return
		}
		decoder = r.cfg.DecoderOf(schema, typ)
	}

	ptr := reflect2.PtrOf(obj)
	if ptr == nil {
		r.ReportError("ReadVal", "can not read into nil pointer")
		return
	}

	decoder.Decode(ptr, r)
}

// WriteVal writes the Avro encoding of obj.
func (w *Writer) WriteVal(schema Schema, val any) {
	if w.Error != nil {
		return
	}
	if w.owner != nil && w.valDepth == 0 {
		w.cfg = w.owner.snapshot()
	}
	w.valDepth++
	defer func() { w.valDepth-- }()

	if isNilSchema(schema) {
		if w.Error == nil {
			w.Error = errors.New("avro: WriteVal: schema cannot be nil")
		}
		return
	}

	encoder := w.cfg.getEncoderFromCache(schema, reflect2.RTypeOf(val))
	if encoder == nil {
		typ := reflect2.TypeOf(val)
		encoder = w.cfg.EncoderOf(schema, typ)
	}
	encoder.Encode(reflect2.PtrOf(val), w)
}

func isNilType(typ reflect2.Type) bool {
	if typ == nil {
		return true
	}

	value := reflect.ValueOf(typ)
	return value.Kind() == reflect.Ptr && value.IsNil()
}

func supportsUnsafeType(typ reflect2.Type) bool {
	unsafeType := reflect2.Type2(typ.Type1())
	return reflect.TypeOf(typ) == reflect.TypeOf(unsafeType)
}

func (c *frozenConfig) DecoderOf(schema Schema, typ reflect2.Type) ValDecoder {
	c = c.snapshot()
	if isNilSchema(schema) {
		return &errorDecoder{err: errors.New("avro: DecoderOf: schema cannot be nil")}
	}
	if isNilType(typ) || typ.Kind() != reflect.Ptr {
		return &errorDecoder{err: errors.New("avro: DecoderOf: decoder type must be a pointer")}
	}
	if !supportsUnsafeType(typ) {
		return &errorDecoder{err: errors.New("avro: DecoderOf: decoder type must support unsafe pointer operations")}
	}
	ptrType := typ.(*reflect2.UnsafePtrType)

	rtype := typ.RType()
	decoder := c.getDecoderFromCache(schema, rtype)
	if decoder != nil {
		return decoder
	}

	decoder = decoderOfType(newDecoderContext(c), schema, ptrType.Elem())
	c.addDecoderToCache(schema, rtype, decoder)
	return decoder
}

type deferDecoder struct {
	decoder ValDecoder
}

func (d *deferDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	d.decoder.Decode(ptr, r)
}

type deferEncoder struct {
	encoder ValEncoder
}

func (d *deferEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	d.encoder.Encode(ptr, w)
}

type decoderContext struct {
	cfg      *frozenConfig
	decoders map[cacheKey]ValDecoder
}

func newDecoderContext(cfg *frozenConfig) *decoderContext {
	return &decoderContext{
		cfg:      cfg.snapshot(),
		decoders: make(map[cacheKey]ValDecoder),
	}
}

type encoderContext struct {
	cfg      *frozenConfig
	encoders map[cacheKey]ValEncoder
}

func newEncoderContext(cfg *frozenConfig) *encoderContext {
	return &encoderContext{
		cfg:      cfg.snapshot(),
		encoders: make(map[cacheKey]ValEncoder),
	}
}

//nolint:dupl
func decoderOfType(d *decoderContext, schema Schema, typ reflect2.Type) ValDecoder {
	if dec := createDecoderOfMarshaler(schema, typ); dec != nil {
		return dec
	}

	// Handle eface (empty interface) case when it isn't a union
	if typ.Kind() == reflect.Interface && schema.Type() != Union {
		if _, ok := typ.(*reflect2.UnsafeIFaceType); !ok {
			return newEfaceDecoder(d, schema)
		}
	}

	switch schema.Type() {
	case Null:
		return &nullCodec{}
	case String, Bytes, Int, Long, Float, Double, Boolean:
		return createDecoderOfNative(schema.(*PrimitiveSchema), typ)
	case Record:
		key := cacheKey{fingerprint: schema.CacheFingerprint(), rtype: typ.RType()}
		defDec := &deferDecoder{}
		d.decoders[key] = defDec
		defDec.decoder = createDecoderOfRecord(d, schema.(*RecordSchema), typ)
		return defDec.decoder
	case Ref:
		key := cacheKey{fingerprint: schema.(*RefSchema).Schema().CacheFingerprint(), rtype: typ.RType()}
		if dec, f := d.decoders[key]; f {
			return dec
		}
		return decoderOfType(d, schema.(*RefSchema).Schema(), typ)
	case Enum:
		return createDecoderOfEnum(schema.(*EnumSchema), typ)
	case Array:
		return createDecoderOfArray(d, schema.(*ArraySchema), typ)
	case Map:
		return createDecoderOfMap(d, schema.(*MapSchema), typ)
	case Union:
		return createDecoderOfUnion(d, schema.(*UnionSchema), typ)
	case Fixed:
		return createDecoderOfFixed(d.cfg, schema.(*FixedSchema), typ)
	default:
		// It is impossible to get here with a valid schema
		return &errorDecoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

func (c *frozenConfig) EncoderOf(schema Schema, typ reflect2.Type) ValEncoder {
	c = c.snapshot()
	if isNilSchema(schema) {
		return &errorEncoder{err: errors.New("avro: EncoderOf: schema cannot be nil")}
	}
	if isNilType(typ) {
		typ = reflect2.TypeOf((*null)(nil))
	}
	if !supportsUnsafeType(typ) {
		return &errorEncoder{err: errors.New("avro: EncoderOf: encoder type must support unsafe operations")}
	}

	rtype := typ.RType()
	encoder := c.getEncoderFromCache(schema, rtype)
	if encoder != nil {
		return encoder
	}

	encoder = encoderOfType(newEncoderContext(c), schema, typ)
	if typ.LikePtr() {
		encoder = &onePtrEncoder{encoder}
	}
	c.addEncoderToCache(schema, rtype, encoder)
	return encoder
}

type onePtrEncoder struct {
	enc ValEncoder
}

func (e *onePtrEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	e.enc.Encode(noescape(unsafe.Pointer(&ptr)), w)
}

//nolint:dupl
func encoderOfType(e *encoderContext, schema Schema, typ reflect2.Type) ValEncoder {
	if enc := createEncoderOfMarshaler(schema, typ); enc != nil {
		return enc
	}

	if typ.Kind() == reflect.Interface {
		return &interfaceEncoder{schema: schema, typ: typ}
	}

	switch schema.Type() {
	case Null:
		return &nullCodec{}
	case String, Bytes, Int, Long, Float, Double, Boolean:
		return createEncoderOfNative(schema.(*PrimitiveSchema), typ)
	case Record:
		key := cacheKey{fingerprint: schema.CacheFingerprint(), rtype: typ.RType()}
		defEnc := &deferEncoder{}
		e.encoders[key] = defEnc
		defEnc.encoder = createEncoderOfRecord(e, schema.(*RecordSchema), typ)
		return defEnc.encoder
	case Ref:
		key := cacheKey{fingerprint: schema.(*RefSchema).Schema().CacheFingerprint(), rtype: typ.RType()}
		if enc, f := e.encoders[key]; f {
			return enc
		}
		return encoderOfType(e, schema.(*RefSchema).Schema(), typ)
	case Enum:
		return createEncoderOfEnum(schema.(*EnumSchema), typ)
	case Array:
		return createEncoderOfArray(e, schema.(*ArraySchema), typ)
	case Map:
		return createEncoderOfMap(e, schema.(*MapSchema), typ)
	case Union:
		return createEncoderOfUnion(e, schema.(*UnionSchema), typ)
	case Fixed:
		return createEncoderOfFixed(e.cfg, schema.(*FixedSchema), typ)
	default:
		// It is impossible to get here with a valid schema
		return &errorEncoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

type errorDecoder struct {
	err error
}

func (d *errorDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	if r.Error == nil {
		r.Error = d.err
	}
}

type errorEncoder struct {
	err error
}

func (e *errorEncoder) Encode(_ unsafe.Pointer, w *Writer) {
	if w.Error == nil {
		w.Error = e.err
	}
}
