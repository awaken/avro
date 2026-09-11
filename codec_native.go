package avro

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/modern-go/reflect2"
)

//nolint:maintidx // Splitting this would not make it simpler.
func createDecoderOfNative(schema *PrimitiveSchema, typ reflect2.Type) ValDecoder {
	resolved := schema.encodedType != ""
	switch typ.Kind() {
	case reflect.Bool:
		if schema.Type() != Boolean {
			break
		}
		return &boolCodec{}

	case reflect.Int:
		switch schema.Type() {
		case Int:
			return &intCodec[int]{timeOfDay: getLogicalType(schema) == TimeMillis}
		case Long:
			if strconv.IntSize == 64 {
				// allow decoding into int when it's 64-bit
				return &longCodec[int]{timeOfDay: getLogicalType(schema) == TimeMicros}
			}
		}

	case reflect.Int8:
		if schema.Type() != Int {
			break
		}
		return &intCodec[int8]{timeOfDay: getLogicalType(schema) == TimeMillis}

	case reflect.Uint8:
		if schema.Type() != Int {
			break
		}
		return &intCodec[uint8]{timeOfDay: getLogicalType(schema) == TimeMillis}

	case reflect.Int16:
		if schema.Type() != Int {
			break
		}
		return &intCodec[int16]{timeOfDay: getLogicalType(schema) == TimeMillis}

	case reflect.Uint16:
		if schema.Type() != Int {
			break
		}
		return &intCodec[uint16]{timeOfDay: getLogicalType(schema) == TimeMillis}

	case reflect.Int32:
		if schema.Type() != Int {
			break
		}
		return &intCodec[int32]{timeOfDay: getLogicalType(schema) == TimeMillis}

	case reflect.Uint32:
		if schema.Type() != Long {
			break
		}
		if resolved {
			return &longConvCodec[uint32]{convert: createLongConverter(schema.encodedType), timeOfDay: getLogicalType(schema) == TimeMicros}
		}
		return &longCodec[uint32]{timeOfDay: getLogicalType(schema) == TimeMicros}

	case reflect.Int64:
		st := schema.Type()
		lt := getLogicalType(schema)
		switch {
		case st == Int && lt == TimeMillis: // time.Duration
			return &timeMillisCodec{}

		case st == Long && lt == TimeMicros: // time.Duration
			return &timeMicrosCodec{
				convert: createLongConverter(schema.encodedType),
			}

		case st == Long:
			isTimestamp := (lt == TimestampMillis || lt == TimestampMicros)
			if isTimestamp && typ.Type1() == timeDurationType {
				return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s and logicalType %s",
					typ.Type1().String(), schema.Type(), lt)}
			}
			if resolved {
				return &longConvCodec[int64]{convert: createLongConverter(schema.encodedType), timeOfDay: getLogicalType(schema) == TimeMicros}
			}
			return &longCodec[int64]{timeOfDay: getLogicalType(schema) == TimeMicros}

		default:
			break
		}

	case reflect.Float32:
		if schema.Type() != Float {
			break
		}
		if resolved {
			return &float32ConvCodec{convert: createFloatConverter(schema.encodedType)}
		}
		return &float32Codec{}

	case reflect.Float64:
		if schema.Type() != Double {
			break
		}
		if resolved {
			return &float64ConvCodec{convert: createDoubleConverter(schema.encodedType)}
		}
		return &float64Codec{}

	case reflect.String:
		if schema.Type() != String {
			break
		}
		return &stringCodec{}

	case reflect.Slice:
		if typ.(reflect2.SliceType).Elem().Kind() != reflect.Uint8 || schema.Type() != Bytes {
			break
		}
		decimal, _ := validDecimalLogicalSchema(schema)
		return &bytesCodec{sliceType: typ.(*reflect2.UnsafeSliceType), decimal: decimal}

	case reflect.Struct:
		st := schema.Type()
		lt := getLogicalType(schema)
		dec, decimal := validDecimalLogicalSchema(schema)
		isTime := typ.Type1().ConvertibleTo(timeType)
		switch {
		case isTime && st == Int && lt == Date:
			return &dateCodec{}
		case isTime && st == Long && lt == TimestampMillis:
			return &timestampMillisCodec{
				convert: createLongConverter(schema.encodedType),
			}
		case isTime && st == Long && lt == TimestampMicros:
			return &timestampMicrosCodec{
				convert: createLongConverter(schema.encodedType),
			}
		case isTime && st == Long && lt == LocalTimestampMillis:
			return &timestampMillisCodec{
				local:   true,
				convert: createLongConverter(schema.encodedType),
			}
		case isTime && st == Long && lt == LocalTimestampMicros:
			return &timestampMicrosCodec{
				local:   true,
				convert: createLongConverter(schema.encodedType),
			}
		case typ.Type1().ConvertibleTo(ratType) && st == Bytes && decimal:
			return &bytesDecimalCodec{prec: dec.Precision(), scale: dec.Scale()}

		default:
			break
		}
	case reflect.Ptr:
		ptrType := typ.(*reflect2.UnsafePtrType)
		elemType := ptrType.Elem()
		typ1 := elemType.Type1()
		dec, ok := validDecimalLogicalSchema(schema)
		if !ok || !typ1.ConvertibleTo(ratType) {
			break
		}

		return &bytesDecimalPtrCodec{prec: dec.Precision(), scale: dec.Scale()}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

//nolint:maintidx // Splitting this would not make it simpler.
func createEncoderOfNative(schema *PrimitiveSchema, typ reflect2.Type) ValEncoder {
	switch typ.Kind() {
	case reflect.Bool:
		if schema.Type() != Boolean {
			break
		}
		return &boolCodec{}

	case reflect.Int:
		switch schema.Type() {
		case Int:
			return &intCodec[int]{timeOfDay: getLogicalType(schema) == TimeMillis}
		case Long:
			return &longCodec[int]{timeOfDay: getLogicalType(schema) == TimeMicros}
		}

	case reflect.Int8:
		if schema.Type() != Int {
			break
		}
		return &intCodec[int8]{timeOfDay: getLogicalType(schema) == TimeMillis}

	case reflect.Uint8:
		if schema.Type() != Int {
			break
		}
		return &intCodec[uint8]{timeOfDay: getLogicalType(schema) == TimeMillis}

	case reflect.Int16:
		if schema.Type() != Int {
			break
		}
		return &intCodec[int16]{timeOfDay: getLogicalType(schema) == TimeMillis}

	case reflect.Uint16:
		if schema.Type() != Int {
			break
		}
		return &intCodec[uint16]{timeOfDay: getLogicalType(schema) == TimeMillis}

	case reflect.Int32:
		switch schema.Type() {
		case Long:
			return &longCodec[int32]{timeOfDay: getLogicalType(schema) == TimeMicros}

		case Int:
			return &intCodec[int32]{timeOfDay: getLogicalType(schema) == TimeMillis}
		}

	case reflect.Uint32:
		if schema.Type() != Long {
			break
		}
		return &longCodec[uint32]{timeOfDay: getLogicalType(schema) == TimeMicros}

	case reflect.Int64:
		st := schema.Type()
		lt := getLogicalType(schema)
		switch {
		case st == Int && lt == TimeMillis: // time.Duration
			return &timeMillisCodec{}

		case st == Long && lt == TimeMicros: // time.Duration
			return &timeMicrosCodec{}

		case st == Long:
			isTimestamp := (lt == TimestampMillis || lt == TimestampMicros)
			if isTimestamp && typ.Type1() == timeDurationType {
				return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s and logicalType %s",
					typ.Type1().String(), schema.Type(), lt)}
			}
			return &longCodec[int64]{timeOfDay: getLogicalType(schema) == TimeMicros}

		default:
			break
		}

	case reflect.Float32:
		switch schema.Type() {
		case Double:
			return &float32DoubleCodec{}
		case Float:
			return &float32Codec{}
		}

	case reflect.Float64:
		if schema.Type() != Double {
			break
		}
		return &float64Codec{}

	case reflect.String:
		if schema.Type() != String {
			break
		}
		return &stringCodec{}

	case reflect.Slice:
		if typ.(reflect2.SliceType).Elem().Kind() != reflect.Uint8 || schema.Type() != Bytes {
			break
		}
		decimal, _ := validDecimalLogicalSchema(schema)
		return &bytesCodec{sliceType: typ.(*reflect2.UnsafeSliceType), decimal: decimal}

	case reflect.Struct:
		st := schema.Type()
		lt := getLogicalType(schema)
		dec, decimal := validDecimalLogicalSchema(schema)
		isTime := typ.Type1().ConvertibleTo(timeType)
		switch {
		case isTime && st == Int && lt == Date:
			return &dateCodec{}
		case isTime && st == Long && lt == TimestampMillis:
			return &timestampMillisCodec{}
		case isTime && st == Long && lt == TimestampMicros:
			return &timestampMicrosCodec{}
		case isTime && st == Long && lt == LocalTimestampMillis:
			return &timestampMillisCodec{local: true}
		case isTime && st == Long && lt == LocalTimestampMicros:
			return &timestampMicrosCodec{local: true}
		case typ.Type1().ConvertibleTo(ratType) && st == Bytes && decimal:
			return &bytesDecimalCodec{prec: dec.Precision(), scale: dec.Scale()}
		default:
			break
		}

	case reflect.Ptr:
		ptrType := typ.(*reflect2.UnsafePtrType)
		elemType := ptrType.Elem()
		typ1 := elemType.Type1()
		dec, ok := validDecimalLogicalSchema(schema)
		if !ok || !typ1.ConvertibleTo(ratType) {
			break
		}

		return &bytesDecimalPtrCodec{prec: dec.Precision(), scale: dec.Scale()}
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func getLogicalSchema(schema Schema) LogicalSchema {
	lts, ok := schema.(LogicalTypeSchema)
	if !ok {
		return nil
	}

	return lts.Logical()
}

func getLogicalType(schema Schema) LogicalType {
	ls := getLogicalSchema(schema)
	if ls == nil {
		return ""
	}

	return ls.Type()
}

type nullCodec struct{}

func (*nullCodec) Decode(unsafe.Pointer, *Reader) {}

func (*nullCodec) Encode(unsafe.Pointer, *Writer) {}

type boolCodec struct{}

func (*boolCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*bool)(ptr)) = r.ReadBool()
}

func (*boolCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteBool(*((*bool)(ptr)))
}

type smallInt interface {
	~int | ~int8 | ~int16 | ~int32 | ~uint | ~uint8 | ~uint16
}

type intCodec[T smallInt] struct {
	timeOfDay bool
}

func (c *intCodec[T]) Decode(ptr unsafe.Pointer, r *Reader) {
	value := int64(r.ReadInt())
	if c.timeOfDay && !readTimeUnits(r, value, time.Millisecond) {
		return
	}
	setInteger[T](ptr, r, value)
}

func (c *intCodec[T]) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	value := *((*T)(ptr))
	encoded := int32(value)
	if T(encoded) != value || encoded < 0 && value > 0 {
		w.Error = fmt.Errorf("avro: %d is outside the Avro int range", value)
		return
	}
	if c.timeOfDay && !validTimeUnits(int64(encoded), time.Millisecond) {
		w.Error = errTimeOfDay
		return
	}
	w.WriteInt(encoded)
}

type largeInt interface {
	~int | ~int32 | ~uint32 | int64
}

type longCodec[T largeInt] struct {
	timeOfDay bool
}

func (c *longCodec[T]) Decode(ptr unsafe.Pointer, r *Reader) {
	value := r.ReadLong()
	if c.timeOfDay && !readTimeUnits(r, value, time.Microsecond) {
		return
	}
	setInteger[T](ptr, r, value)
}

func (c *longCodec[T]) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	value := int64(*((*T)(ptr)))
	if c.timeOfDay && !validTimeUnits(value, time.Microsecond) {
		w.Error = errTimeOfDay
		return
	}
	w.WriteLong(value)
}

type longConvCodec[T largeInt] struct {
	convert   func(*Reader) int64
	timeOfDay bool
}

func (c *longConvCodec[T]) Decode(ptr unsafe.Pointer, r *Reader) {
	value := c.convert(r)
	if c.timeOfDay && !readTimeUnits(r, value, time.Microsecond) {
		return
	}
	setInteger[T](ptr, r, value)
}

type codecInteger interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32
}

// setInteger checks narrowing and sign changes before publishing a decoded value.
func setInteger[T codecInteger](ptr unsafe.Pointer, r *Reader, value int64) {
	if r.Error != nil {
		return
	}
	converted := T(value)
	if int64(converted) != value || value < 0 && converted > 0 {
		r.ReportError("decode integer", fmt.Sprintf("%d is outside the %T range", value, converted))
		return
	}
	*((*T)(ptr)) = converted
}

type float32Codec struct{}

func (c *float32Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*float32)(ptr)) = r.ReadFloat()
}

func (*float32Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteFloat(*((*float32)(ptr)))
}

type float32ConvCodec struct {
	convert func(*Reader) float32
}

func (c *float32ConvCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*float32)(ptr)) = c.convert(r)
}

type float32DoubleCodec struct{}

func (*float32DoubleCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteDouble(float64(*((*float32)(ptr))))
}

type float64Codec struct{}

func (c *float64Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*float64)(ptr)) = r.ReadDouble()
}

func (*float64Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteDouble(*((*float64)(ptr)))
}

type float64ConvCodec struct {
	convert func(*Reader) float64
}

func (c *float64ConvCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*float64)(ptr)) = c.convert(r)
}

type stringCodec struct{}

func (c *stringCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*string)(ptr)) = r.ReadString()
}

func (*stringCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteString(*((*string)(ptr)))
}

type bytesCodec struct {
	sliceType *reflect2.UnsafeSliceType
	decimal   *DecimalLogicalSchema
}

func (c *bytesCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	b := r.ReadBytes()
	if r.Error != nil {
		return
	}
	if err := checkRawDecimal(b, c.decimal, r.cfg.getMaxByteSliceSize()); err != nil {
		r.Error = err
		return
	}
	c.sliceType.UnsafeSet(ptr, reflect2.PtrOf(b))
}

func (c *bytesCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	b := *((*[]byte)(ptr))
	if err := checkRawDecimal(b, c.decimal, w.cfg.getMaxByteSliceSize()); err != nil {
		w.Error = err
		return
	}
	w.WriteBytes(b)
}

type dateCodec struct{}

var errDateRange = errors.New("avro: date is outside the int32 day range")

func (c *dateCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	i := r.ReadInt()
	sec := int64(i) * int64(24*time.Hour/time.Second)
	*((*time.Time)(ptr)) = time.Unix(sec, 0).UTC()
}

func (c *dateCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	t := *((*time.Time)(ptr))
	year, month, day := t.Date()
	// All int32 day offsets fit within these years. Bound Unix arithmetic first.
	if year < -6_000_000 || year > 6_000_000 {
		w.Error = errDateRange
		return
	}
	civil := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	days := civil.Unix() / int64(24*time.Hour/time.Second)
	if days < math.MinInt32 || days > math.MaxInt32 {
		w.Error = errDateRange
		return
	}
	w.WriteInt(int32(days))
}

type timestampMillisCodec struct {
	local   bool
	convert func(*Reader) int64
}

func (c *timestampMillisCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	var i int64
	if c.convert != nil {
		i = c.convert(r)
	} else {
		i = r.ReadLong()
	}
	if r.Error != nil {
		return
	}
	*((*time.Time)(ptr)) = timestampFromUnits(i, time.Millisecond)
}

func (c *timestampMillisCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	value, err := timestampValue(*((*time.Time)(ptr)), time.Millisecond, c.local)
	if err != nil {
		w.Error = err
		return
	}
	w.WriteLong(value)
}

type timestampMicrosCodec struct {
	local   bool
	convert func(*Reader) int64
}

func (c *timestampMicrosCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	var i int64
	if c.convert != nil {
		i = c.convert(r)
	} else {
		i = r.ReadLong()
	}
	if r.Error != nil {
		return
	}
	*((*time.Time)(ptr)) = timestampFromUnits(i, time.Microsecond)
}

func (c *timestampMicrosCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	value, err := timestampValue(*((*time.Time)(ptr)), time.Microsecond, c.local)
	if err != nil {
		w.Error = err
		return
	}
	w.WriteLong(value)
}

var errTimestampRange = errors.New("avro: timestamp is outside the int64 unit range")

// timestampFromUnits uses UTC as a zone-free carrier for local civil clocks.
// This also preserves ambiguous or nonexistent clocks at DST transitions.
func timestampFromUnits(value int64, unit time.Duration) time.Time {
	perSecond := int64(time.Second / unit)
	return time.Unix(value/perSecond, (value%perSecond)*int64(unit)).UTC()
}

func timestampValue(t time.Time, unit time.Duration, local bool) (int64, error) {
	if !local {
		t = t.UTC()
	}
	year, month, day := t.Date()
	// Every int64 millisecond/microsecond value fits within these years.
	if year < -300_000_000 || year > 300_000_000 {
		return 0, errTimestampRange
	}
	if local {
		t = time.Date(year, month, day, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	}
	sec := t.Unix()
	perSecond := int64(time.Second / unit)
	fraction := int64(t.Nanosecond()) / int64(unit)
	if sec >= 0 {
		if sec > (math.MaxInt64-fraction)/perSecond {
			return 0, errTimestampRange
		}
		return sec*perSecond + fraction, nil
	}
	// Form a negative value from its next second to retain the partial lower bound.
	if sec+1 < math.MinInt64/perSecond {
		return 0, errTimestampRange
	}
	whole := (sec + 1) * perSecond
	remainder := perSecond - fraction
	if whole < math.MinInt64+remainder {
		return 0, errTimestampRange
	}
	return whole - remainder, nil
}

type timeMillisCodec struct{}

var errTimeOfDay = errors.New("avro: time value must be within [0,24h)")

func validTimeUnits(value int64, unit time.Duration) bool {
	return value >= 0 && value < int64(24*time.Hour/unit)
}

// readTimeUnits validates raw units before duration multiplication can overflow.
func readTimeUnits(r *Reader, value int64, unit time.Duration) bool {
	if r.Error != nil {
		return false
	}
	if !validTimeUnits(value, unit) {
		r.Error = errTimeOfDay
		return false
	}
	return true
}

func (c *timeMillisCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	i := r.ReadInt()
	if !readTimeUnits(r, int64(i), time.Millisecond) {
		return
	}
	*((*time.Duration)(ptr)) = time.Duration(i) * time.Millisecond
}

func (c *timeMillisCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	d := *((*time.Duration)(ptr))
	if d < 0 || d >= 24*time.Hour {
		w.Error = errTimeOfDay
		return
	}
	w.WriteInt(int32(d.Nanoseconds() / int64(time.Millisecond)))
}

type timeMicrosCodec struct {
	convert func(*Reader) int64
}

func (c *timeMicrosCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	var i int64
	if c.convert != nil {
		i = c.convert(r)
	} else {
		i = r.ReadLong()
	}
	if !readTimeUnits(r, i, time.Microsecond) {
		return
	}
	*((*time.Duration)(ptr)) = time.Duration(i) * time.Microsecond
}

func (c *timeMicrosCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	d := *((*time.Duration)(ptr))
	if d < 0 || d >= 24*time.Hour {
		w.Error = errTimeOfDay
		return
	}
	w.WriteLong(d.Nanoseconds() / int64(time.Microsecond))
}

type bytesDecimalCodec struct {
	prec  int
	scale int
}

func (c *bytesDecimalCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	if value := r.readDecimal(r.ReadBytes(), c.prec, c.scale); value != nil {
		*((*big.Rat)(ptr)) = *value
	}
}

func (c *bytesDecimalCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	writeBytesDecimal(w, (*big.Rat)(ptr), c.prec, c.scale)
}

type bytesDecimalPtrCodec struct {
	prec  int
	scale int
}

func (c *bytesDecimalPtrCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	if value := r.readDecimal(r.ReadBytes(), c.prec, c.scale); value != nil {
		*((**big.Rat)(ptr)) = value
	}
}

func (c *bytesDecimalPtrCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	writeBytesDecimal(w, *((**big.Rat)(ptr)), c.prec, c.scale)
}
