package avro

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfFixed(cfg *frozenConfig, fixed *FixedSchema, typ reflect2.Type) ValDecoder {
	if err := cfg.checkFixedSize(fixed.Size()); err != nil {
		return &errorDecoder{err: err}
	}
	decimal, _ := validDecimalLogicalSchema(fixed)

	switch typ.Kind() {
	case reflect.Array:
		arrayType := typ.(reflect2.ArrayType)
		if arrayType.Elem().Kind() != reflect.Uint8 || arrayType.Len() != fixed.Size() {
			break
		}
		return &fixedCodec{arrayType: typ.(*reflect2.UnsafeArrayType), decimal: decimal}
	case reflect.Uint64:
		if fixed.Size() != 8 {
			break
		}

		return &fixedUint64Codec{decimal: decimal}
	case reflect.Ptr:
		ptrType := typ.(*reflect2.UnsafePtrType)
		elemType := ptrType.Elem()

		dec, decimal := validDecimalLogicalSchema(fixed)
		typ1 := elemType.Type1()
		if elemType.Kind() != reflect.Struct || !typ1.ConvertibleTo(ratType) || !decimal {
			break
		}
		return &fixedDecimalCodec{prec: dec.Precision(), scale: dec.Scale(), size: fixed.Size()}
	case reflect.Struct:
		ls := fixed.Logical()
		if ls == nil {
			break
		}
		typ1 := typ.Type1()
		if !typ1.ConvertibleTo(durType) || ls.Type() != Duration || fixed.Size() != 12 {
			break
		}
		return &fixedDurationCodec{}
	}

	return &errorDecoder{
		err: fmt.Errorf("avro: %s is unsupported for Avro %s, size=%d", typ.String(), fixed.Type(), fixed.Size()),
	}
}

func createEncoderOfFixed(cfg *frozenConfig, fixed *FixedSchema, typ reflect2.Type) ValEncoder {
	if err := cfg.checkFixedSize(fixed.Size()); err != nil {
		return &errorEncoder{err: err}
	}
	decimal, _ := validDecimalLogicalSchema(fixed)

	switch typ.Kind() {
	case reflect.Array:
		arrayType := typ.(reflect2.ArrayType)
		if arrayType.Elem().Kind() != reflect.Uint8 || arrayType.Len() != fixed.Size() {
			break
		}
		return &fixedCodec{arrayType: typ.(*reflect2.UnsafeArrayType), decimal: decimal}
	case reflect.Uint64:
		if fixed.Size() != 8 {
			break
		}

		return &fixedUint64Codec{decimal: decimal}
	case reflect.Ptr:
		ptrType := typ.(*reflect2.UnsafePtrType)
		elemType := ptrType.Elem()

		dec, decimal := validDecimalLogicalSchema(fixed)
		typ1 := elemType.Type1()
		if elemType.Kind() != reflect.Struct || !typ1.ConvertibleTo(ratType) || !decimal {
			break
		}
		return &fixedDecimalCodec{prec: dec.Precision(), scale: dec.Scale(), size: fixed.Size()}

	case reflect.Struct:
		ls := fixed.Logical()
		if ls == nil {
			break
		}
		typ1 := typ.Type1()
		if typ1.ConvertibleTo(durType) && ls.Type() == Duration && fixed.Size() == 12 {
			return &fixedDurationCodec{}
		}
	}

	return &errorEncoder{
		err: fmt.Errorf("avro: %s is unsupported for Avro %s, size=%d", typ.String(), fixed.Type(), fixed.Size()),
	}
}

type fixedUint64Codec struct {
	decimal *DecimalLogicalSchema
}

// checkFixedSize runs before reflection, allocation, encoding or skipping.
func (c *frozenConfig) checkFixedSize(size int) error {
	if !isAllocatableFixedSize(size) {
		return fmt.Errorf("avro: fixed size %d exceeds the platform allocation limit", size)
	}
	if limit := c.getMaxByteSliceSize(); limit > 0 && size > limit {
		return fmt.Errorf("avro: fixed size %d exceeds Config.MaxByteSliceSize (%d)", size, limit)
	}
	return nil
}

func (c *fixedUint64Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	var buffer [8]byte
	r.Read(buffer[:])
	if r.Error != nil {
		return
	}
	if err := checkRawDecimal(buffer[:], c.decimal, r.cfg.getMaxByteSliceSize()); err != nil {
		r.Error = err
		return
	}
	*(*uint64)(ptr) = binary.BigEndian.Uint64(buffer[:])
}

func (c *fixedUint64Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], *(*uint64)(ptr))
	if err := checkRawDecimal(buffer[:], c.decimal, w.cfg.getMaxByteSliceSize()); err != nil {
		w.Error = err
		return
	}
	_, _ = w.Write(buffer[:])
}

type fixedCodec struct {
	arrayType *reflect2.UnsafeArrayType
	decimal   *DecimalLogicalSchema
}

func (c *fixedCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	if c.decimal != nil {
		b := make([]byte, c.arrayType.Len())
		r.Read(b)
		if r.Error != nil {
			return
		}
		if err := checkRawDecimal(b, c.decimal, r.cfg.getMaxByteSliceSize()); err != nil {
			r.Error = err
			return
		}
		for i, value := range b {
			c.arrayType.UnsafeSetIndex(ptr, i, reflect2.PtrOf(value))
		}
		return
	}
	for i := range c.arrayType.Len() {
		c.arrayType.UnsafeSetIndex(ptr, i, reflect2.PtrOf(r.readByte()))
	}
}

func (c *fixedCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	if c.decimal != nil {
		b := make([]byte, c.arrayType.Len())
		for i := range b {
			b[i] = *(*byte)(c.arrayType.UnsafeGetIndex(ptr, i))
		}
		if err := checkRawDecimal(b, c.decimal, w.cfg.getMaxByteSliceSize()); err != nil {
			w.Error = err
			return
		}
		_, _ = w.Write(b)
		return
	}
	for i := range c.arrayType.Len() {
		bytePtr := c.arrayType.UnsafeGetIndex(ptr, i)
		w.writeByte(*((*byte)(bytePtr)))
	}
}

type fixedDecimalCodec struct {
	prec  int
	scale int
	size  int
}

func (c *fixedDecimalCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	b := make([]byte, c.size)
	r.Read(b)
	if value := r.readDecimal(b, c.prec, c.scale); value != nil {
		*((**big.Rat)(ptr)) = value
	}
}

func (c *fixedDecimalCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error != nil {
		return
	}
	value := *((**big.Rat)(ptr))
	unscaled, err := scaledDecimal(value, c.prec, c.scale, w.cfg.getMaxByteSliceSize())
	if err != nil {
		w.Error = err
		return
	}
	b := decimalBytes(unscaled)
	if len(b) > c.size {
		w.Error = fmt.Errorf("avro: decimal encodes to %d bytes, exceeds fixed size=%d", len(b), c.size)
		return
	}
	if len(b) < c.size {
		padded := make([]byte, c.size)
		if unscaled.Sign() < 0 {
			for i := range padded {
				padded[i] = 0xff
			}
		}
		copy(padded[c.size-len(b):], b)
		b = padded
	}
	_, _ = w.Write(b)
}

type fixedDurationCodec struct{}

func (*fixedDurationCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	b := make([]byte, 12)
	r.Read(b)
	var duration LogicalDuration
	duration.Months = binary.LittleEndian.Uint32(b[0:4])
	duration.Days = binary.LittleEndian.Uint32(b[4:8])
	duration.Milliseconds = binary.LittleEndian.Uint32(b[8:12])
	*((*LogicalDuration)(ptr)) = duration
}

func (*fixedDurationCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	duration := (*LogicalDuration)(ptr)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, duration.Months)
	_, _ = w.Write(b)
	binary.LittleEndian.PutUint32(b, duration.Days)
	_, _ = w.Write(b)
	binary.LittleEndian.PutUint32(b, duration.Milliseconds)
	_, _ = w.Write(b)
}
