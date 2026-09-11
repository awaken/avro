package avro

import (
	"encoding"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var (
	textMarshalerType   = reflect2.TypeOfPtr((*encoding.TextMarshaler)(nil)).Elem()
	textUnmarshalerType = reflect2.TypeOfPtr((*encoding.TextUnmarshaler)(nil)).Elem()
)

func createDecoderOfMarshaler(schema Schema, typ reflect2.Type) ValDecoder {
	if typ.Implements(textUnmarshalerType) && schema.Type() == String {
		return &textMarshalerCodec{typ}
	}
	ptrType := reflect2.PtrTo(typ)
	if ptrType.Implements(textUnmarshalerType) && schema.Type() == String {
		return &referenceDecoder{
			&textMarshalerCodec{ptrType},
		}
	}
	return nil
}

func createEncoderOfMarshaler(schema Schema, typ reflect2.Type) ValEncoder {
	if typ.Implements(textMarshalerType) && schema.Type() == String {
		return &textMarshalerCodec{
			typ: typ,
		}
	}
	return nil
}

type textMarshalerCodec struct {
	typ reflect2.Type
}

func (c textMarshalerCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	b := r.ReadBytes()
	if r.Error != nil {
		return
	}

	obj := c.typ.UnsafeIndirect(ptr)
	if reflect2.IsNil(obj) {
		ptrType, ok := c.typ.(*reflect2.UnsafePtrType)
		if !ok {
			r.ReportError("textMarshalerCodec", "cannot initialize type "+c.typ.String())
			return
		}
		newPtr := ptrType.Elem().UnsafeNew()
		*((*unsafe.Pointer)(ptr)) = newPtr
		obj = c.typ.UnsafeIndirect(ptr)
	}
	unmarshaler := (obj).(encoding.TextUnmarshaler)
	err := unmarshaler.UnmarshalText(b)
	if err != nil {
		r.ReportError("textMarshalerCodec", err.Error())
	}
}

func (c textMarshalerCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.typ.UnsafeIndirect(ptr)
	if c.typ.IsNullable() && reflect2.IsNil(obj) {
		w.WriteBytes(nil)
		return
	}
	marshaler := (obj).(encoding.TextMarshaler)
	b, err := marshaler.MarshalText()
	if err != nil {
		w.Error = err
		return
	}
	w.WriteBytes(b)
}
