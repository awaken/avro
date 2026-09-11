package avro

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"time"
)

// ReadNext reads the next Avro element as a generic interface.
func (r *Reader) ReadNext(schema Schema) any {
	if isNilSchema(schema) {
		r.ReportError("ReadNext", "schema cannot be nil")
		return nil
	}

	var ls LogicalSchema
	lts, ok := schema.(LogicalTypeSchema)
	if ok {
		ls = lts.Logical()
	}

	switch schema.Type() {
	case Null:
		return nil
	case Boolean:
		return r.ReadBool()
	case Int:
		if ls != nil {
			switch ls.Type() {
			case Date:
				i := r.ReadInt()
				sec := int64(i) * int64(24*time.Hour/time.Second)
				return time.Unix(sec, 0).UTC()

			case TimeMillis:
				value := r.ReadInt()
				if !readTimeUnits(r, int64(value), time.Millisecond) {
					return nil
				}
				return time.Duration(value) * time.Millisecond
			}
		}
		return int(r.ReadInt())
	case Long:
		readLong := r.ReadLong
		if encodedType := schema.(*PrimitiveSchema).encodedType; encodedType != "" {
			if convert := createLongConverter(encodedType); convert != nil {
				readLong = func() int64 { return convert(r) }
			}
		}
		if ls != nil {
			switch ls.Type() {
			case TimeMicros:
				value := readLong()
				if !readTimeUnits(r, value, time.Microsecond) {
					return nil
				}
				return time.Duration(value) * time.Microsecond

			case TimestampMillis, LocalTimestampMillis:
				i := readLong()
				if r.Error != nil {
					return nil
				}
				return timestampFromUnits(i, time.Millisecond)

			case TimestampMicros, LocalTimestampMicros:
				i := readLong()
				if r.Error != nil {
					return nil
				}
				return timestampFromUnits(i, time.Microsecond)
			}
		}
		return readLong()
	case Float:
		if encodedType := schema.(*PrimitiveSchema).encodedType; encodedType != "" {
			if convert := createFloatConverter(encodedType); convert != nil {
				return convert(r)
			}
		}
		return r.ReadFloat()
	case Double:
		if encodedType := schema.(*PrimitiveSchema).encodedType; encodedType != "" {
			if convert := createDoubleConverter(encodedType); convert != nil {
				return convert(r)
			}
		}
		return r.ReadDouble()
	case String:
		return r.ReadString()
	case Bytes:
		if dec, ok := validDecimalLogicalSchema(schema); ok {
			value := r.readDecimal(r.ReadBytes(), dec.Precision(), dec.Scale())
			if r.Error != nil {
				return nil
			}
			return value
		}
		return r.ReadBytes()
	case Record:
		fields := schema.(*RecordSchema).Fields()
		obj := make(map[string]any, len(fields))
		for _, field := range fields {
			switch field.action {
			case FieldIgnore:
				createSkipDecoder(field.Type()).Decode(nil, r)
				if r.Error != nil {
					return obj
				}
				continue
			case FieldSetDefault:
				obj[field.Name()] = r.readDefault(field)
				if r.Error != nil {
					return obj
				}
				continue
			}

			obj[field.Name()] = r.ReadNext(field.Type())
			if r.Error != nil {
				return obj
			}
		}
		return obj
	case Ref:
		return r.ReadNext(schema.(*RefSchema).Schema())
	case Enum:
		idx := int(r.ReadInt())
		symbol, ok := schema.(*EnumSchema).Symbol(idx)
		if !ok {
			r.ReportError("Read", "unknown enum symbol")
			return nil
		}
		return symbol
	case Array:
		arr := []any{}
		r.ReadArrayCB(func(r *Reader) bool {
			elem := r.ReadNext(schema.(*ArraySchema).Items())
			arr = append(arr, elem)
			return true
		})
		return arr
	case Map:
		obj := map[string]any{}
		r.ReadMapCB(func(r *Reader, field string) bool {
			elem := r.ReadNext(schema.(*MapSchema).Values())
			obj[field] = elem
			return true
		})
		return obj
	case Union:
		types := schema.(*UnionSchema).Types()
		idx64 := r.ReadLong()
		if idx64 < 0 || idx64 > int64(len(types)-1) {
			r.ReportError("Read", "unknown union type")
			return nil
		}
		idx := int(idx64)
		schema = types[idx]
		if schema.Type() == Null {
			return nil
		}

		key := schemaTypeName(schema)
		obj := map[string]any{}
		obj[key] = r.ReadNext(types[idx])
		return obj
	case Fixed:
		size := schema.(*FixedSchema).Size()
		if err := r.cfg.checkFixedSize(size); err != nil {
			r.ReportError("Read", err.Error())
			return nil
		}
		obj := make([]byte, size)
		r.Read(obj)
		if ls != nil {
			switch ls.Type() {
			case Decimal:
				if dec, ok := validDecimalLogicalSchema(schema); ok {
					value := r.readDecimal(obj, dec.Precision(), dec.Scale())
					if r.Error != nil {
						return nil
					}
					return value
				}
			case Duration:
				if size == 12 {
					return LogicalDuration{
						Months:       binary.LittleEndian.Uint32(obj[0:4]),
						Days:         binary.LittleEndian.Uint32(obj[4:8]),
						Milliseconds: binary.LittleEndian.Uint32(obj[8:12]),
					}
				}
			}
		}
		return byteSliceToArray(obj, size)
	default:
		r.ReportError("Read", fmt.Sprintf("unexpected schema type: %v", schema.Type()))
		return nil
	}
}

func (r *Reader) readDefault(field *Field) any {
	b, err := encodeDefault(field)
	if err != nil {
		r.Error = fmt.Errorf("decode default: %w", err)
		return nil
	}

	rr := r.cfg.borrowReader(b)
	defer r.cfg.returnReader(rr)

	obj := rr.ReadNext(field.Type())
	if rr.Error != nil {
		r.Error = fmt.Errorf("decode default: %w", rr.Error)
	}
	return obj
}

// ReadArrayCB reads an array with a callback per item.
func (r *Reader) ReadArrayCB(fn func(*Reader) bool) {
	remaining := int64(r.cfg.getMaxSliceAllocSize())
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 || r.Error != nil {
			break
		}
		if l > remaining {
			r.ReportError("ReadArrayCB", "size is greater than `Config.MaxSliceAllocSize`")
			return
		}
		remaining -= l
		for range l {
			if !fn(r) || r.Error != nil {
				return
			}
		}
	}
}

// ReadMapCB reads a map with a callback per item.
func (r *Reader) ReadMapCB(fn func(*Reader, string) bool) {
	remaining := int64(r.cfg.getMaxMapAllocSize())
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 || r.Error != nil {
			break
		}
		if l > remaining {
			r.ReportError("ReadMapCB", "size is greater than `Config.MaxMapAllocSize`")
			return
		}
		remaining -= l

		for range l {
			field := r.ReadString()
			if r.Error != nil || !fn(r, field) || r.Error != nil {
				return
			}
		}
	}
}

var byteType = reflect.TypeFor[byte]()

func byteSliceToArray(b []byte, size int) any {
	vArr := reflect.New(reflect.ArrayOf(size, byteType)).Elem()
	reflect.Copy(vArr, reflect.ValueOf(b))
	return vArr.Interface()
}
