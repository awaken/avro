package avro

import (
	"fmt"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var defaultEncodingConfig = Config{}.Freeze().(*frozenConfig).snapshot()

func createDefaultDecoder(d *decoderContext, field *Field, typ reflect2.Type) ValDecoder {
	b, err := encodeDefault(field)
	if err != nil {
		return &errorDecoder{err: fmt.Errorf("decode default: %w", err)}
	}
	return &defaultDecoder{
		data:    b,
		decoder: decoderOfType(d, field.Type(), typ),
	}
}

func encodeDefault(field *Field) ([]byte, error) {
	cfg := defaultEncodingConfig

	return field.encodeDefault(func(def any) ([]byte, error) {
		if field.Type().Type() == Union {
			def = recordUnionDefault(field)
		}

		defaultType := reflect2.TypeOf(def)
		if defaultType == nil {
			defaultType = reflect2.TypeOf((*null)(nil))
		}
		defaultEncoder := encoderOfType(newEncoderContext(cfg), field.Type(), defaultType)
		if defaultType.LikePtr() {
			defaultEncoder = &onePtrEncoder{defaultEncoder}
		}
		w := cfg.borrowWriter()
		defer cfg.returnWriter(w)

		defaultEncoder.Encode(reflect2.PtrOf(def), w)
		if w.Error != nil {
			return nil, w.Error
		}
		b := w.Buffer()
		data := make([]byte, len(b))
		copy(data, b)

		return data, nil
	})
}

type defaultDecoder struct {
	data    []byte
	decoder ValDecoder
}

// Decode implements ValDecoder.
func (d *defaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	rr := r.cfg.borrowReader(d.data)
	defer r.cfg.returnReader(rr)

	d.decoder.Decode(ptr, rr)
	if rr.Error != nil && r.Error == nil {
		r.Error = fmt.Errorf("decode default: %w", rr.Error)
	}
}

var _ ValDecoder = &defaultDecoder{}
