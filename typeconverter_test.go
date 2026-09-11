package avro_test

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
)

func TestTypeConverters_RegisterIgnoresNil(t *testing.T) {
	tests := []struct {
		name string
		conv avro.TypeConverter
	}{
		{name: "nil interface"},
		{name: "typed nil", conv: (*avro.TypeConversionFuncs)(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converters := avro.NewTypeConverters()

			assert.NotPanics(t, func() {
				converters.RegisterTypeConverters(tt.conv)
			})
			_, err := converters.DecodeTypeConvert(nil, avro.NewPrimitiveSchema(avro.Boolean, nil))
			assert.Error(t, err)
		})
	}
}

func TestTypeConverters_RejectNilSchema(t *testing.T) {
	var typedNil *avro.PrimitiveSchema
	tests := []struct {
		name   string
		schema avro.Schema
	}{
		{name: "nil interface"},
		{name: "typed nil", schema: typedNil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converters := avro.NewTypeConverters()
			assert.NotPanics(t, func() {
				_, err := converters.EncodeTypeConvert(nil, test.schema)
				assert.Error(t, err)
			})
			assert.NotPanics(t, func() {
				_, err := converters.DecodeTypeConvert(nil, test.schema)
				assert.Error(t, err)
			})
		})
	}
}

var (
	boolConverter = avro.TypeConversionFuncs{
		AvroType: avro.Boolean,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			b := in.(bool)
			if b {
				return "yes", nil
			} else {
				return "no", nil
			}
		},
	}

	floatToAvroInt = func(in any, _ avro.Schema) (any, error) {
		switch v := in.(type) {
		case float32:
			if float32(int(v)) != v {
				return 0, fmt.Errorf("%v is not an integer", in)
			}
			return int(v), nil
		case float64:
			if float64(int(v)) != v {
				return 0, fmt.Errorf("%v is not an integer", in)
			}
			return int(v), nil
		case *float64:
			if float64(int(*v)) != *v {
				return 0, fmt.Errorf("%v is not an integer", *v)
			}
			return int(*v), nil
		}
		return in, nil
	}

	intConverter = avro.TypeConversionFuncs{
		AvroType:              avro.Int,
		EncoderTypeConversion: floatToAvroInt,
	}

	fixedDecimalConverter = avro.TypeConversionFuncs{
		AvroType:        avro.Fixed,
		AvroLogicalType: avro.Decimal,
		EncoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			switch v := in.(type) {
			case string:
				val, _ := new(big.Rat).SetString(v)
				return val, nil
			}
			return in, nil
		},
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			r := in.(*big.Rat)
			f, _ := r.Float64()
			return f, nil
		},
	}

	unionConverter = avro.TypeConversionFuncs{
		AvroType: avro.Union,
		EncoderTypeConversion: func(in any, schema avro.Schema) (any, error) {
			union := schema.(*avro.UnionSchema)
			if union.Contains(avro.String) {
				return fmt.Sprintf("%v", in), nil
			}
			if union.Contains(avro.Int) {
				return floatToAvroInt(in, schema)
			}
			return in, nil
		},
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			switch v := in.(type) {
			case bool:
				if v {
					return "yes", nil
				} else {
					return "no", nil
				}
			case *big.Rat:
				f, _ := v.Float64()
				return f, nil
			}

			return in, nil
		},
	}
)

func nonConverter(typ avro.Type) avro.TypeConversionFuncs {
	return avro.TypeConversionFuncs{
		AvroType: typ,
	}
}

func errorConverter(typ avro.Type, err error) avro.TypeConversionFuncs {
	return avro.TypeConversionFuncs{
		AvroType: typ,
		EncoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			return nil, err
		},
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			return nil, err
		},
	}
}
