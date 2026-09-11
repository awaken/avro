package avro_test

import (
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/awaken/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNativeIntegerDecodeBounds(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
		target any
	}{
		{"int8_positive", `"int"`, int32(128), int8(7)},
		{"int8_negative", `"int"`, int32(-129), int8(7)},
		{"int16_positive", `"int"`, int32(32768), int16(7)},
		{"int16_negative", `"int"`, int32(-32769), int16(7)},
		{"uint8_negative", `"int"`, int32(-1), uint8(7)},
		{"uint8_positive", `"int"`, int32(256), uint8(7)},
		{"uint16_negative", `"int"`, int32(-1), uint16(7)},
		{"uint16_positive", `"int"`, int32(65536), uint16(7)},
		{"uint32_negative", `"long"`, int64(-1), uint32(7)},
		{"uint32_positive", `"long"`, int64(math.MaxUint32) + 1, uint32(7)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := avro.MustParse(tt.schema)
			wire, err := avro.Marshal(schema, tt.value)
			require.NoError(t, err)
			dst := reflect.New(reflect.TypeOf(tt.target))
			dst.Elem().Set(reflect.ValueOf(tt.target))
			err = avro.Unmarshal(schema, wire, dst.Interface())
			assert.Error(t, err)
			assert.Equal(t, tt.target, dst.Elem().Interface(), "failure must leave the destination unchanged")
		})
	}
	reader := avro.MustParse(`"long"`)
	writer := avro.MustParse(`"int"`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)
	wire, err := avro.Marshal(writer, int32(-1))
	require.NoError(t, err)
	got := uint32(7)
	assert.Error(t, avro.Unmarshal(resolved, wire, &got))
	assert.Equal(t, uint32(7), got)
}

func TestNativeIntegerEncodeBounds(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Skip("native int cannot exceed Avro int on 32-bit systems")
	}
	schema := avro.MustParse(`"int"`)
	for _, value := range []int64{int64(math.MaxInt32) + 1, int64(math.MinInt32) - 1} {
		_, err := avro.Marshal(schema, int(value))
		assert.Error(t, err)
	}
}

func TestNativeIntegerExactBounds(t *testing.T) {
	for _, value := range []any{int8(-128), int8(127), int16(-32768), int16(32767), uint8(255), uint16(65535), int32(math.MinInt32), int32(math.MaxInt32), int(0), uint32(math.MaxUint32), int64(math.MinInt64), int64(math.MaxInt64)} {
		schema := avro.MustParse(`"int"`)
		switch value.(type) {
		case uint32, int64:
			schema = avro.MustParse(`"long"`)
		}
		wire, err := avro.Marshal(schema, value)
		require.NoError(t, err)
		dst := reflect.New(reflect.TypeOf(value))
		require.NoError(t, avro.Unmarshal(schema, wire, dst.Interface()))
		assert.Equal(t, value, dst.Elem().Interface())
	}
}

func TestDateCivilDay(t *testing.T) {
	schema := avro.MustParse(`{"type":"int","logicalType":"date"}`)
	for _, day := range []int64{-719528, -1, 0, 11016, math.MinInt32, math.MaxInt32} {
		civil := time.Unix(day*86400, 0).UTC()
		for _, offset := range []int{-12 * 3600, 0, 14 * 3600} {
			input := time.Date(civil.Year(), civil.Month(), civil.Day(), 23, 59, 59, 1, time.FixedZone("offset", offset))
			wire, err := avro.Marshal(schema, input)
			require.NoError(t, err)
			var got int32
			require.NoError(t, avro.Unmarshal(avro.MustParse(`"int"`), wire, &got))
			assert.Equal(t, int32(day), got, "civil date %v", input)
		}
	}
}

func TestDateRejectsOverflow(t *testing.T) {
	schema := avro.MustParse(`{"type":"int","logicalType":"date"}`)
	for _, value := range []time.Time{
		time.Unix((int64(math.MaxInt32)+1)*86400, 0).UTC(),
		time.Unix((int64(math.MinInt32)-1)*86400, 0).UTC(),
		time.Date(10_000_000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(-10_000_000, 1, 1, 0, 0, 0, 0, time.UTC),
	} {
		_, err := avro.Marshal(schema, value)
		assert.Error(t, err)
	}
}

func TestTimeOfDayBounds(t *testing.T) {
	for _, unit := range []time.Duration{time.Millisecond, time.Microsecond} {
		schema := avro.MustParse(`{"type":"int","logicalType":"time-millis"}`)
		plain := avro.MustParse(`"int"`)
		if unit == time.Microsecond {
			schema = avro.MustParse(`{"type":"long","logicalType":"time-micros"}`)
			plain = avro.MustParse(`"long"`)
		}
		for _, value := range []time.Duration{-1, -unit, 24 * time.Hour, time.Duration(math.MaxInt64)} {
			_, err := avro.Marshal(schema, value)
			assert.Error(t, err, "%v in %v", value, unit)
		}
		for _, units := range []int64{-1, int64(24 * time.Hour / unit), math.MaxInt32} {
			var raw any = int32(units)
			if unit == time.Microsecond {
				raw = units
			}
			if units == math.MaxInt32 && unit == time.Microsecond {
				raw = int64(math.MaxInt64)
			}
			wire, err := avro.Marshal(plain, raw)
			require.NoError(t, err)
			got := time.Second
			assert.Error(t, avro.Unmarshal(schema, wire, &got))
			assert.Equal(t, time.Second, got)
			var generic any
			assert.Error(t, avro.Unmarshal(schema, wire, &generic))
		}
		for _, value := range []time.Duration{0, unit, 24*time.Hour - unit, 24*time.Hour - 1} {
			wire, err := avro.Marshal(schema, value)
			require.NoError(t, err)
			var got time.Duration
			require.NoError(t, avro.Unmarshal(schema, wire, &got))
			assert.Equal(t, value/unit*unit, got, "subunit precision is truncated")
		}
	}
}

func TestTimeOfDayRawIntegerBounds(t *testing.T) {
	millis := avro.MustParse(`{"type":"int","logicalType":"time-millis"}`)
	micros := avro.MustParse(`{"type":"long","logicalType":"time-micros"}`)
	for _, value := range []int32{-1, 86_400_000} {
		_, err := avro.Marshal(millis, value)
		assert.Error(t, err)
		wire, err := avro.Marshal(avro.MustParse(`"int"`), value)
		require.NoError(t, err)
		got := int32(7)
		assert.Error(t, avro.Unmarshal(millis, wire, &got))
		assert.Equal(t, int32(7), got)
	}
	if strconv.IntSize == 64 {
		value := int64(86_400_000_000)
		_, err := avro.Marshal(micros, int(value))
		assert.Error(t, err)
		wire, err := avro.Marshal(avro.MustParse(`"long"`), value)
		require.NoError(t, err)
		got := int(7)
		assert.Error(t, avro.Unmarshal(micros, wire, &got))
		assert.Equal(t, int(7), got)
	}
}

func TestLocalTimestampCivilEncoding(t *testing.T) {
	old := time.Local
	t.Cleanup(func() { time.Local = old })
	for _, host := range []*time.Location{time.UTC, time.FixedZone("host", 11*3600)} {
		time.Local = host
		for _, unit := range []time.Duration{time.Millisecond, time.Microsecond} {
			logical := `{"type":"long","logicalType":"local-timestamp-millis"}`
			if unit == time.Microsecond {
				logical = `{"type":"long","logicalType":"local-timestamp-micros"}`
			}
			schema := avro.MustParse(logical)
			input := time.Date(2020, 1, 2, 3, 4, 5, 123456000, time.FixedZone("input", -7*3600))
			civil := time.Date(2020, 1, 2, 3, 4, 5, 123456000, time.UTC)
			want := civil.Unix()*int64(time.Second/unit) + int64(civil.Nanosecond())/int64(unit)
			wire, err := avro.Marshal(schema, input)
			require.NoError(t, err)
			var got int64
			require.NoError(t, avro.Unmarshal(avro.MustParse(`"long"`), wire, &got))
			assert.Equal(t, want, got, "host %v, unit %v", host, unit)
			union := avro.MustParse(`["null",` + logical + `]`)
			wire, err = avro.Marshal(union, map[string]any{"long." + string(schema.(avro.LogicalTypeSchema).Logical().Type()): input})
			require.NoError(t, err)
			require.Equal(t, byte(2), wire[0])
			require.NoError(t, avro.Unmarshal(avro.MustParse(`"long"`), wire[1:], &got))
			assert.Equal(t, want, got)
		}
	}
}

func TestLocalTimestampCivilDecoding(t *testing.T) {
	old := time.Local
	rome, err := time.LoadLocation("Europe/Rome")
	require.NoError(t, err)
	time.Local = rome
	t.Cleanup(func() { time.Local = old })
	for _, clock := range []string{
		"1969-12-31T23:59:59.123456Z",
		"2020-03-29T01:30:00Z",
		"2020-03-29T02:30:00Z", // This civil clock does not exist in Rome.
		"2020-10-25T02:30:00Z", // This civil clock occurs twice in Rome.
	} {
		civil, err := time.Parse(time.RFC3339Nano, clock)
		require.NoError(t, err)
		for _, unit := range []time.Duration{time.Millisecond, time.Microsecond} {
			logical := `{"type":"long","logicalType":"local-timestamp-millis"}`
			if unit == time.Microsecond {
				logical = `{"type":"long","logicalType":"local-timestamp-micros"}`
			}
			schema := avro.MustParse(logical)
			units := civil.Unix()*int64(time.Second/unit) + int64(civil.Nanosecond())/int64(unit)
			wire, err := avro.Marshal(avro.MustParse(`"long"`), units)
			require.NoError(t, err)
			var got time.Time
			require.NoError(t, avro.Unmarshal(schema, wire, &got))
			assert.Equal(t, civil.Truncate(unit), got)
			var generic any
			require.NoError(t, avro.Unmarshal(schema, wire, &generic))
			assert.Equal(t, civil.Truncate(unit), generic)
			again, err := avro.Marshal(schema, got)
			require.NoError(t, err)
			assert.Equal(t, wire, again)
		}
	}
}

func TestTimestampCheckedUnits(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, unit := range []time.Duration{time.Millisecond, time.Microsecond} {
			name := "timestamp-millis"
			if unit == time.Microsecond {
				name = "timestamp-micros"
			}
			if local {
				name = "local-" + name
			}
			schema := avro.MustParse(`{"type":"long","logicalType":"` + name + `"}`)
			perSecond := int64(time.Second / unit)
			for _, bound := range []int64{math.MinInt64, math.MaxInt64} {
				value := time.Unix(bound/perSecond, (bound%perSecond)*int64(unit)).UTC()
				wire, err := avro.Marshal(schema, value)
				require.NoError(t, err)
				var got int64
				require.NoError(t, avro.Unmarshal(avro.MustParse(`"long"`), wire, &got))
				assert.Equal(t, bound, got)
				step := unit
				if bound < 0 {
					step = -unit
				}
				_, err = avro.Marshal(schema, value.Add(step))
				assert.Error(t, err)
			}
			_, err := avro.Marshal(schema, time.Date(1_000_000_000, 1, 1, 0, 0, 0, 0, time.UTC))
			assert.Error(t, err)
		}
	}
}
