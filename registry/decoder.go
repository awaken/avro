package registry

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/awaken/avro/v2"
)

const (
	// KeySchemaIDHeader identifies the Kafka key's schema GUID.
	KeySchemaIDHeader = "__key_schema_id"
	// ValueSchemaIDHeader identifies the Kafka value's schema GUID.
	ValueSchemaIDHeader = "__value_schema_id"
)

// Header is a Kafka record header. Order is significant when names repeat.
type Header struct {
	Key   string `json:"key" yaml:"key" xml:"key"`
	Value []byte `json:"value" yaml:"value" xml:"value"`
}

// DecoderFunc is a function used to customize the Decoder.
type DecoderFunc func(*Decoder)

// WithAPI sets the avro configuration on the decoder.
func WithAPI(api avro.API) DecoderFunc {
	return func(d *Decoder) {
		d.api = api
	}
}

// Decoder decodes Confluent wire formatted Avro payloads.
type Decoder struct {
	client *Client
	api    avro.API
}

// NewDecoder returns a decoder that will get schemas from client.
func NewDecoder(client *Client, opts ...DecoderFunc) *Decoder {
	d := &Decoder{
		client: client,
		api:    avro.DefaultConfig,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}
	return d
}

func isNilAPI(api avro.API) bool {
	if api == nil {
		return true
	}

	value := reflect.ValueOf(api)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Decode decodes data into v.
// The data must be formatted using the Confluent wire format, otherwise
// an error will be returned. Top-level bytes use Confluent raw-byte encoding.
// See:
// https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format.
func (d *Decoder) Decode(ctx context.Context, data []byte, v any) error {
	if len(data) < 5 {
		return errors.New("data too short")
	}

	id, err := extractSchemaID(data)
	if err != nil {
		return fmt.Errorf("extracting schema id: %w", err)
	}
	if d.client == nil {
		return errors.New("registry client cannot be nil")
	}

	schema, err := d.client.GetSchema(ctx, id)
	if err != nil {
		return fmt.Errorf("getting schema: %w", err)
	}
	if isNilAPI(d.api) {
		return errors.New("avro API cannot be nil")
	}

	return d.decodeDatum(schema, data[5:], v)
}

func extractSchemaID(data []byte) (int, error) {
	if len(data) < 5 {
		return 0, errors.New("data too short")
	}
	if data[0] != 0 {
		return 0, fmt.Errorf("invalid magic byte: %x", data[0])
	}
	return int(binary.BigEndian.Uint32(data[1:5])), nil
}

// DecodeHeaders decodes data into v, using the last matching Kafka schema header.
// key selects KeySchemaIDHeader; false selects ValueSchemaIDHeader. A present
// header must contain version byte 1 and exactly 16 GUID bytes; data contains
// only the Avro datum. An absent header falls back to Decode's version-0 prefix.
// Invalid headers and GUID lookup failures never fall back to another schema.
// Registries without the GUID endpoint return their HTTP error.
// See https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format-schema-guid-in-header.
func (d *Decoder) DecodeHeaders(ctx context.Context, data []byte, headers []Header, key bool, v any) error {
	name := ValueSchemaIDHeader
	if key {
		name = KeySchemaIDHeader
	}
	for i := len(headers) - 1; i >= 0; i-- {
		if headers[i].Key != name {
			continue
		}
		header := headers[i].Value
		if len(header) != 17 || header[0] != 1 {
			return fmt.Errorf("invalid %s header: expected version 1 and 16 GUID bytes", name)
		}
		if d.client == nil {
			return errors.New("registry client cannot be nil")
		}
		if isNilAPI(d.api) {
			return errors.New("avro API cannot be nil")
		}
		guid := fmt.Sprintf("%x-%x-%x-%x-%x", header[1:5], header[5:7], header[7:9], header[9:11], header[11:17])
		schema, err := d.client.GetSchemaByGUID(ctx, guid)
		if err != nil {
			return fmt.Errorf("getting GUID schema: %w", err)
		}
		return d.decodeDatum(schema, data, v)
	}
	return d.Decode(ctx, data, v)
}

// Confluent omits the length of a top-level bytes datum. Add only that prefix
// through a reader, preserving configured allocation limits without copying data.
func (d *Decoder) decodeDatum(schema avro.Schema, data []byte, v any) error {
	if schema.Type() != avro.Bytes {
		return d.api.Unmarshal(schema, data, v)
	}
	var prefix [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(prefix[:], uint64(len(data))*2)
	reader := io.MultiReader(bytes.NewReader(prefix[:n]), bytes.NewReader(data))
	return d.api.NewDecoder(schema, reader).Decode(v)
}
