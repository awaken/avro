package avro

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"

	"github.com/go-viper/mapstructure/v2"
)

var (
	protocolReserved = []string{"doc", "types", "messages", "protocol", "namespace"}
	messageReserved  = []string{"doc", "response", "request", "errors", "one-way"}
)

type protocolConfig struct {
	doc   string
	props map[string]any
}

// ProtocolOption is a function that sets a protocol option.
type ProtocolOption func(*protocolConfig)

// WithProtoDoc sets the doc on a protocol.
func WithProtoDoc(doc string) ProtocolOption {
	return func(opts *protocolConfig) {
		opts.doc = doc
	}
}

// WithProtoProps sets the properties on a protocol.
func WithProtoProps(props map[string]any) ProtocolOption {
	return func(opts *protocolConfig) {
		opts.props = props
	}
}

// Protocol is an Avro protocol.
type Protocol struct {
	name
	properties

	types    []NamedSchema
	messages map[string]*Message

	doc string

	hash string
}

// NewProtocol creates a protocol with copies of the type and message collections.
// The referenced schemas and messages must remain immutable after construction.
func NewProtocol(
	name, namepsace string,
	types []NamedSchema,
	messages map[string]*Message,
	opts ...ProtocolOption,
) (*Protocol, error) {
	var cfg protocolConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	n, err := newName(name, namepsace, nil)
	if err != nil {
		return nil, err
	}
	for _, key := range slices.Sorted(maps.Keys(messages)) {
		if err := validateName(key); err != nil {
			return nil, fmt.Errorf("avro: invalid message name %q: %w", key, err)
		}
	}

	p := &Protocol{
		name:       n,
		properties: newProperties(cfg.props, protocolReserved),
		types:      slices.Clone(types),
		messages:   maps.Clone(messages),
		doc:        cfg.doc,
	}

	b := md5.Sum([]byte(p.String()))
	p.hash = hex.EncodeToString(b[:])

	return p, nil
}

// Message returns a message with the given name or nil.
func (p *Protocol) Message(name string) *Message {
	return p.messages[name]
}

// Doc returns the protocol doc.
func (p *Protocol) Doc() string {
	return p.doc
}

// Hash returns the MD5 hash of the protocol.
func (p *Protocol) Hash() string {
	return p.hash
}

// Types returns a copy of the protocol's type list. The schemas are shared.
func (p *Protocol) Types() []NamedSchema {
	return slices.Clone(p.types)
}

// String returns deterministic protocol JSON, sorting messages by name.
// Declared type and field order is preserved.
func (p *Protocol) String() string {
	types := ""
	for _, f := range p.types {
		types += f.String() + ","
	}
	if len(types) > 0 {
		types = types[:len(types)-1]
	}

	messages := ""
	for _, k := range slices.Sorted(maps.Keys(p.messages)) {
		key, _ := json.Marshal(k)
		messages += string(key) + `:` + p.messages[k].String() + ","
	}
	if len(messages) > 0 {
		messages = messages[:len(messages)-1]
	}

	name, _ := json.Marshal(p.Name())
	namespace, _ := json.Marshal(p.Namespace())
	return `{"protocol":` + string(name) +
		`,"namespace":` + string(namespace) +
		`,"types":[` + types + `],"messages":{` + messages + `}}`
}

// Message is an Avro protocol message.
type Message struct {
	properties

	req    *RecordSchema
	resp   Schema
	errs   *UnionSchema
	oneWay bool

	doc string
}

// NewMessage creates a message, treating a nil response as the null schema.
// The request must be non-nil. Schemas are shared and must remain immutable.
// Errors, if present, must include the implicit string branch first. A one-way
// message requires a null response and no declared errors. ParseProtocol validates
// these constraints when constructing messages from JSON.
func NewMessage(req *RecordSchema, resp Schema, errors *UnionSchema, oneWay bool, opts ...ProtocolOption) *Message {
	var cfg protocolConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if isNilSchema(resp) {
		resp = NewNullSchema()
	}

	return &Message{
		properties: newProperties(cfg.props, messageReserved),
		req:        req,
		resp:       resp,
		errs:       errors,
		oneWay:     oneWay,
		doc:        cfg.doc,
	}
}

// Request returns the message request schema.
func (m *Message) Request() *RecordSchema {
	return m.req
}

// Response returns the message response schema.
func (m *Message) Response() Schema {
	return m.resp
}

// Errors returns the message errors union schema.
func (m *Message) Errors() *UnionSchema {
	return m.errs
}

// OneWay determines of the message is a one way message.
func (m *Message) OneWay() bool {
	return m.oneWay
}

// Doc returns the message doc.
func (m *Message) Doc() string {
	return m.doc
}

// String returns the canonical form of the message.
func (m *Message) String() string {
	fields := ""
	for _, f := range m.req.fields {
		fields += f.String() + ","
	}
	if len(fields) > 0 {
		fields = fields[:len(fields)-1]
	}

	str := `{"request":[` + fields + `],"response":` + m.resp.String()
	if m.errs != nil && len(m.errs.Types()) > 1 {
		errs, _ := NewUnionSchema(m.errs.Types()[1:])
		str += `,"errors":` + errs.String()
	}
	if m.oneWay {
		str += `,"one-way":true`
	}
	str += "}"
	return str
}

// ParseProtocolFile parses an Avro protocol from a file.
func ParseProtocolFile(path string) (*Protocol, error) {
	s, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return ParseProtocol(string(s))
}

// MustParseProtocol parses an Avro protocol, panicing if there is an error.
func MustParseProtocol(protocol string) *Protocol {
	parsed, err := ParseProtocol(protocol)
	if err != nil {
		panic(err)
	}

	return parsed
}

// ParseProtocol parses an Avro protocol.
func ParseProtocol(protocol string) (*Protocol, error) {
	cache := &SchemaCache{}

	var m map[string]any
	if err := schemaJSONAPI.Unmarshal([]byte(protocol), &m); err != nil {
		return nil, err
	}

	seen := seenCache{}
	return parseProtocol(m, seen, cache)
}

type protocol struct {
	Protocol  string                    `mapstructure:"protocol"`
	Namespace string                    `mapstructure:"namespace"`
	Doc       string                    `mapstructure:"doc"`
	Types     []any                     `mapstructure:"types"`
	Messages  map[string]map[string]any `mapstructure:"messages"`
	Props     map[string]any            `mapstructure:",remain"`
}

func parseProtocol(m map[string]any, seen seenCache, cache *SchemaCache) (*Protocol, error) {
	var (
		p    protocol
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &p, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding protocol: %w", err)
	}

	if err := checkParsedName(p.Protocol); err != nil {
		return nil, err
	}

	var (
		types []NamedSchema
		err   error
	)
	if len(p.Types) > 0 {
		types, err = parseProtocolTypes(p.Namespace, p.Types, seen, cache)
		if err != nil {
			return nil, err
		}
	}

	messages := map[string]*Message{}
	if len(p.Messages) > 0 {
		for _, k := range slices.Sorted(maps.Keys(p.Messages)) {
			if err := validateName(k); err != nil {
				return nil, fmt.Errorf("avro: invalid message name %q: %w", k, err)
			}
			message, err := parseMessage(p.Namespace, p.Messages[k], seen, cache)
			if err != nil {
				return nil, err
			}

			messages[k] = message
		}
	}
	if err := normalizeProperties(p.Props); err != nil {
		return nil, err
	}

	return NewProtocol(p.Protocol, p.Namespace, types, messages, WithProtoDoc(p.Doc), WithProtoProps(p.Props))
}

func parseProtocolTypes(namespace string, types []any, seen seenCache, cache *SchemaCache) ([]NamedSchema, error) {
	ts := make([]NamedSchema, len(types))
	for i, typ := range types {
		schema, err := parseType(namespace, typ, seen, cache)
		if err != nil {
			return nil, err
		}

		namedSchema, ok := schema.(NamedSchema)
		if !ok {
			return nil, errors.New("avro: protocol types must be named schemas")
		}

		ts[i] = namedSchema
	}

	return ts, nil
}

type message struct {
	Doc      string           `mapstructure:"doc"`
	Request  []map[string]any `mapstructure:"request"`
	Response any              `mapstructure:"response"`
	Errors   []any            `mapstructure:"errors"`
	OneWay   bool             `mapstructure:"one-way"`
	Props    map[string]any   `mapstructure:",remain"`
}

func parseMessage(namespace string, m map[string]any, seen seenCache, cache *SchemaCache) (*Message, error) {
	// Check JSON shapes before decoding can collapse missing and null values.
	if _, ok := m["request"].([]any); !ok {
		return nil, errors.New("avro: message request must be an array")
	}
	if _, ok := m["response"]; !ok {
		return nil, errors.New("avro: message response is required")
	}
	if value, ok := m["one-way"]; ok {
		if _, ok := value.(bool); !ok {
			return nil, errors.New("avro: message one-way must be a boolean")
		}
	}
	if value, ok := m["errors"]; ok {
		if _, ok := value.([]any); !ok {
			return nil, errors.New("avro: message errors must be an array")
		}
	}

	var (
		msg  message
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &msg, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding message: %w", err)
	}

	fields := make([]*Field, len(msg.Request))
	for i, f := range msg.Request {
		field, err := parseField(namespace, f, seen, cache)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}
	request := &RecordSchema{
		name:       name{},
		properties: properties{},
		fields:     fields,
	}

	response, err := parseType(namespace, msg.Response, seen, cache)
	if err != nil {
		return nil, err
	}

	types := []Schema{NewPrimitiveSchema(String, nil)}
	if len(msg.Errors) > 0 {
		for _, e := range msg.Errors {
			schema, err := parseType(namespace, e, seen, cache)
			if err != nil {
				return nil, err
			}

			resolved := schema
			if ref, ok := schema.(*RefSchema); ok {
				resolved = ref.Schema()
			}
			if rec, ok := resolved.(*RecordSchema); !ok || !rec.IsError() {
				return nil, errors.New("avro: declared errors must resolve to error records")
			}

			types = append(types, schema)
		}
	}
	errs, err := NewUnionSchema(types)
	if err != nil {
		return nil, err
	}

	oneWay := msg.OneWay
	if oneWay && (len(errs.Types()) > 1 || response.Type() != Null) {
		return nil, errors.New("avro: one-way messages require a null response and no declared errors")
	}
	if err := normalizeProperties(msg.Props); err != nil {
		return nil, err
	}

	return NewMessage(request, response, errs, oneWay, WithProtoDoc(msg.Doc), WithProtoProps(msg.Props)), nil
}
