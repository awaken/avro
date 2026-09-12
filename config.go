package avro

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/modern-go/reflect2"
)

const (
	defaultMaxByteSliceSize  = 1_048_576 // 1 MiB
	defaultMaxCollectionSize = 1_048_576
)

// DefaultConfig is the default API.
var DefaultConfig = Config{}.Freeze()

// Config customises how the codec should behave.
type Config struct {
	// TagKey is the struct tag key used when en/decoding structs.
	// This defaults to "avro".
	TagKey string

	// BlockLength is the length of blocks for maps and arrays.
	// This defaults to 100.
	BlockLength int

	// DisableBlockSizeHeader disables encoding of an array/map size in bytes.
	// Encoded array/map will be prefixed with only the number of elements in
	// contrast with default behavior which prefixes them with the number of elements
	// and the total number of bytes in the array/map. Both approaches are valid according to the
	// Avro specification, however not all decoders support the latter.
	DisableBlockSizeHeader bool

	// UnionResolutionError determines if an error will be returned
	// when a type cannot be resolved while decoding a union.
	UnionResolutionError bool

	// PartialUnionTypeResolution dictates if the union type resolution
	// should be attempted even when not all union types are registered.
	// When enabled, the underlying type will get resolved if it is registered
	// even if other types of the union are not. If resolution fails, logic
	// falls back to default union resolution behavior based on the value of
	// UnionResolutionError.
	PartialUnionTypeResolution bool

	// Disable caching layer for encoders and decoders, forcing them to get rebuilt on every
	// call to Marshal() and Unmarshal()
	DisableCaching bool

	// MaxByteSliceSize bounds decoded bytes/strings and encoded or decoded fixed values.
	// It defaults to 1 MiB; a negative value disables this limit. Fixed values are
	// checked before reflection, allocation or skipping, including logical types.
	// Decimal conversion also bounds operands, scale powers and encoded bytes;
	// intermediate arithmetic can use several times this amount of memory.
	MaxByteSliceSize int

	// MaxSliceAllocSize is the maximum number of elements the decoder will add to one slice.
	// It defaults to 1,048,576 and is enforced cumulatively across all array blocks.
	// A negative value disables the limit. If the limit is exceeded, decoding returns an error.
	MaxSliceAllocSize int

	// MaxMapAllocSize is the maximum number of entries the decoder will add to one map.
	// It defaults to 1,048,576 and is enforced cumulatively across all map blocks.
	// A negative value disables the limit. If the limit is exceeded, decoding returns an error.
	MaxMapAllocSize int
}

// Freeze fixes the codec settings and creates a registration API. The returned API
// also implements ExactUnmarshaler. Registrations affect subsequent API calls;
// each datum uses one registration snapshot, including on existing streams.
func (c Config) Freeze() API {
	api := &frozenConfig{config: c}
	api.current.Store(newFrozenConfig(c, NewTypeResolver(), NewTypeConverters()))
	return api
}

// Each generation owns its caches and pools; an older build cannot publish into a newer cache.
func newFrozenConfig(c Config, resolver *TypeResolver, converters *TypeConverters) *frozenConfig {
	api := &frozenConfig{config: c, resolver: resolver, typeConverters: converters}

	api.readerPool = &sync.Pool{
		New: func() any {
			return &Reader{
				cfg:    api,
				reader: nil,
				buf:    nil,
				head:   0,
				tail:   0,
			}
		},
	}
	api.writerPool = &sync.Pool{
		New: func() any {
			return &Writer{
				cfg:   api,
				out:   nil,
				buf:   make([]byte, 0, 512),
				Error: nil,
			}
		},
	}

	return api
}

// API represents fixed codec settings with updatable type registrations.
type API interface {
	// Marshal returns the Avro encoding of v.
	Marshal(schema Schema, v any) ([]byte, error)

	// Unmarshal parses the Avro encoded data and stores the result in the value pointed to by v.
	// If v is nil or not a pointer, Unmarshal returns an error.
	Unmarshal(schema Schema, data []byte, v any) error

	// NewEncoder returns a new encoder that writes to w using schema.
	NewEncoder(schema Schema, w io.Writer) *Encoder

	// NewDecoder returns a new decoder that reads from reader r using schema.
	NewDecoder(schema Schema, r io.Reader) *Decoder

	// DecoderOf returns the value decoder for a given schema and type.
	DecoderOf(schema Schema, typ reflect2.Type) ValDecoder

	// EncoderOf returns the value encoder for a given schema and type.
	EncoderOf(schema Schema, typ reflect2.Type) ValEncoder

	// Register registers names for subsequent API calls. Primitive types are pre-registered.
	Register(name string, obj any)

	// RegisterTypeConverters registers conversions for subsequent API calls.
	RegisterTypeConverters(conv ...TypeConverter)

	// TypeOf returns the schema type for a given name.
	TypeOf(name string) (reflect2.Type, error)

	// NamesOf returns the names associated with a given type.
	NamesOf(typ reflect2.Type) ([]string, error)
}

// ExactUnmarshaler decodes one datum and rejects any unconsumed input bytes.
// Config.Freeze implements it. API wrappers used for framed decoding must expose
// this capability explicitly; ordinary Unmarshal may accept trailing bytes.
// The destination must be a non-nil pointer and may be partly populated on error.
type ExactUnmarshaler interface {
	UnmarshalExact(schema Schema, data []byte, v any) error
}

// ConfigProvider lets an API wrapper expose its underlying frozen configuration
// to NewReader and NewWriter. AvroConfig must return the wrapped API, not itself.
// Primitive I/O uses those settings; wrapper method overrides are not invoked.
type ConfigProvider interface {
	AvroConfig() API
}

// readerConfig resolves wrappers explicitly; unsupported APIs fail without losing their limits.
func readerConfig(api API) (*frozenConfig, error) {
	for range 64 {
		if api == nil {
			break
		}
		v := reflect.ValueOf(api)
		switch v.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if v.IsNil() {
				return nil, errors.New("avro: configuration cannot be nil")
			}
		}
		if cfg, ok := api.(*frozenConfig); ok {
			return cfg, nil
		}
		provider, ok := api.(ConfigProvider)
		if !ok {
			break
		}
		api = provider.AvroConfig()
	}
	return nil, errors.New("avro: configuration must come from Config.Freeze or ConfigProvider")
}

type frozenConfig struct {
	config  Config
	current atomic.Pointer[frozenConfig]
	regMu   sync.Mutex

	decoderCache sync.Map // map[cacheKey]ValDecoder
	encoderCache sync.Map // map[cacheKey]ValEncoder

	readerPool *sync.Pool
	writerPool *sync.Pool

	resolver *TypeResolver

	typeConverters *TypeConverters
}

func (c *frozenConfig) snapshot() *frozenConfig {
	if current := c.current.Load(); current != nil {
		return current
	}
	return c
}

func (c *frozenConfig) Marshal(schema Schema, v any) ([]byte, error) {
	c = c.snapshot()
	writer := c.borrowWriter()
	defer c.returnWriter(writer)

	writer.WriteVal(schema, v)
	if err := writer.Error; err != nil {
		return nil, err
	}

	result := writer.Buffer()
	copied := make([]byte, len(result))
	copy(copied, result)

	return copied, nil
}

func (c *frozenConfig) borrowWriter() *Writer {
	writer := c.writerPool.Get().(*Writer)
	writer.Reset(nil)
	return writer
}

func (c *frozenConfig) returnWriter(writer *Writer) {
	writer.out = nil
	writer.Error = nil

	c.writerPool.Put(writer)
}

func (c *frozenConfig) Unmarshal(schema Schema, data []byte, v any) error {
	return c.unmarshal(schema, data, v, false)
}

// UnmarshalExact decodes one datum into v and requires complete input consumption.
func (c *frozenConfig) UnmarshalExact(schema Schema, data []byte, v any) error {
	return c.unmarshal(schema, data, v, true)
}

func (c *frozenConfig) unmarshal(schema Schema, data []byte, v any, exact bool) error {
	c = c.snapshot()
	reader := c.borrowReader(data)
	defer c.returnReader(reader)

	reader.ReadVal(schema, v)
	err := reader.Error

	//nolint:errorlint // Only a direct EOF represents an empty zero-width value.
	if err == io.EOF {
		err = nil
	}
	if err == nil && exact && reader.head != reader.tail {
		return fmt.Errorf("avro: %d trailing bytes after datum", reader.tail-reader.head)
	}
	return err
}

func (c *frozenConfig) borrowReader(data []byte) *Reader {
	reader := c.readerPool.Get().(*Reader)
	reader.Reset(data)
	return reader
}

func (c *frozenConfig) returnReader(reader *Reader) {
	reader.Reset(nil)
	c.readerPool.Put(reader)
}

func (c *frozenConfig) NewEncoder(schema Schema, w io.Writer) *Encoder {
	writer, ok := w.(*Writer)
	if !ok {
		writer = NewWriter(w, 512, WithWriterConfig(c))
	}
	return &Encoder{
		s: schema,
		w: writer,
	}
}

func (c *frozenConfig) NewDecoder(schema Schema, r io.Reader) *Decoder {
	reader := NewReader(r, 512, WithReaderConfig(c))
	return &Decoder{
		s: schema,
		r: reader,
	}
}

func (c *frozenConfig) Register(name string, obj any) {
	c.regMu.Lock()
	defer c.regMu.Unlock()

	old := c.snapshot()
	resolver := old.resolver.clone()
	resolver.Register(name, obj)
	c.current.Store(newFrozenConfig(c.config, resolver, old.typeConverters))
}

func (c *frozenConfig) RegisterTypeConverters(convs ...TypeConverter) {
	// Converter metadata is user code. Invoke it before taking the registration lock.
	added := NewTypeConverters()
	added.RegisterTypeConverters(convs...)
	if !added.hasRegistered() {
		return
	}
	c.regMu.Lock()
	defer c.regMu.Unlock()

	old := c.snapshot()
	converters := old.typeConverters.clone()
	added.convs.Range(func(key, value any) bool {
		converters.convs.Store(key, value)
		return true
	})
	converters.registered.Store(true)
	c.current.Store(newFrozenConfig(c.config, old.resolver, converters))
}

func (c *frozenConfig) TypeOf(name string) (reflect2.Type, error) {
	c = c.snapshot()
	return c.resolver.Type(name)
}

func (c *frozenConfig) NamesOf(typ reflect2.Type) ([]string, error) {
	c = c.snapshot()
	return c.resolver.Name(typ)
}

type cacheKey struct {
	fingerprint [32]byte
	rtype       uintptr
	instance    Schema
}

func (c *frozenConfig) newCacheKey(schema Schema, rtype uintptr) cacheKey {
	key := cacheKey{fingerprint: schema.CacheFingerprint(), rtype: rtype}
	if c.typeConverters.hasRegistered() {
		key.instance = schema
	}
	return key
}

func (c *frozenConfig) addDecoderToCache(schema Schema, rtype uintptr, dec ValDecoder) {
	if c.config.DisableCaching {
		return
	}
	key := c.newCacheKey(schema, rtype)
	c.decoderCache.Store(key, dec)
}

func (c *frozenConfig) getDecoderFromCache(schema Schema, rtype uintptr) ValDecoder {
	if c.config.DisableCaching {
		return nil
	}
	key := c.newCacheKey(schema, rtype)
	if dec, ok := c.decoderCache.Load(key); ok {
		return dec.(ValDecoder)
	}

	return nil
}

func (c *frozenConfig) addEncoderToCache(schema Schema, rtype uintptr, enc ValEncoder) {
	if c.config.DisableCaching {
		return
	}
	key := c.newCacheKey(schema, rtype)
	c.encoderCache.Store(key, enc)
}

func (c *frozenConfig) getEncoderFromCache(schema Schema, rtype uintptr) ValEncoder {
	if c.config.DisableCaching {
		return nil
	}
	key := c.newCacheKey(schema, rtype)
	if enc, ok := c.encoderCache.Load(key); ok {
		return enc.(ValEncoder)
	}

	return nil
}

func (c *frozenConfig) getTagKey() string {
	tagKey := c.config.TagKey
	if tagKey == "" {
		return "avro"
	}
	return tagKey
}

func (c *frozenConfig) getBlockLength() int {
	blockSize := c.config.BlockLength
	if blockSize <= 0 {
		return 100
	}
	return blockSize
}

func (c *frozenConfig) getMaxByteSliceSize() int {
	size := c.config.MaxByteSliceSize
	if size == 0 {
		return defaultMaxByteSliceSize
	}
	return size
}

func (c *frozenConfig) getMaxSliceAllocSize() int {
	size := c.config.MaxSliceAllocSize
	if size < 0 || size > maxAllocSize {
		return maxAllocSize
	}
	if size == 0 {
		return defaultMaxCollectionSize
	}
	return size
}

func (c *frozenConfig) getMaxMapAllocSize() int {
	size := c.config.MaxMapAllocSize
	if size < 0 || size > maxAllocSize {
		return maxAllocSize
	}
	if size == 0 {
		return defaultMaxCollectionSize
	}
	return size
}
