<picture>
  <source media="(prefers-color-scheme: dark)" srcset="http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec&mode=dark">
  <source media="(prefers-color-scheme: light)" srcset="http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec">
  <img alt="Logo" src="http://svg.wiersma.co.za/hamba/project?title=avro&tag=A%20fast%20Go%20avro%20codec">
</picture>

[![Go Report Card](https://goreportcard.com/badge/github.com/awaken/avro/v2)](https://goreportcard.com/report/github.com/awaken/avro/v2)
[![Build Status](https://github.com/awaken/avro/actions/workflows/test.yml/badge.svg)](https://github.com/awaken/avro/actions)
[![Coverage Status](https://coveralls.io/repos/github/awaken/avro/badge.svg?branch=main)](https://coveralls.io/github/awaken/avro?branch=main)
[![Go Reference](https://pkg.go.dev/badge/github.com/awaken/avro/v2.svg)](https://pkg.go.dev/github.com/awaken/avro/v2)
[![GitHub release](https://img.shields.io/github/release/awaken/avro.svg)](https://github.com/awaken/avro/releases)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/awaken/avro/main/LICENCE)

A fast, security-hardened Go Avro codec.

This is a maintained fork of the archived
[`hamba/avro`](https://github.com/hamba/avro), based on its final `v2.31.0`
release. It fixes the 2026 array/map denial-of-service and integer-overflow
advisories, adds bounded OCF decompression, hardens generated Go source, and
keeps untrusted-input resource limits enabled by default.

## Overview

Install with:

```shell
go get github.com/awaken/avro/v2
```

See [CHANGELOG.md](CHANGELOG.md) for behavioral changes and
[SECURITY.md](SECURITY.md) for the security policy.

### Secure defaults

The zero-value configuration limits strings and byte slices to 1 MiB, arrays
and maps to 1,048,576 items, and OCF compressed/decompressed blocks to 64 MiB.
An OCF block may declare at most 1,048,576 records. Applications can lower
these budgets for their workloads. A negative value explicitly disables the
corresponding limit and should be reserved for trusted input.

## Usage

```go
type SimpleRecord struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

schema, err := avro.Parse(`{
    "type": "record",
    "name": "simple",
    "namespace": "org.hamba.avro",
    "fields" : [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"}
    ]
}`)
if err != nil {
	log.Fatal(err)
}

in := SimpleRecord{A: 27, B: "foo"}

data, err := avro.Marshal(schema, in)
if err != nil {
	log.Fatal(err)
}

fmt.Println(data)
// Outputs: [54 6 102 111 111]

out := SimpleRecord{}
err = avro.Unmarshal(schema, data, &out)
if err != nil {
	log.Fatal(err)
}

fmt.Println(out)
// Outputs: {27 foo}
```

More examples in the [godoc](https://pkg.go.dev/github.com/awaken/avro/v2).

#### Types Conversions

| Avro                          | Go Struct                                                  | Go Interface             |
|-------------------------------|------------------------------------------------------------|--------------------------|
| `null`                        | `nil`                                                      | `nil`                    |
| `boolean`                     | `bool`                                                     | `bool`                   |
| `bytes`                       | `[]byte`                                                   | `[]byte`                 |
| `float`                       | `float32`                                                  | `float32`                |
| `double`                      | `float64`                                                  | `float64`                |
| `long`                        | `int`\*, `int64`, `uint32`\**                              | `int`, `int64`, `uint32` |
| `int`                         | `int`\*, `int32`, `int16`, `int8`, `uint8`\**, `uint16`\** | `int`, `uint8`, `uint16` |
| `fixed`                       | `uint64`                                                   | `uint64`                 |
| `string`                      | `string`                                                   | `string`                 |
| `array`                       | `[]T`                                                      | `[]any`                  |
| `enum`                        | `string`                                                   | `string`                 |
| `fixed`                       | `[n]byte`                                                  | `[n]byte`                |
| `map`                         | `map[string]T{}`                                           | `map[string]any`         |
| `record`                      | `struct`                                                   | `map[string]any`         |
| `union`                       | *see below*                                                | *see below*              |
| `int.date`                    | `time.Time`                                                | `time.Time`              |
| `int.time-millis`             | `time.Duration`                                            | `time.Duration`          |
| `long.time-micros`            | `time.Duration`                                            | `time.Duration`          |
| `long.timestamp-millis`       | `time.Time`                                                | `time.Time`              |
| `long.timestamp-micros`       | `time.Time`                                                | `time.Time`              |
| `long.local-timestamp-millis` | `time.Time`                                                | `time.Time`              |
| `long.local-timestamp-micros` | `time.Time`                                                | `time.Time`              |
| `bytes.decimal`               | `*big.Rat`                                                 | `*big.Rat`               |
| `fixed.decimal`               | `*big.Rat`                                                 | `*big.Rat`               |
| `string.uuid`                 | `string`                                                   | `string`                 |

\* Please note that the size of the Go type `int` is platform dependent. Decoding an Avro `long` into a Go `int` is
only allowed on 64-bit platforms and will result in an error on 32-bit platforms. Similarly, be careful when encoding a
Go `int` using Avro `int` on a 64-bit platform, as that can result in an integer overflow causing misinterpretation of
the data.

\** Please note that when the Go type is an unsigned integer care must be taken to ensure that information is not lost
when converting between the Avro type and Go type. For example, storing a *negative* number in Avro of `int = -100`
would be interpreted as `uint16 = 65,436` in Go. Another example would be storing numbers in Avro `int = 256` that
are larger than the Go type `uint8 = 0`.

##### Unions

The following union types are accepted: `map[string]any`, `*T` and `any`.

* **map[string]any:** If the union value is `nil`, a `nil` map will be en/decoded.
When a non-`nil` union value is encountered, a single key is en/decoded. The key is the avro
type name, or schema full name in the case of a named schema (enum, fixed or record).
* ***T:** This is allowed in a "nullable" union. A nullable union is defined as a two schema union,
with one of the types being `null` (ie. `["null", "string"]` or `["string", "null"]`), in this case
a `*T` is allowed, with `T` matching the conversion table above. In the case of a slice, the slice can be used
directly.
* ***struct{}:** implementing the `UnionConverter` interface:

```go
// UnionConverter to handle Avro Union's in a type-safe way
type UnionConverter interface {
    // FromAny payload decode into any of the mentioned types in the Union.
    FromAny(payload any) error
    // ToAny from the Union struct
    ToAny() (any, error)
}

// for example:
const Schema = `{"name": "Payload", "type": "record", "fields": [{"name": "union", "type": ["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]}]}`

type Payload struct {
    Union *UnionRecord `avro:"union"`
}

type UnionRecord struct {
    Int  *int
    Test *TestRecord
}

func (u *UnionRecord) ToAny() (any, error) {
    if u.Int != nil {
        return u.Int, nil
    } else if u.Test != nil {
        return u.Test, nil
    }

    return nil, errors.New("no value to encode")
}

func (u *UnionRecord) FromAny(payload any) error {
    switch t := payload.(type) {
    case int:
        u.Int = &t
    case TestRecord:
        u.Test = &t
    default:
        return errors.New("unknown type during decode of union")
    }

    return nil
}

type TestRecord struct {
    A int64  `avro:"a"`
    B string `avro:"b"`
}
```
Note due to way Go checks if some type implements these interface, the type used _must_ be a pointer as the interface methods _must_
be implemented with pointer receivers.
* **any:** An `interface` can be provided and the type or name resolved. Primitive types
are pre-registered, but named types, maps and slices will need to be registered with the `Register` function.
In the case of arrays and maps the enclosed schema type or name is postfix to the type with a `:` separator,
e.g `"map:string"`. Behavior when a type cannot be resolved will depend on your chosen configuation options:
	* !Config.UnionResolutionError && !Config.PartialUnionTypeResolution: the map type above is used
	* Config.UnionResolutionError && !Config.PartialUnionTypeResolution: an error is returned
	* !Config.UnionResolutionError && Config.PartialUnionTypeResolution: any registered type will get resolved while any unregistered type will fallback to the map type above.
	* Config.UnionResolutionError && Config.PartialUnionTypeResolution: any registered type will get resolved while any unregistered type will return an error.

##### TextMarshaler and TextUnmarshaler

The interfaces `TextMarshaler` and `TextUnmarshaler` are supported for a `string` schema type. The object will
be tested first for implementation of these interfaces, in the case of a `string` schema, before trying regular
encoding and decoding.

Enums may also implement `TextMarshaler` and `TextUnmarshaler`, and must resolve to valid symbols in the given enum schema.

##### Identical Underlying Types

One type can be [ConvertibleTo](https://go.dev/ref/spec#Conversions) another type if they have identical underlying types.
A non-native type is allowed to be used if it can be convertible to *time.Time*, *big.Rat* or *avro.LogicalDuration* for the particular of *LogicalTypes*.

Ex.: `type Timestamp time.Time`

##### Custom Type Conversion

In case of incompatible types, custom type conversion functions can be registered with the `RegisterTypeConverters` function.
This requires the use of `map[string]any` or `[]any`.
The type conversion for encoding will receive the original value that is to be encoded, and must return a data type that is compatible with the schema, as specified in the table above.
The type conversion for decoding will receive the decoded value with a data type that is compatible with the schema, and its return value will be used as the final decoded value.

##### Byte and Decimal Limits

`Config.MaxByteSliceSize` bounds decoded bytes and strings, encoded/decoded fixed
values, and decimal scaling operands and output. It defaults to 1 MiB. Decimal
arithmetic can use several times that amount of temporary memory. A negative
value disables the configured limit; platform allocation limits still apply.

## Benchmark

Benchmark source code can be found at: [https://github.com/nrwiersma/avro-benchmarks](https://github.com/nrwiersma/avro-benchmarks)

```
BenchmarkGoAvroDecode-8      	  788455	      1505 ns/op	     418 B/op	      27 allocs/op
BenchmarkGoAvroEncode-8      	  624343	      1908 ns/op	     806 B/op	      63 allocs/op
BenchmarkGoGenAvroDecode-8   	 1360375	       876.4 ns/op	     320 B/op	      11 allocs/op
BenchmarkGoGenAvroEncode-8   	 2801583	       425.9 ns/op	     240 B/op	       3 allocs/op
BenchmarkHambaDecode-8       	 5046832	       238.7 ns/op	      47 B/op	       0 allocs/op
BenchmarkHambaEncode-8       	 6017635	       196.2 ns/op	     112 B/op	       1 allocs/op
BenchmarkLinkedinDecode-8    	 1000000	      1003 ns/op	    1688 B/op	      35 allocs/op
BenchmarkLinkedinEncode-8    	 3170553	       381.5 ns/op	     248 B/op	       5 allocs/op
```

Always benchmark with your own workload. The result depends heavily on the data input.

## Go structs generation

Go structs can be generated for you from the schema. The types generated follow the same logic in [types conversions](#types-conversions)
You can use the avrogen command line tool to generate the structs, or use it as a lib in internal commands, it's the `gen` package.

Install the struct generator with:

```shell
go install github.com/awaken/avro/v2/cmd/avrogen@<version>
```

Example usage assuming there's a valid schema in `in.avsc`:

```shell
avrogen -pkg avro -o bla.go -tags json:snake,yaml:upper-camel in.avsc
```

**Tip:** Omit `-o FILE` to dump the generated Go structs to stdout instead of a file.

Check the options and usage with `-h`:

```shell
avrogen -h
```

### Custom logical type mapping with avrogen

You can register custom logical type mappings to be used during code generation. 

The format of a custom logical type mapper is `avroLogicalType,goType[,importPath]`. For example,
to map the logical type `uuid` to the Go type `github.com/google/uuid.UUID`, you would use:

```shell
avrogen -pkg avro -o bla.go -logical-type uuid,uuid.UUID,github.com/google/uuid in.avsc
```

If the type you are mapping to is a built-in Go type (e.g., `string`, `int`, etc.), you can omit the import path element in the mapping definition:

```shell
avrogen -pkg avro -o bla.go -logical-type date,int32 in.avsc
```

If you intend to use multiple custom logical type mappings, you can specify the `-logicaltype` flag multiple times.

## Avro schema validation

### avrosv

A small Avro schema validation command-line utility is also available. This simple tool leverages the
schema parsing functionality of the library, showing validation errors or optionally dumping parsed
schemas to the console. It can be used in CI/CD pipelines to validate schema changes in a repository.

Install the Avro schema validator with:

```shell
go install github.com/awaken/avro/v2/cmd/avrosv@<version>
```

Example usage assuming there's a valid schema in `in.avsc` (exit status code is `0`):

```shell
avrosv in.avsc
```

An invalid schema will result in a diagnostic output and a non-zero exit status code:

```shell
avrosv bad-default-schema.avsc; echo $?
Error: avro: invalid default for field someString. <nil> not a string
2
```

Schemas referencing other schemas can also be validated by providing all of them (schemas are parsed in order):

```shell
avrosv base-schema.avsc schema-withref.avsc
```

Check the options and usage with `-h`:

```shell
avrosv -h
```

### Name Validation

Avro names are validated according to the
[Avro specification](https://avro.apache.org/docs/1.11.1/specification/#names).

However, the official Java library does not validate said names accordingly, resulting to some files out in the wild
to have invalid names. Thus, this library has a configuration option to allow for these invalid names to be parsed.

```go
avro.SkipNameValidation = true
```

Note that this variable is global, so ideally you'd need to unset it after you're done with the invalid schema.

## Protocol compatibility

Protocol JSON sorts message names and preserves declared type and field order.
`Hash` is the MD5 of that representation. Corrected message ordering and explicit
null responses change hashes previously produced by older versions; rebuild
protocol-text caches when upgrading.

Every parsed message requires a request array and a response schema. An empty
request is `[]`; a null response is `"null"`, not JSON `null`. One-way calls require
`"one-way":true`, a null response and no declared errors. Omitting the flag, or
setting it to false, keeps the call two-way. Declared errors must resolve to error
records. Message names follow the existing name-validation setting.

`NewProtocol` copies its input collections, and `Types` returns a copy of the list.
Referenced schemas and messages remain shared and must be immutable.
`NewMessage` treats a nil response as the null schema; its caller must provide a
non-nil request and valid error and one-way schemas.

## Numeric and date compatibility

Integer codecs reject values outside the destination's width or signedness instead
of wrapping. Failed integer and time-of-day decodes preserve the destination.

Dates use the input's calendar year, month and day, ignoring its clock and zone.
Dates outside the signed 32-bit day range are rejected. This changes older output
for non-midnight or non-UTC inputs.

Time-millis and time-micros values must be at least zero and less than 24 hours.
Validation includes raw integer representations and occurs before narrowing or
duration multiplication. Duration encoding still discards positive subunit
fractions. Values previously accepted outside one day now return an error.

Local timestamps encode the input's wall-clock fields without consulting the host
zone. Decoding returns those fields in a UTC `time.Time` carrier, including clocks
that are ambiguous or nonexistent in a daylight-saving zone. That UTC value is a
civil-time carrier, not an assertion that the original event happened in UTC.
This changes the location and instant returned by older local-timestamp decoders;
callers requiring an instant must choose a zone and an ambiguity policy explicitly.
Ordinary timestamps still encode instants. Both forms reject counts outside the
signed 64-bit millisecond or microsecond range.

`Config.MaxByteSliceSize` also limits fixed values, including decimals and
durations, when encoding, decoding or skipping. The default is 1 MiB; a negative
value disables the configured limit. Oversized values fail before fixed-value
reflection or allocation. Increase the limit explicitly for larger trusted data.

Decimal encoding requires an exact value at the declared scale. For example,
`1/4` at scale two encodes as `0.25`; `1/3` returns an error. Encoding never mutates
the input rational. Encoding and decoding both enforce unscaled digit precision,
including raw byte, fixed-array and fixed-uint64 representations. Failed decimal
decodes preserve the destination. Decimal scaling and output use the configured
byte limit; precision diagnostics report counts without formatting the value.

## Embedded Go fields

Record fields use the shallowest matching Go field. At equal depth, a field with
an explicit, nonempty name tag wins over untagged fields. Equal-priority matches
are ambiguous, including the same field reached through multiple embedding paths.
Declaration order does not break ties.

An ambiguous field is unbound: encoding uses its schema default or reports a
missing required field; decoding skips its wire value and leaves all competing
Go fields unchanged. Add a direct field or distinct name tags to disambiguate
previously accepted layouts.

Anonymous structs are promoted unless explicitly named by a tag. A named
embedding is one nested field; `-` excludes it. Empty tag names retain the Go
field name or ordinary anonymous promotion. Tag options do not change selection.
The configured `TagKey` applies to all these rules. Pointer embeddings follow the
same rules, and only a selected destination path is allocated. Recursive and
repeated type graphs are traversed without enumerating every embedding path.

## Go Version Support

The minimum Go version is 1.26.

This library supports the last two versions of Go. While the minimum Go version is
not guaranteed to increase along side Go, it may jump from time to time to support
additional features. This will be not be considered a breaking change.

## Who uses hamba/avro?

- [Apache Arrow for Go](https://github.com/apache/arrow-go)
- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
- [pulsar-client-go](https://github.com/apache/pulsar-client-go)

Record field names and aliases share a namespace. Parsing and constructors reject
repeated aliases and collisions with field names. Alias text remains unrestricted.
During schema evolution, each reader field may match one writer name through its
name or aliases. An exact match plus an alias match is ambiguous and is rejected;
field order does not select a winner. Defaults apply only to unmatched fields.
Writer aliases do not participate in reader field matching.

SOE decoding requires exactly one datum and rejects trailing payload bytes,
including in `DecodeUnverified`. APIs from `Config.Freeze` implement
`ExactUnmarshaler`. Custom APIs used for SOE decoding must implement or forward
`UnmarshalExact`; otherwise decoding returns `soe.ErrExactAPI`. Custom encoding
and ordinary `API.Unmarshal` retain their existing contracts. A failed decode
can partly populate the destination.

OCF decoding requires the declared record count to consume the entire decompressed
block. Extra bytes, including bytes in a zero-record block, are errors. Empty
blocks and zero-width records remain valid. Record errors stop iteration and are
returned by `Error`; the failed destination may be partly populated. Core
`Decoder.Buffered` reports read-ahead bytes; `DecodeDatum` supports externally
framed zero-width records without changing stream `Decode` EOF behavior.

Registry responses default to a 4 MiB body limit, including trailing whitespace
and bytes decoded by the HTTP transport. Configure `registry.WithResponseLimit`
when larger schemas are required. Successful responses must contain one complete
JSON document; operations without a returned value also permit an empty body.
Oversized responses return `registry.ErrResponseLimit` and close after at most
one byte beyond the limit. Bodies are not drained after errors. The default HTTP
client has a timeout; custom clients should also set a timeout or request deadline.
### Registration and schema values

`Register` and `RegisterTypeConverters` affect subsequent calls, including the
next datum on an existing stream. A datum already being processed retains one
registration snapshot. Separate generations prevent stale codec builds from
replacing newly registered codecs.

Reader/Writer configuration accepts APIs from `Config.Freeze`. An API wrapper
can implement `ConfigProvider` with `AvroConfig() avro.API` returning its wrapped
API. Primitive I/O uses that API's settings, not wrapper method overrides.
Unsupported or nil APIs set `Reader.Error` or `Writer.Error` without panicking;
`Reset` preserves the configuration error.

Custom numeric schema properties retain `float64` when JSON round-tripping
preserves their value. Otherwise they use `encoding/json.Number`, including
large integers, overflow, underflow and long decimals. Handle both types when
reading numeric properties. Strings and map keys must contain valid UTF-8.
A map-form null union accepts nil, or a nonnil value explicitly converted to nil.

Canonical JSON escapes historical names accepted by `SkipNameValidation`.
Their corrected fingerprints may differ; use `LegacyParsingCanonicalForm` and
`LegacyFingerprintUsing` when migrating old identities. Array, map and reference
constructors return nil for nil children; their `Checked` variants also return
an error. Existing OCF files supply their own append schema, so unused schema
text is neither parsed nor cached.

### Confluent GUID headers

`registry.Decoder.DecodeHeaders(ctx, data, headers, key, &value)` uses the last
matching `registry.Header`. Set `key` for `KeySchemaIDHeader`; otherwise it uses
`ValueSchemaIDHeader`. A header contains version byte 1 followed by 16 GUID
bytes; the payload contains only the Avro datum. An absent matching header falls
back to the version-0 payload prefix. Malformed headers and failed GUID lookups
return errors without fallback. `Decode` keeps version-0 payload framing.
Both entry points accept Confluent top-level `bytes` without an Avro length
prefix; an extra application-supplied length prefix is now part of the value.

`Client.GetSchemaByGUID` accepts a hexadecimal UUID and caches successful
lookups separately from numeric IDs. Referenced schemas use immutable subject
versions and resolved responses, with at most 1024 direct references. Existing
response limits, HTTP timeouts and context cancellation apply. Older registries
without the GUID endpoint return their HTTP error.

The format and lookup follow the public
[Confluent wire specification](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format-schema-guid-in-header)
and [registry API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--schemas-guids-(string-guid)).

### Schema resolution

`SchemaCompatibility.Resolve(reader, writer)` supports recursive named records.
It builds the complete decoding graph before returning, preserving nested
promotions, defaults and references. Inputs remain unchanged; concurrent calls
may share the same declared schemas.

Resolved writer unions keep every original wire index, including branches that
promote to the same reader type. Their public `Types`, JSON and canonical
fingerprints describe a valid reader union (a one-member union for a non-union
reader). `CacheFingerprint` also identifies the decoding plan. JSON serialization
does not retain this plan; store both declared schemas and resolve them again.
Encoding a resolved writer union returns `ErrResolvedSchemaEncoding`; encode
using a declared writer schema instead.

### Generator naming and references

The generator visits every reachable record, including `NewRefSchema` targets.
It emits equivalent repeated definitions once and retains the first metadata.
Conflicting definitions with the same Avro full name fail generation.

Normalized Go identifiers must be exported and unambiguous. Colliding record or
enum names, enum constants, fields, and enabled encoder methods produce a sticky
`Write` error before any source is written; `Reset` clears it. Names such as `_`
that normalize to inaccessible identifiers are rejected. `WithFullName(true)`
includes namespaces in both record and enum names. Existing names are otherwise
retained; the generator does not invent suffixes to resolve collisions.

Generated encoders initialize each reachable definition graph in a private schema
cache. Recursive references work without preloading `avro.DefaultSchemaCache`,
and each generated `Schema` method returns its own named record. A cycle made
only of struct values is rejected because Go requires indirection. Nullable
pointers, slices, maps and interface unions provide that indirection.

Custom templates can use `SchemaDefinitions`, `SchemaCacheName` and each typedef's
`FullName` to implement the same initialization. An individual typedef's `Schema`
can contain references to other definitions and may not parse independently.
