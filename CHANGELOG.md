# Changelog

## Unreleased

## v2.31.5 - 2026-09-11

### Dependencies

- Refresh all imported package and test dependencies, including
  `klauspost/compress` v1.20.0, `x/tools` v0.50.0, `x/mod` v0.41.0,
  `x/sync` v0.23.0, and `testify` v1.12.1.
- Raise the minimum Go version to 1.26; test Go 1.26 and 1.27 in CI.
- Run test and fuzz workflows only on manual request (`workflow_dispatch`).

### Fixed

- Harden codec construction, union resolution, custom converters, defaults,
  recursive schemas, and compatibility caches against invalid or stale state.
- Reject integer overflow and invalid time-of-day values; correct calendar
  dates, local timestamps, and exact decimal precision and scaling.
- Enforce fixed-value and decimal resource limits before allocation.
- Validate record aliases, logical schemas, defaults, and protocol messages;
  produce deterministic protocol JSON and hashes.
- Require complete SOE payload and OCF block consumption; handle zero-width
  records, codec limits, and encoder/decoder lifecycle errors consistently.
- Bound registry response bodies and require complete, valid JSON responses.
- Correct generated types, imports, reset state, and schema validation.
- Expand regression tests across codecs, schemas, generation, OCF, SOE,
  registry clients, and command-line tools.

### Compatibility

- Previously accepted out-of-range integers, time values, inexact decimals,
  ambiguous aliases, malformed protocols, and trailing framed data now fail.
- Dates use calendar fields; local timestamps decode into a UTC civil-time
  carrier. Corrected protocol serialization changes some protocol hashes.
- Custom SOE APIs must implement or forward `ExactUnmarshaler`.
- Registry responses default to 4 MiB; use `WithResponseLimit` for larger
  schemas. Fixed values and decimal arithmetic use `Config.MaxByteSliceSize`.
- See the README for migration details and error behavior.

## v2.31.3 - 2026-08-16

### Dependencies

- Upgrade `github.com/go-viper/mapstructure/v2` from v2.4.0 to v2.5.0.
- Upgrade `github.com/klauspost/compress` from v1.18.7 to v1.19.2.
- Upgrade `github.com/stretchr/testify` from v1.9.0 to v1.11.1.
- Upgrade the loaded transitive test dependencies `github.com/google/go-cmp`
  from v0.6.0 to v0.7.0 and `gopkg.in/check.v1` from its 2016 revision to
  `v1.0.0-20201130134442-10cb98267c6c`; refresh the resulting `kr/pretty`,
  `kr/text`, and `rogpeppe/go-internal` dependency chain.

## v2.31.2 - 2026-08-16

### Security

- Fix CPU-exhaustion loops when hostile array or map blocks end early.
- Validate wire-sized integers before converting them to platform `int`.
- Enforce cumulative array and map budgets, enabled by default.
- Bound OCF compressed bytes, decompressed bytes, and records per block by
  default; reject negative, oversized, truncated, and invalid-sync blocks
  before decoding.
- Preflight Snappy output and stream-limit Deflate output to prevent
  decompression bombs; constrain Zstandard decoder memory.
- Prevent Avro names, enum symbols, and struct tags from injecting declarations
  into generated Go source when name validation is explicitly disabled.
- Reject empty unions and stop readers that repeatedly return no progress.
- Upgrade dependencies past all advisories reported by `govulncheck`.

### Fixed

- Decode floats and doubles with Avro's required little-endian byte order on
  every architecture.
- Prevent `Reader.ReadBytes` results from exposing spare slab capacity.
- Make Reader and Writer reset operations clear stale errors.
- Compile and test cleanly on 32-bit targets.

### Performance

- Encode integers directly with `binary.AppendUvarint`.
- Append float and double encodings without temporary allocations.
- Avoid allocating the declared size of a truncated OCF block before proving
  the bytes are present.

### Compatibility

- The module path is now `github.com/awaken/avro/v2`.
- The minimum Go version is 1.25.
- Default collection and OCF resource budgets can reject exceptionally large
  inputs that the archived upstream accepted without bounds.
