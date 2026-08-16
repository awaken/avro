# Changelog

## Unreleased

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
