# Security policy

## Supported versions

Only the latest released version of `github.com/awaken/avro/v2` receives
security fixes.

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public issue. Use GitHub's
private vulnerability reporting for `awaken/avro`, including:

- the affected API and version;
- a minimal reproducer or malformed Avro input;
- the expected impact;
- any suggested mitigation.

Reports will be acknowledged as soon as practical. A coordinated disclosure
date will be agreed after the issue is reproduced and a fix is ready.

## Untrusted input

The default decoder budgets are intentional security boundaries. Tune them
down for the application when possible. Disabling them with negative values
removes denial-of-service protection and is appropriate only for trusted,
size-controlled data.
