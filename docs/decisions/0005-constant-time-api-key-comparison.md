# 0005. Constant-time comparison for API key validation

Date: 2026-08-04
Status: Accepted

## Context

`internal/api/auth_interceptor.go` implements the gRPC unary interceptor
added for issue #9 to require a bearer-token API key on the ingestion
service's gRPC endpoint. The original implementation stored `validKeys` as
`map[string]struct{}` and checked membership with
`_, valid := auth.validKeys[token]`. Code review flagged this: Go's string
equality (and therefore map key lookup) short-circuits at the first
mismatched byte, which is a textbook timing side-channel on an auth check,
even though in practice gRPC's own network jitter dwarfs the nanosecond-scale
signal a real attacker could extract.

## Decision

`validKeys` is a `[]string` instead of a map. `authenticate` compares the
presented token against every configured key with
`subtle.ConstantTimeCompare([]byte(token), []byte(key))`, OR-ing the results
together (`match |= subtle.ConstantTimeCompare(...)`) rather than returning
as soon as one matches. Returning early on the first match would reintroduce
a timing signal correlated with the matching key's position in the slice, so
the loop always runs to completion regardless of where (or whether) a match
occurs.

## Consequences

- Removes the map-lookup and string-equality timing side-channel from the
  auth check.
- `authenticate` is now O(n) byte comparisons per request, where n is the
  number of configured API keys, instead of the map's O(1) average lookup.
  Accepted because the key set is small and static (one key per
  tenant/service account, expected to stay in the single digits to low
  dozens, not something that grows unboundedly): at that size the absolute
  cost is sub-microsecond, so the map's speed advantage was never needed and
  it bought a real, if low-probability, security weakness for no benefit.
- No benchmark was added for this change. Given the key-set size assumption
  above, the cost is negligible enough that a benchmark wouldn't inform any
  decision here; this is a deliberate exception to the hot-path benchmarking
  discipline, not an oversight (`authenticate` runs once per request on the
  gRPC auth path, not the ingestion hot path itself).
- `subtle.ConstantTimeCompare` still short-circuits and returns `0`
  immediately when the two byte slices differ in length, before doing any
  byte-by-byte work. This is a known, accepted limitation: token length
  isn't secret or derived from key material, so leaking it isn't a
  meaningful weakness.
- If the key set were ever expected to grow large (e.g. per-request dynamic
  keys, or thousands of tenants), this approach would need revisiting; that
  isn't the shape of this system's key set today.

## Alternatives considered

- **Keep the `map[string]struct{}` lookup.** Rejected: this is exactly what
  code review flagged, a non-constant-time comparison on an auth check.
- **Early-return on first `ConstantTimeCompare` match.** Rejected: it
  defeats the purpose. An attacker who can measure response timing could
  still infer which key index matched (and therefore whether a given
  candidate token is "close" to a valid key's position in configuration
  order), which is the same class of leak this change exists to close.
- **Hash each key (e.g. HMAC) and compare hashes instead of raw bytes.**
  Considered as a way to also normalize comparison length. Rejected as
  unnecessary complexity here: `ConstantTimeCompare`'s length short-circuit
  is already an accepted, non-sensitive leak per Consequences, so hashing
  would add a step without closing a gap that matters for this system.
