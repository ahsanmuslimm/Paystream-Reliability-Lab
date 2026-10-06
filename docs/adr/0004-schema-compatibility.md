# ADR-0004: Schema compatibility strategy

Status: accepted (2026-10-06)
Drivers: P2 spike planning, FR-03, Document 03 section 2

## Context

Three event schemas (Transaction, FraudAlert, Notification) evolve across
releases. Schema Registry supports several compatibility modes and subject
naming strategies; choosing wrong creates cross-service breakage.

## Decision

- **Avro** with **BACKWARD compatibility** (new consumers can read data
  written by the previous schema; the Registry default).
- **TopicNameStrategy** subjects (`bank.transactions.v1` → subject
  `bank.transactions.v1`): subject and topic version move together, matching
  the `<domain>.<entity>.v<N>` topic standard.
- New optional fields must carry defaults; fields must never be renamed or
  retyped (add a new field and deprecate the old one); breaking changes
  require a new topic version (`.v2`) with its own subject.
- The `.avsc` files in `kafka-config/schemas/` are the single source of truth:
  Schema Registry registration and the Java code generation (via the
  `common-avro` Maven module) both read from them.
- CI contract tests (T7, Stage 2) verify: field addition with default passes;
  field removal fails; type change fails.

## Consequences

- Consumers always read with `specific.avro.reader=true`; producer and
  consumer code cannot silently drift from the registry.
- `null` unions must declare defaults, enforced by `validate-config.py`.
- Evolution is a pull-request activity with CI verification, not a console
  action (config-as-code principle).
