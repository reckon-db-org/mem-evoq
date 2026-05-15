# mem-evoq

[![Hex.pm](https://img.shields.io/hexpm/v/mem_evoq.svg)](https://hex.pm/packages/mem_evoq)
[![Hexdocs.pm](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/mem_evoq)

In-memory event-store adapter for [evoq](https://codeberg.org/reckon-db-org/evoq).

## Why this exists

[evoq](https://codeberg.org/reckon-db-org/evoq) is backend-agnostic. The production adapter is [reckon-evoq](https://codeberg.org/reckon-db-org/reckon-evoq), which targets [reckon-db](https://codeberg.org/reckon-db-org/reckon-db). For tests, demos, and as a reference implementation of the `evoq_event_store` adapter behaviour, this package provides an in-memory adapter that has no disk persistence and no clustering — process restart loses state, which is the intended semantic.

Pairs with evoq for:

- Writing integration tests against the full dispatch → store → emit → project cycle without spinning up Khepri/Ra
- Demos that fit in 30 lines of Erlang
- A second concrete implementation of the `evoq_event_store` contract (proof that the contract is implementable; not specific to reckon-db)
- Testing your integrity-violation handling: mem-evoq supports optional integrity exactly as reckon-db does

## Installation

```erlang
%% rebar.config
{deps, [
    {evoq, "~> 1.15"},
    {mem_evoq, "~> 0.1"}
]}.
```

## Quick start

```erlang
{ok, _} = application:ensure_all_started(mem_evoq).
ok = application:set_env(evoq, event_store_adapter, mem_evoq_adapter).

{ok, _} = mem_evoq:start_store(my_test_store).

%% Any evoq dispatch now targets the in-memory store.
%% When done:
ok = mem_evoq:stop_store(my_test_store).
```

## With integrity enabled

mem-evoq mirrors reckon-db's tamper-resistance behaviour so you can test integrity-violation handling without infrastructure:

```erlang
Key = crypto:strong_rand_bytes(32),
{ok, _} = mem_evoq:start_store(my_test_store, #{
    integrity => #{enabled => true, key => Key}
}).
```

Events written under this store carry `prev_event_hash` and `mac`. Reads verify both. A tampered event (mutated directly in store state via a test helper) surfaces as `{error, {integrity_violation, _}}` on read — same shape as reckon-db.

## What this is NOT

- **Not production**. No persistence, no clustering, no replication. For production use, pair evoq with `reckon-evoq` + `reckon-db`.
- **Not a full reckon-db substitute**. Filter modes, scavenging, archive operations, and cluster-aware queries are out of scope. See `guides/integrity.md` for the full limitations table.

## Related packages

- [evoq](https://codeberg.org/reckon-db-org/evoq) — the CQRS / Event Sourcing framework this is an adapter for
- [reckon-evoq](https://codeberg.org/reckon-db-org/reckon-evoq) — production adapter targeting reckon-db
- [reckon-gater](https://codeberg.org/reckon-db-org/reckon-gater) — shared types and integrity primitives both adapters consume
- [reckon-db](https://codeberg.org/reckon-db-org/reckon-db) — the production event store

## License

Apache 2.0 — see [LICENSE](LICENSE).
