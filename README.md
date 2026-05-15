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
{ok, _} = application:ensure_all_started(mem_evoq),
ok = application:set_env(evoq, event_store_adapter, mem_evoq_adapter),

{ok, _} = mem_evoq:start_store(my_test_store),

%% Any evoq dispatch now targets the in-memory store.
%% When done:
ok = mem_evoq:stop_store(my_test_store).
```

Or talk to the adapter directly without evoq in the loop:

```erlang
{ok, _} = mem_evoq:start_store(direct_demo),

{ok, 1} = mem_evoq_adapter:append(
    direct_demo, <<"order$1">>, -1,  %% -1 = ?NO_STREAM
    [#{event_type => <<"order_placed">>, data => #{total => 42}},
     #{event_type => <<"order_confirmed">>, data => #{}}]),

{ok, [_, _]} = mem_evoq_adapter:read(
    direct_demo, <<"order$1">>, 0, 10, forward).
```

## With integrity enabled

mem-evoq mirrors reckon-db's tamper-resistance behaviour so you can test integrity-violation handling without infrastructure:

```erlang
Key = crypto:strong_rand_bytes(32),
{ok, _} = mem_evoq:start_store(secure_demo, #{
    integrity => #{enabled => true, key => Key}
}),

{ok, 0} = mem_evoq_adapter:append(
    secure_demo, <<"audit$1">>, -1,
    [#{event_type => <<"login_attempted">>, data => #{user => <<"a">>}}]),

%% Strict read verifies the full chain.
{ok, [_]} = mem_evoq_adapter:read(
    secure_demo, <<"audit$1">>, 0, 10, forward, #{verify => strict}).
```

Events written under this store carry `prev_event_hash` and `mac`. Reads verify both. A tampered event surfaces as `{error, {integrity_violation, _}}`. See [Integrity guide](guides/integrity.md) for verification modes, snapshot anchoring, and subscription catch-up behaviour.

## Limitations

| Concern | mem-evoq | reckon-db |
|---|---|---|
| Persistence | ✘ — process restart loses state | ✓ disk-backed (Khepri/Ra) |
| Clustering / replication | ✘ single process | ✓ Raft consensus |
| Tamper-resistance | ✓ identical primitives, opt-in per store | ✓ identical primitives, opt-in per store |
| Subscription fan-out | ✓ live + catch-up | ✓ live + catch-up |
| Snapshots | ✓ save/load/list/delete + anchor | ✓ |
| Read filters | by stream / event_type / pattern / tags | full reckon-db filter taxonomy |
| Scavenging / archive | ✘ | ✓ |
| Capability tokens / per-region keys / vault | ✘ | ✓ |
| Key rotation | ✘ — restart with the new key | ✓ |

For anything in the right column not matched on the left, pair evoq with `reckon-evoq` + `reckon-db`.

## Related packages

- [evoq](https://codeberg.org/reckon-db-org/evoq) — the CQRS / Event Sourcing framework this is an adapter for
- [reckon-evoq](https://codeberg.org/reckon-db-org/reckon-evoq) — production adapter targeting reckon-db
- [reckon-gater](https://codeberg.org/reckon-db-org/reckon-gater) — shared types and integrity primitives both adapters consume
- [reckon-db](https://codeberg.org/reckon-db-org/reckon-db) — the production event store

## License

Apache 2.0 — see [LICENSE](LICENSE).
