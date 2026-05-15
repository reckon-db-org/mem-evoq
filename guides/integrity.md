# Integrity in mem-evoq

mem-evoq mirrors reckon-db's tamper-resistance behaviour as an opt-in per-store feature. The point: downstream consumers can write tests for integrity-violation handling without spinning up Khepri / Ra.

> **Status:** integrity config plumbing is in place as of 0.1.0 scaffold; the write-path / read-path / snapshot integrity machinery lands in subsequent moves (see `plans/PLAN_MEM_EVOQ.md`).

## Enabling integrity on a store

```erlang
Key = crypto:strong_rand_bytes(32),
{ok, _} = mem_evoq:start_store(my_test_store, #{
    integrity => #{enabled => true, key => Key}
}).
```

Validation enforced at start time:

- Key must be exactly 32 bytes (`crypto:strong_rand_bytes(32)` is the conventional way to produce one)
- A wrong-size key yields `{error, {integrity_key_invalid_size, ActualBytes}}`
- A malformed integrity map yields `{error, integrity_config_invalid}`

## What you get (once moves 14–17 land)

Same surface as reckon-db 2.1.0:

- Events written under integrity-enabled stores carry `prev_event_hash` + `mac`
- Reads verify chain + MAC, surface `{error, {integrity_violation, _}}` on failure
- Snapshots carry `anchor_hash` + `mac`; loads verify both
- Subscriptions perform per-event MAC checks during catch-up; tampered events halt replay with a `subscription_error`

The primitives are reused verbatim from `reckon_gater_integrity` — no duplicate cryptographic code, no implementation drift between mem-evoq and reckon-db.

## What's NOT here

mem-evoq is single-node by design. It deliberately omits:

- Persistence (process restart loses state)
- Clustering / replication
- Per-region keys, rotation, vault integration
- Capability-token-bound write authorisation (the per-event `mac` is the only authenticity layer)

For production tamper-resistance, pair evoq with reckon-evoq + reckon-db.

## Testing patterns

The shape `mem_evoq` is designed to support, once the integrity moves land:

```erlang
%% Test that your code reacts correctly to integrity_violation:
{ok, _} = mem_evoq:start_store(my_store, #{
    integrity => #{enabled => true, key => Key}
}),
%% ... write events through your application ...
%% ... tamper with stored state via a test helper ...
%% ... assert that your read path surfaces the violation ...
```

Equivalent test against reckon-db requires starting Khepri + Ra and a real data directory; against mem-evoq it's pure Erlang and gen_server state.
