# Integrity in mem-evoq

mem-evoq mirrors reckon-db's tamper-resistance behaviour as an opt-in per-store feature. The point: downstream consumers can write tests for integrity-violation handling without spinning up Khepri / Ra.

The cryptographic primitives are reused verbatim from `reckon_gater_integrity` — there is no duplicate code, and behaviour cannot drift between mem-evoq and reckon-db.

## Enabling integrity on a store

```erlang
Key = crypto:strong_rand_bytes(32),
{ok, _} = mem_evoq:start_store(my_store, #{
    integrity => #{enabled => true, key => Key}
}).
```

Validation enforced at start time:

- Key must be exactly 32 bytes
- A wrong-size key yields `{error, {integrity_key_invalid_size, ActualBytes}}`
- A malformed integrity map yields `{error, integrity_config_invalid}`

## What gets added to every event

When the store is integrity-enabled, every appended event gains:

- `prev_event_hash` — for the first event in a stream, the genesis hash (32 zero bytes); for subsequent events, the chain hash of the predecessor
- `mac` — an HMAC-SHA256 over the canonical event encoding, shaped as `{KeyId, MacBytes}`

The first append on each stream records a per-stream chain-start watermark. Subsequent appends thread the chain forward.

## Verifying on read

Read accepts an `Opts` map with a `verify` key:

```erlang
{ok, Events} = mem_evoq_adapter:read(
    StoreId, StreamId, FromVersion, Count, forward, #{verify => Mode}).
```

| Mode | Behaviour |
|---|---|
| `skip_legacy` (default) | Verify integrity-bearing events; ignore legacy events with no integrity fields. Suitable for stores that started off without integrity and were enabled later. |
| `strict` | Every event must carry valid integrity fields. A legacy event surfaces as `{error, {integrity_violation, #{kind := missing_integrity, ...}}}`. |
| `skip_all` | Disable verification entirely. The store still attaches integrity fields on write. |

A tampered event surfaces as:

```erlang
{error, {integrity_violation, #{
    layer => storage,
    stream_id => <<"...">>,
    version => 3,
    kind => mac_mismatch | chain_mismatch | missing_integrity,
    context => #{...}
}}}
```

Backward reads use the same `verify` semantics. Under the hood the slice is reversed, verified forward, and reversed back — matches reckon-db 2.1.1.

## Snapshots

Save populates two integrity fields on the snapshot record:

- `anchor_hash` — the chain hash of the event at the snapshot version (`save` refuses with `{error, {snapshot_anchor_unavailable, _}}` if no integrity-bearing event exists at that version)
- `mac` — HMAC over the canonical snapshot encoding (including the anchor)

Load verifies both. The interesting property is the *anchor*: even if an attacker re-signs a tampered event with the legitimate key (so its individual MAC passes), the recomputed chain hash at the snapshot's version will no longer match the stored anchor — load surfaces `{error, {integrity_violation, #{kind := snapshot_anchor_mismatch, ...}}}`.

## Subscription catch-up

When a subscription replays existing events to the subscriber pid before installing the live subscription, each event's MAC is verified first. A violation halts the catch-up — the subscriber receives:

```erlang
{subscription_error, {integrity_violation, #{...}}}
```

…instead of the remaining batch. No partially-tampered prefix is delivered.

## Testing patterns

The shape `mem_evoq` is designed to support:

```erlang
{ok, _} = mem_evoq:start_store(my_store, #{
    integrity => #{enabled => true, key => Key}
}),
%% Drive the application through whatever facade writes events.
%% Tamper with stored state via sys:replace_state on the store pid
%% (the store gen_server holds events in plain map entries; see
%% test/unit/mem_evoq_integrity_snapshot_tests.erl for examples).
%% Assert that your read path surfaces the integrity_violation.
```

Equivalent test against reckon-db requires starting Khepri + Ra and a real data directory; against mem-evoq it's pure Erlang and gen_server state — sub-second test runs, no temp dirs to clean up.

## What's NOT here

mem-evoq is single-node by design. It deliberately omits:

- Persistence (process restart loses state)
- Clustering / replication
- Per-region keys, rotation, vault integration
- Capability-token-bound write authorisation (the per-event `mac` is the only authenticity layer)

For production tamper-resistance, pair evoq with reckon-evoq + reckon-db.
