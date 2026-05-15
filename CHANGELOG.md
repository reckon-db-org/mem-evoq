# Changelog

All notable changes to mem-evoq will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-05-15

### Fixed

- **Snapshot adapter contract**: 0.1.0 / 0.1.1 exposed `save_snapshot/3,5`, `load_snapshot/2`, `load_snapshot_at/3`, `list_snapshots/2`, `delete_snapshot/2`. None of those names match the `evoq_snapshot_adapter` callbacks evoq's `evoq_snapshot_store` actually calls. The adapter could not be wired as evoq's snapshot store; aggregate snapshotting end-to-end was broken.
- mem_evoq_adapter now declares `-behaviour(evoq_snapshot_adapter)` and implements the canonical names: `save/5`, `read/2`, `read_at_version/3`, `delete/2`, `delete_at_version/3`, `list_versions/2`.
- Snapshot reads translate `#snapshot{}` → `#evoq_snapshot{}` at the boundary. `anchor_hash` and `mac` are storage-only and intentionally NOT propagated.
- Stream delete renamed: `mem_evoq_adapter:delete/2` (stream) → `mem_evoq_adapter:delete_stream/2`. The 2-arg `delete/2` now means snapshot delete-all (per behaviour).

### Removed (pre-1.0 cleanup)

- `save_snapshot/3` (record-taking variant; the 5-arg `save/5` covers it).
- `load_snapshot/2`, `load_snapshot_at/3`, `delete_snapshot/2`, `list_snapshots/2` — all renamed per the behaviour. No backward shim because we just shipped 0.1.x hours ago.

### Added

- `test/integration/mem_evoq_evoq_seam_SUITE` gains three snapshot tests that drive through `evoq_snapshot_store` rather than the adapter directly — proving the contract works in both directions and locking the bug down.

## [0.1.1] - 2026-05-15

### Fixed

- **Adapter contract**: `mem_evoq_adapter` was returning raw reckon_gater `#event{}` records to evoq, but the `evoq_event_store` contract expects `#evoq_event{}` records (or maps). The mismatch made the adapter unusable when actually wired into evoq — `evoq_event_store:read/5` would crash with `function_clause` inside its `event_to_map/1` helper. 0.1.0 worked when called directly in mem-evoq's own tests but failed at the evoq seam.
- Read paths (`read/5,6`, `read_all/3,4`, `read_all_global/3`, `read_by_event_types/3`, `read_by_tags/3,4`) now translate `#event{} → #evoq_event{}` at the adapter boundary.
- Subscription delivery now goes through a per-subscription bridge process that translates `{events, [#event{}]}` → `{events, [#evoq_event{}]}` before forwarding. `subscription_error` messages (e.g. integrity violations during catch-up) pass through unchanged. Matches the pattern reckon-evoq uses.

`mac` and `signature` are storage-only concerns and are intentionally NOT propagated through the adapter — same as reckon-evoq. Tests that assert on them now go through `sys:get_state` on the store gen_server.

## [0.1.0] - 2026-05-15

Initial release. In-memory event-store adapter for [evoq](https://codeberg.org/reckon-db-org/evoq) — a reference implementation of the `evoq_event_store` callback contract, intended for tests, demos, and integration scaffolding without spinning up Khepri/Ra.

### Adapter surface

- `mem_evoq_adapter` implements the full `evoq_event_store` callback set: append, read (forward/backward, with optional `verify` mode), read_all / read_all_global, version, exists, has_events, list_streams, delete, save_snapshot / load_snapshot / list_snapshots / delete_snapshot, subscribe / subscribe_all / unsubscribe.
- `mem_evoq` facade: `start_store/1,2`, `stop_store/1`, `list_stores/0`.
- `mem_evoq_registry` — ETS-backed StoreId → Pid lookup with monitor-driven cleanup.
- `mem_evoq_store` — per-store gen_server holding streams, snapshots, subscribers, and (when enabled) the integrity context.

### Write path

- `append/4` honours `?NO_STREAM`, `?ANY_VERSION`, `?STREAM_EXISTS`, and exact-version expected-version semantics matching reckon-db.
- Cross-stream global ordering via `epoch_us` microsecond timestamps.

### Read path

- Forward and backward reads slice exact `[FromVersion, FromVersion+Count-1]` (forward) / `[max(0, FromVersion-Count+1), FromVersion]` (backward) windows.
- Out-of-range requests return partial results (no padding, no error). Stream-not-found returns `{stream_not_found, StreamId}`.
- Optional `read/6` accepting `#{verify => skip_legacy | strict | skip_all}` for integrity-enabled stores. Backward verification uses the reverse → verify forward → reverse-back trick from reckon-db 2.1.1.

### Subscriptions

- Per-store subscribe / subscribe_all / unsubscribe.
- Filter taxonomy: `by_stream`, `by_event_type`, `by_event_pattern`, `by_tags` (with `any` / `all` match).
- `from => 0` triggers synchronous catch-up replay before the call returns.
- Dead-subscriber cleanup via `process` monitor — subscriptions self-evict on `'DOWN'`.

### Snapshots

- Save / load / load-at-version / list / delete.
- Stream deletion cascades to snapshots.

### Tamper resistance (opt-in per store)

- `start_store/2` accepts `#{integrity => #{enabled => true, key => Key}}` — Key must be exactly 32 bytes. Invalid sizes / malformed maps surface as `{error, ...}` at start time.
- Every appended event under an integrity-enabled store carries `prev_event_hash` (genesis for v0, chain hash of predecessor thereafter) and `mac` (HMAC-SHA256 via `reckon_gater_integrity`).
- Per-stream chain-start watermark recorded on the first append; lazy enablement supports stores switched on mid-life.
- Read verification: `strict` fails on legacy events; `skip_legacy` (default) verifies integrity-bearing events only; `skip_all` disables verification entirely.
- Snapshots carry `anchor_hash` (chain hash of the event at the snapshot's version) + `mac`. Load recomputes the anchor — a tampered underlying stream surfaces as `snapshot_anchor_mismatch` even if the tampered event itself has been re-signed with the legitimate key.
- Subscription catch-up MAC-verifies every event before delivery; violations halt the catch-up with `{subscription_error, {integrity_violation, _}}`.

### Tests

- 93 EUnit tests across append / read / metadata / read_all_global / subscription / snapshot / integrity / snapshot-integrity surfaces.
- 6 Common Test cases (`test/integration/mem_evoq_integrity_writes_SUITE`) mirroring `reckon_db_integrity_writes_SUITE`.
- 6 PropEr properties × 100 random runs each (`test/prop/prop_mem_evoq`).
- Total: 699 distinct test invocations on `rebar3 eunit && rebar3 ct && rebar3 proper`.

### Out of scope (see README limitations table)

Persistence, clustering / replication, key rotation, per-region keys, vault integration, capability-token-bound writes, scavenging / archive, and reckon-db's full filter taxonomy. For any of these, pair evoq with `reckon-evoq` + `reckon-db`.
