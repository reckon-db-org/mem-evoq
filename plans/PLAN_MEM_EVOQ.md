# Plan: mem-evoq Implementation

**Status:** Scaffold landed; callback implementation pending.
**Target release:** 0.1.0 to hex.pm.
**Companion:** Move 4 in the evoq pressure-on-status-quo plan
(in `/home/rl/work/codeberg.org/reckon-db-org/reckon-ecosystem/`
once written, currently in conversation).

---

## Moves in execution order

Each move is one commit chain. Done-criterion is what proves it.

### Scaffold + skeleton

1. **Repo bones.** ✅ DONE (this commit).
2. **OTP skeleton.** ✅ DONE — app, sup, registry, store gen_server,
   facade. All store callbacks return `{error, not_implemented}`.
3. **Adapter shell.** ✅ DONE — `mem_evoq_adapter` exports the full
   evoq_event_store surface and forwards to the store gen_server.

### Core read/write path (no integrity yet)

4. **`append/4`.** Implement against the existing `do_append`
   semantics in `reckon_db_streams` — `ExpectedVersion` check, version
   assignment, store events in `#state{streams = ...}`. *Done = unit
   test appends 5 events, reads them, sees them.*

5. **`read/5`.** Forward + backward, version-bounded, count-bounded.
   *Done = roundtrip both directions; nonexistent stream returns
   `{error, {stream_not_found, _}}`.*

6. **Metadata operations.** `version/2`, `exists/2`, `list_streams/1`,
   `has_events/1`, `delete/2`. *Done = unit test per operation.*

7. **`read_all_global/3`.** Cross-stream reader sorted by epoch_us.
   *Done = 3 streams × 5 events, reads all 15 in epoch order.*

### Subscriptions

8. **Subscription state.** Add `subscribers` to store state.
   `subscribe/4` registers, returns a SubKey. *Done = register +
   query + cancel.*

9. **Catch-up replay.** On subscribe with `from => 0`, spawn-and-send
   existing events as `{events, [...]}` batches. *Done = subscribe
   after 5 writes, receive all 5.*

10. **Live fan-out.** Append fans out to matching subscribers via
    their pid. *Done = subscribe empty stream → write → receive each
    event as it lands.*

11. **Filter dispatch.** Support `stream`, `event_type`,
    `event_pattern`, `tags`. *Done = subscribe by event_type, verify
    only matching events delivered.*

### Snapshots

12. **`save/3,5` + `load/2` + `load_at/3` + `list/2` + `delete/2,3`.**
    Per-stream map of version → snapshot. *Done = save/load roundtrip
    + latest-version lookup.*

### Integrity (opt-in, mirrors reckon-db)

13. **Config plumbing.** ✅ PARTIAL — `start_store/2` accepts and
    validates integrity opts at startup. Storage-level usage of the
    key pending moves 14-17.

14. **Write-path integrity.** Resolve chain tip, set `prev_event_hash`,
    compute MAC, store. Set `chain_start` watermark on first
    integrity-bearing append per stream. *Done = events written under
    integrity-enabled store carry `prev_event_hash` + `mac`.*

15. **Read-path verification.** Verify each event on read via
    `reckon_gater_integrity:verify_event/3`. Support
    `#{verify => skip_legacy | strict | skip_all}` opts. *Done =
    test bypass tampers state → read returns
    `{error, {integrity_violation, _}}`.*

16. **Snapshot integrity.** `anchor_hash` + `mac` on save; verify on
    load. *Done = tamper a saved snapshot → load refuses with
    `snapshot_mac_mismatch`.*

17. **Subscription verification on catch-up.** Per-event MAC check.
    Halt + emit `subscription_error` on violation. *Done = tamper
    then subscribe → receive `{subscription_error,
    {integrity_violation, _}}`.*

### Tests + tooling

18. **Round-trip integration test** mirroring the reckon-db
    integrity_writes_SUITE structure. *Done = same property
    assertions pass.*

19. **PropEr suite** for the in-memory model. Property: any sequence
    of (append, read, snapshot) produces consistent state. *Done =
    `rebar3 proper` runs clean.*

### Release

20. **README + integrity guide.** Quick-start, integrity-enable
    example, explicit limitations table.

21. **CHANGELOG 0.1.0 entry.** Initial release, in-scope, deferred
    list.

22. **`rebar3 hex publish` 0.1.0.** *Done = `mem_evoq-0.1.0.tar` on
    hex.pm.*

23. **Tag + push v0.1.0 to Codeberg.**

### Downstream payoff

24. **Replace a test in evoq** that currently uses mocks or skips
    integration with one using `mem_evoq` end-to-end. *Done = at
    least one evoq integration test uses mem_evoq_adapter.*

---

## Architectural notes

### State shape (mem_evoq_store)

```erlang
#state{
    store_id      :: atom(),
    streams       :: #{StreamId => [event()]},
    snapshots     :: #{StreamId => #{Version => snapshot()}},
    subscribers   :: #{SubKey => sub_info()},
    integrity     :: disabled | #{key := binary(),
                                  chain_start := #{StreamId => Version}}
}
```

### Adapter ↔ store communication

Adapter looks up store pid in `mem_evoq_registry` (ETS, public,
read-concurrent). Every callback is a `gen_server:call/2` into the
store with a 5s timeout. No async paths.

### Subscriber delivery

In-process pid fan-out. No remote nodes. No pg2 / pg, no Erlang
distribution. If the subscriber pid is on a different node, behaviour
is undefined — mem-evoq is single-node by design.

### Integrity primitives

Pure reuse of `reckon_gater_integrity`:

- `compute_chain_hash/2`, `compute_event_mac/2`,
  `compute_snapshot_mac/2`
- `verify_event/3`, `verify_snapshot/3`
- `is_legacy_event/1`, `is_legacy_snapshot/1`
- `genesis_prev_hash/0`

No code duplication; the contract is enforced by the same module that
reckon-db uses.

---

## Out of scope

These are NOT going into mem-evoq, ever:

- Persistence (disk, file, dets, mnesia, etc.)
- Clustering, replication, multi-node coordination
- pg-based distributed registry
- Scavenging, archive, snapshot rotation
- Capability verification (different domain — gateway concern)
- Cluster health probes, raft consensus

If you find yourself wanting any of these, you want reckon-evoq +
reckon-db. mem-evoq is the test substrate.
