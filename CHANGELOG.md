# Changelog

All notable changes to mem-evoq will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Scaffold

* OTP application skeleton: `mem_evoq_app`, `mem_evoq_sup`,
  `mem_evoq_registry` (ETS-backed StoreId→pid lookup),
  `mem_evoq_store` (per-store gen_server skeleton).
* Public facade: `mem_evoq:start_store/1,2`, `stop_store/1`,
  `list_stores/0`.
* Adapter shell: `mem_evoq_adapter` exports the full
  `evoq_event_store` callback surface; every callback forwards to
  the store gen_server, which currently returns
  `{error, not_implemented}` so the wiring is verifiable but the
  behaviour is loud rather than silent.
* Integrity config plumbing: `mem_evoq:start_store/2` accepts
  `#{integrity => disabled | #{enabled := true, key := binary()}}`
  and validates the 32-byte key constraint at start time.

Build is clean (`rebar3 compile`). No callbacks implemented yet —
that work is sequenced in `plans/PLAN_MEM_EVOQ.md`.
