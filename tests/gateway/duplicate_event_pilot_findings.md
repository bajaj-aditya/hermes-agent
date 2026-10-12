# Duplicate-event pilot — findings (tests only, no production change)

Base: worktree at `818c13be1dc` (branch `freya/duplicate-event-pilot`). Test file:
`tests/gateway/test_duplicate_event_pilot.py` (9 tests: 8 pass, 1 strict-xfail).
Logs of the real runs: `/home/hermes/.hermes/kanban/workspaces/t_7b8c0aa1/pilot-artifacts/`.

## Rerun

```bash
CHECKOUT=/path/to/this/checkout          # worktree containing the two test files
PV=/path/of/your/choice/pilot-venv       # new, empty directory for the isolated test environment
cd "$CHECKOUT"
python -m pm.build_env --source . --out "$PV" --group dev --group test   # fresh build; never move a venv
HERMES_PYTHON=$PV/bin/python scripts/run_tests.sh tests/gateway/test_duplicate_event_pilot.py -v -rxX
HERMES_PYTHON=$PV/bin/python scripts/run_tests.sh tests/gateway/test_duplicate_event_pilot.py -v --runxfail   # shows the defect failure
```
(`python` must be the checkout's bootstrapped interpreter. In the original run it was
`/home/hermes/.hermes/hermes-agent/venv/bin/python`, with
`PV=/home/hermes/.hermes/kanban/workspaces/t_7b8c0aa1/pilot-venv`.)
Use `$TMPDIR` for any scratch files. The reconnect test goes through `carry_dedup()`, which calls
`carry_inbound_dedup(inbound_dedup_caches(adapter), new)` where that helper exists (older sources, e.g. fork main
`5eb1381f`) and `carry_inbound_dedup(adapter, new)` otherwise; same real seam either way.
Runner sets a temp `HERMES_HOME`; the test fixture also points `HERMES_KANBAN_DB` at a temp file and refuses
AF_INET/AF_INET6 connects, `getaddrinfo` and `create_connection`.

## Seams that exist (all used; no refactor needed)

| Seam | Location |
|---|---|
| Discord live ingress + in-memory dedup claim | `plugins/platforms/discord/adapter.py:1797` `_dispatch_discord_message`, `:1716` admission, `:1375` `_dedup` |
| Dedup carried across adapter rebuild | `gateway/platforms/helpers.py:20` `MessageDeduplicator` (in-memory, TTL 300s), `:69` `carry_inbound_dedup` |
| Session guard + pending queue | `gateway/platforms/base.py:4038` `handle_message`, `:4080` `_handle_message_while_active`, `:4562` `_process_message_background` |
| Retry of final send | `base.py:3648` `_send_with_retry`, reached via `:4378` `send_final_ledgered` |
| Durable recovery ledger (SQLite) + backfill | `adapter.py:2532` `_run_missed_message_backfill`, `:2992` active-claim (10 min), hooks `:3231/:3239` |
| Kanban idempotency | `hermes_cli/kanban_db.py:1182` `create_task`, lookup at `:1254` |
| Kanban notifier retry (rewind/advance cursor) | `gateway/kanban_watchers_notifier.py:545` `delivery_failed`, `:712` `_send_pings`, `:749` `deliver` |

## Scenario → test → result

| # | Test | Boundary | Result |
|---|---|---|---|
| 1 | `test_adapter_boundary_sequential_and_concurrent_replay_runs_handler_once` | Real Discord adapter+base queue; handler/send mocked | PASS: 3 concurrent + 1 late replay of one message id → handler 1×, send 1×, and exactly 1 real kanban row created by the handler WITHOUT any idempotency key |
| 1 | `test_replay_after_adapter_rebuild_is_dropped_when_dedup_carried` | Real `carry_inbound_dedup` | PASS (negative control with dedup disabled fails it: `run3-…log`) |
| 1 | `test_dedup_layer_absent_kanban_idempotency_key_still_yields_one_task` | Real temp kanban DB; **key and handler are test-supplied** | PASS: two adapters, 2 creates → 1 row |
| 1 | `test_concurrent_creators_with_same_key_yield_one_task` | Real `create_task`, two real connections, deterministic interleave; asserts both threads finished with no exception and two creates succeeded before the `n == 1` check | **strict XFAIL — defect** (below) |
| 2 | `test_cancelled_turn_leaves_explicit_cancelled_state_and_is_recovered_once` | Real ledger + real backfill, 2 adapter objects, shared on-disk ledger | PASS: `processing` → `cancelled` (replied=0, nothing sent) → backfill re-runs handler once → `responded`; later scan does not rerun |
| 2 | `test_hard_kill_state_blocks_recovery_inside_claim_window_then_recovers` | Same; hard kill is **simulated** (turn left `processing`; row aged via SQL) | PASS: fresh claim blocks recovery; after 10 min window it is recovered once |
| 3 | `test_adapter_send_failure_retried_by_real_send_with_retry` | Real `_send_with_retry`; `send` returns retryable failure then success; backoff sleep patched | PASS: handler 1×, send 2× same content, ledger `responded` once |
| 3 | `test_kanban_notifier_transport_failure_rewinds_then_delivers_once` | Real kanban DB + real `_kanban_notifier_watcher`; adapter `send` raises once | PASS over 3 ticks: 2 attempts, 1 delivered notice, 1 `completed` event, tick 3 silent |
| 4 | `test_second_event_queues_behind_first_and_runs_after_it` | Real session guard + `_pending_messages` | PASS: event 2 does not start while 1 runs; order and replies `[8001, 8002]` |

## Defect found (not fixed)

`create_task` checks `idempotency_key` before its write transaction (`kanban_db.py:1252-1257`, comment
admits a concurrent race may double-insert) and `idx_tasks_idempotency` is a plain, non-UNIQUE index
(`kanban_db_connect.py:953`). With both callers' lookups completing before either insert, two live tasks
exist for one key (`--runxfail` log: `2 live tasks for one idempotency key`). So the kanban key is
at-most-once only for serialized callers.

## Facts that bound the claims

- In the seams inspected (Discord ingress/dispatch, base handle_message/queue, `create_task`, kanban notifier),
  no code derives a kanban idempotency key from an inbound chat message id; the only key sources found were
  the kanban CLI arg and the api_server `Idempotency-Key` header. Behaviour driven by the agent itself (e.g. a
  prompt-derived `kanban create` with a key) was not tested. Scenario 1's first test therefore shows that, with
  a mocked handler creating one real temporary task WITHOUT any key, duplicate ingress yields one row through
  the in-memory dedup alone. The separate stable-key test uses a test-supplied key and handler: it illustrates
  what an integration could get and is not production wiring.
- Inbound dedup is in-memory, TTL 300s: a replay after expiry or after an un-carried process restart is
  admitted again (not tested; read from `helpers.py:20-45`).
- Discord ledger rows are written only when `DISCORD_MISSED_MESSAGE_BACKFILL` is enabled (adapter checks
  `_missed_message_backfill_enabled`); tests enable it.

## Parent review and replay (reported by parent; not re-run by this author)

- Installed baseline `818c13be1dc`: 9 tests → 8 passed, 1 strict xfail; `--runxfail` → exactly 1 failure
  (`2 live tasks for one idempotency key`), 8 passed.
- Nearby existing suites `test_message_deduplicator.py`, `test_discord_double_dispatch.py`,
  `test_kanban_notifier.py`: 22 passed.
- Separate fork-main worktree, only the 2 test files cherry-picked onto
  `5eb1381f3965fd96454d1c32f6bbefcd99cda347`: same 9-test results.
- Parent logs: `parent-rerun.log/.xml`, `parent-runxfail.log/.xml`, `fork-main-replay.log/.xml`,
  `parent-nearby-regressions.log`.
- Publication PR: https://github.com/bajaj-aditya/hermes-agent/pull/2 at head
  `6cf9ced7ada8b79fc6fba86517ae26d46ecd8678` before this documentation correction. Merge status is not asserted here.

## Not checked (unchecked boundaries)

- No claim of exactly-once anywhere. These are at-least-once-with-dedup mechanisms; the tests show specific
  observed behaviours under specific interleavings.
- Real process crash/SIGKILL, SQLite durability under kill, multi-process races (other than the one
  deterministic two-thread interleave), WAL/fsync behaviour.
- A send that failed *after* the platform accepted it (ambiguous failure / timeout): `_send_with_retry`
  deliberately does not retry timeouts; duplicate visible output in that case is not tested.
- Provider/agent-loop failure and re-run policy (handler is a mock; failure → error notice path untested).
- Real Discord event shapes (gateway resume replays, edits, thread-starter events beyond the existing
  `test_discord_double_dispatch.py`), text batching (`_text_batch_delay_seconds` set to 0), busy-text
  debounce, and merging of ≥2 queued follow-ups (pending slot merges text; only 2 events tested).
- Notifier wake/`notify+wake` legs and `last_ping_event_id` checkpoint; only the passive ping leg ran.
- Gateway runner (`GatewayRunner` wiring of handler, profiles); runner is constructed bare as in existing tests.
