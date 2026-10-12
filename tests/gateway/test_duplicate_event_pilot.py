"""Duplicate-event pilot: what the production gateway actually does with replayed,
interrupted, failed-then-retried and back-to-back inbound events.

Everything here drives real production code; only the edges are replaced.

REAL (production, unmodified):
  * ``DiscordAdapter._dispatch_discord_message`` / ``_handle_message`` and its
    ``MessageDeduplicator`` (plugins/platforms/discord/adapter.py)
  * ``BasePlatformAdapter.handle_message`` / ``_process_message_background`` /
    pending-message queue / ``_send_with_retry`` (gateway/platforms/base.py)
  * the Discord recovery ledger (real SQLite file under the temp HERMES_HOME) and
    ``_run_missed_message_backfill``
  * the kanban DB (real SQLite under a temp path) and the gateway kanban notifier
    (``GatewayRunner._kanban_notifier_watcher`` -> gateway/kanban_watchers_notifier.py)

MOCKED (the boundary):
  * Discord network: ``adapter.send`` / ``send_typing`` / history iterators; no discord client
  * the gateway message handler (``adapter._message_handler``) -- the "work" invocation
  * ``asyncio.sleep`` inside retry back-off
  * all sockets: an autouse fixture refuses AF_INET/AF_INET6 connects and DNS

NOT proven here: process-crash durability (nothing is killed), cross-process races, the
real Discord gateway's redelivery behaviour, the agent loop/provider. The kanban idempotency
key used in the handler stand-in is chosen by the TEST; production has no inbound-event ->
kanban-task wiring that derives one (see findings doc).
"""

import asyncio
import datetime as dt
import socket
import sqlite3
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import SendResult
from gateway.run import GatewayRunner
import gateway.platforms.helpers as helpers

import plugins.platforms.discord.adapter as discord_platform  # noqa: E402
from plugins.platforms.discord.adapter import DiscordAdapter  # noqa: E402
from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_notify as kbn

discord = discord_platform.discord


# ---------------------------------------------------------------------------
# Hermetic environment
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _block_network(monkeypatch):
    """Any attempt to open an IP connection or resolve a name fails the test."""
    def _refuse(*_a, **_k):
        raise AssertionError("pilot tests must not touch the network")

    real_connect = socket.socket.connect

    def guarded_connect(self, address, *a, **k):
        if self.family in (socket.AF_INET, socket.AF_INET6):
            _refuse()
        return real_connect(self, address, *a, **k)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", _refuse)
    monkeypatch.setattr(socket, "getaddrinfo", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)


@pytest.fixture
def home(tmp_path, monkeypatch):
    h = tmp_path / "hermes_home"
    h.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(h))
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "kanban.db"))
    for var in ("DISCORD_AUTO_THREAD", "DISCORD_NO_THREAD_CHANNELS", "DISCORD_FREE_RESPONSE_CHANNELS",
                "DISCORD_ALLOWED_CHANNELS", "DISCORD_IGNORED_CHANNELS", "DISCORD_ALLOW_BOTS"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DISCORD_REQUIRE_MENTION", "false")
    monkeypatch.setenv("DISCORD_ALLOW_ALL_USERS", "true")
    monkeypatch.setenv("DISCORD_MISSED_MESSAGE_BACKFILL", "true")
    kb.init_db()
    return h


class _Thread(discord.Thread):
    """Synthetic Discord thread (isinstance(discord.Thread) holds, so the adapter takes its thread path)."""

    def __init__(self, thread_id=5000, parent_id=100):
        self.id = thread_id
        self.name = "synthetic-thread"
        self.parent_id = parent_id
        self.parent = SimpleNamespace(id=parent_id, name="general")
        self.guild = SimpleNamespace(id=1, name="Synthetic Server")
        self.topic = None
        self.history_messages = []

    def history(self, **_kwargs):
        async def _gen():
            for m in self.history_messages:
                yield m
        return _gen()


def synthetic_message(message_id, channel, content="synthetic request"):
    return SimpleNamespace(
        id=message_id, content=content, mentions=[], attachments=[], reference=None,
        message_snapshots=None, reactions=[], created_at=datetime.now(timezone.utc),
        channel=channel, guild=getattr(channel, "guild", None),
        author=SimpleNamespace(id=42, display_name="Synthetic User", name="synthetic", bot=False),
        type=discord.MessageType.default,
    )


class Handler:
    """Stand-in for the gateway runner's message handler: the *work* boundary."""

    def __init__(self, reply="synthetic reply"):
        self.calls = []
        self.reply = reply
        self.gates = {}  # message id -> asyncio.Event the call waits on

    async def __call__(self, event):
        self.calls.append(event.message_id)
        gate = self.gates.get(event.message_id)
        if gate is not None:
            await gate.wait()
        return f"{self.reply} to {event.message_id}"


def make_adapter(handler, *, send=None):
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="synthetic-token"))
    adapter._client = SimpleNamespace(
        user=SimpleNamespace(id=999, bot=True, display_name="Hermes", name="hermes"),
        get_channel=lambda _id: None,
    )
    adapter._ready_event.set()
    adapter._text_batch_delay_seconds = 0
    adapter.send = send or AsyncMock(return_value=SendResult(success=True, message_id="out-1"))
    adapter.send_typing = AsyncMock()
    adapter.set_message_handler(handler)
    return adapter


async def settle(adapter, timeout=5.0):
    """Wait until the adapter has no running turn and no queued follow-up."""
    async def _idle():
        while adapter._active_sessions or adapter._pending_messages or adapter._background_tasks:
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_idle(), timeout)


def carry_dedup(predecessor, rebuilt):
    """The real reconnect seam, across both signatures: older sources take a cache dict built by
    ``inbound_dedup_caches(adapter)``; current source takes the predecessor adapter directly."""
    if hasattr(helpers, "inbound_dedup_caches"):
        helpers.carry_inbound_dedup(helpers.inbound_dedup_caches(predecessor), rebuilt)
    else:
        helpers.carry_inbound_dedup(predecessor, rebuilt)


class RecordingAdapter:
    """Kanban-notifier delivery target (mirrors tests/gateway/test_kanban_notifier.py)."""

    def __init__(self):
        self.sent = []
        self.handled = []

    async def send(self, chat_id, text, metadata=None):
        self.sent.append({"chat_id": chat_id, "text": text, "metadata": metadata or {}})

    async def handle_message(self, event):
        self.handled.append(event)
        event._gateway_accepted = True


def _make_notifier_runner(adapter):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._kanban_sub_fail_counts = {}
    runner._kanban_dispatcher_lock_handle = object()  # model a gateway that owns the dispatcher lock
    return runner


async def _run_one_notifier_tick(monkeypatch, runner):
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        if delay == 5:
            return None
        runner._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await runner._kanban_notifier_watcher(interval=1)


def ledger_rows(home):
    """Read the real recovery ledger straight from disk (independent of the adapter)."""
    paths = [p for p in home.rglob("*.db") if "discord" in p.name.lower()]
    assert paths, f"no discord recovery ledger under {home}: {list(home.rglob('*'))}"
    conn = sqlite3.connect(paths[0])
    try:
        return {r[0]: r[1:] for r in conn.execute(
            "SELECT message_id, status, replied, attempts FROM discord_messages")}
    finally:
        conn.close()


def age_ledger_claim(adapter, message_id, minutes=11):
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes)).isoformat()
    adapter._with_discord_recovery_db(lambda c: c.execute(
        "UPDATE discord_messages SET updated_at=? WHERE message_id=?", (old, message_id)))


# ---------------------------------------------------------------------------
# Scenario 1 -- repeated identical inbound event => ONE logical task
# ---------------------------------------------------------------------------

class TestScenario1RepeatedInboundEvent:

    @pytest.mark.asyncio
    async def test_adapter_boundary_sequential_and_concurrent_replay_runs_handler_once(self, home):
        """BOUNDARY: real DiscordAdapter + base queue + real temp kanban DB. The handler is mocked but
        creates a real task row WITHOUT any idempotency_key, so one row means ingress dedup alone held."""
        class TaskCreatingHandler(Handler):
            async def __call__(self, event):
                conn = kbc.connect()
                try:
                    kb.create_task(conn, title=f"task for {event.message_id}", assignee="worker")
                finally:
                    conn.close()
                return await super().__call__(event)

        handler = TaskCreatingHandler()
        adapter = make_adapter(handler)
        msg = synthetic_message(7001, _Thread())

        results = await asyncio.gather(
            adapter._dispatch_discord_message(msg),
            adapter._dispatch_discord_message(msg),
            adapter._dispatch_discord_message(msg),
        )
        await settle(adapter)
        await adapter._dispatch_discord_message(msg)  # late replay after the turn finished
        await settle(adapter)

        assert results.count(True) == 1
        assert handler.calls == ["7001"]
        assert adapter.send.await_count == 1
        conn = kbc.connect()
        try:
            rows = conn.execute("SELECT COUNT(*) FROM tasks WHERE idempotency_key IS NULL").fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        finally:
            conn.close()
        assert (rows, total) == (1, 1)

    @pytest.mark.asyncio
    async def test_replay_after_adapter_rebuild_is_dropped_when_dedup_carried(self, home):
        """BOUNDARY: real reconnect seam (``carry_inbound_dedup``, via ``carry_dedup``) between two adapter objects."""
        handler = Handler()
        first = make_adapter(handler)
        msg = synthetic_message(7002, _Thread())
        await first._dispatch_discord_message(msg)
        await settle(first)

        rebuilt = make_adapter(handler)
        carry_dedup(first, rebuilt)
        assert await rebuilt._dispatch_discord_message(msg) is False
        await settle(rebuilt)

        assert handler.calls == ["7002"]

    @pytest.mark.asyncio
    async def test_dedup_layer_absent_kanban_idempotency_key_still_yields_one_task(self, home):
        """BOUNDARY: real kanban SQLite DB. The key (``discord:<message id>``) and the handler that
        applies it are TEST-SUPPLIED -- production has no such wiring. This proves only that IF an
        integration passes a stable key, a replay that gets past the in-memory dedup (fresh adapter,
        no carry) still resolves to the one task row."""
        created = []

        async def handler(event):
            conn = kbc.connect()
            try:
                created.append(kb.create_task(
                    conn, title="from discord", assignee="worker",
                    idempotency_key=f"discord:{event.message_id}"))
            finally:
                conn.close()
            return "ok"

        msg = synthetic_message(7003, _Thread())
        for _ in range(2):  # two independent adapters == in-memory dedup does not span them
            adapter = make_adapter(handler)
            await adapter._dispatch_discord_message(msg)
            await settle(adapter)

        assert len(created) == 2 and len(set(created)) == 1
        conn = kbc.connect()
        try:
            n = conn.execute("SELECT COUNT(*) FROM tasks WHERE idempotency_key=?", ("discord:7003",)).fetchone()[0]
        finally:
            conn.close()
        assert n == 1

    @pytest.mark.xfail(
        strict=True,
        reason="create_task checks idempotency_key BEFORE its write txn and idx_tasks_idempotency is not "
               "UNIQUE (kanban_db.py ~L1252, kanban_db_connect.py L953); two concurrent creators both "
               "miss and both insert. Run with --runxfail to see the failure.")
    def test_concurrent_creators_with_same_key_yield_one_task(self, home):
        """Deterministic interleaving of the real create_task on two real connections: both
        lookups complete before either insert."""
        barrier = threading.Barrier(2, timeout=10)

        class Conn:
            def __init__(self, inner):
                self._inner = inner

            def execute(self, sql, *a, **k):
                cur = self._inner.execute(sql, *a, **k)
                if "idempotency_key = ?" in sql and sql.lstrip().startswith("SELECT id FROM tasks"):
                    barrier.wait()  # both threads have now looked, before either writes
                return cur

            def __getattr__(self, name):
                return getattr(self._inner, name)

        ids, errors = [], []

        def creator():
            try:
                conn = kbc.connect()
                try:
                    ids.append(kb.create_task(Conn(conn), title="race", assignee="w", idempotency_key="race-key"))
                finally:
                    conn.close()
            except BaseException as exc:  # a thread error must not masquerade as the expected xfail
                errors.append(exc)

        threads = [threading.Thread(target=creator) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        assert not any(t.is_alive() for t in threads), "creator thread did not finish"
        assert not errors, f"creator thread raised: {errors!r}"
        assert len(ids) == 2, f"expected two successful creates, got {ids!r}"

        conn = kbc.connect()
        try:
            n = conn.execute("SELECT COUNT(*) FROM tasks WHERE idempotency_key='race-key' AND status!='archived'"
                             ).fetchone()[0]
        finally:
            conn.close()
        assert n == 1, f"{n} live tasks for one idempotency key (ids={ids})"


# ---------------------------------------------------------------------------
# Scenario 2 -- interruption before acknowledgement => explicit recoverable state
# ---------------------------------------------------------------------------

class TestScenario2InterruptionRecovery:
    """Exercises the real recovery ledger + ``_run_missed_message_backfill`` across two adapter
    OBJECTS sharing one on-disk ledger. No process is killed: this is cancellation + a fresh
    adapter, plus a hand-aged ledger row for the hard-kill state. It does NOT prove crash
    durability of an in-flight SQLite write or of OS-level kill."""

    @staticmethod
    def _wire_backfill(adapter, thread, monkeypatch):
        async def candidates(_channels):
            for m in thread.history_messages:
                yield m
        monkeypatch.setattr(adapter, "_iter_missed_message_backfill_candidates", candidates)
        monkeypatch.setattr(adapter, "_missed_message_backfill_channels", lambda: {str(thread.id)})

    @pytest.mark.asyncio
    async def test_cancelled_turn_leaves_explicit_cancelled_state_and_is_recovered_once(self, home, monkeypatch):
        thread = _Thread()
        msg = synthetic_message(7101, thread)
        thread.history_messages = [msg]
        stuck = Handler()
        stuck.gates["7101"] = asyncio.Event()  # never set: turn is mid-flight, unacknowledged

        a = make_adapter(stuck)
        await a._dispatch_discord_message(msg)
        for _ in range(200):
            if stuck.calls:
                break
            await asyncio.sleep(0.01)
        assert ledger_rows(home)["7101"][0] == "processing"  # explicit in-flight state

        # Interrupt: cancel the running turn before any final reply is acknowledged.
        for task in list(a._session_tasks.values()):
            a._expected_cancelled_tasks.add(task)
            task.cancel()
        await asyncio.gather(*a._session_tasks.values(), return_exceptions=True)
        status, replied, _ = ledger_rows(home)["7101"]
        assert (status, replied) == ("cancelled", 0)
        a.send.assert_not_awaited()

        # "Restart": a new adapter object, empty in-memory dedup, same on-disk ledger.
        ok = Handler()
        b = make_adapter(ok)
        self._wire_backfill(b, thread, monkeypatch)
        await b._run_missed_message_backfill()
        await settle(b)
        assert ok.calls == ["7101"]
        assert ledger_rows(home)["7101"][0] == "responded"

        # A second scan after completion must not re-run it.
        b._dedup.clear()
        await b._run_missed_message_backfill()
        await settle(b)
        assert ok.calls == ["7101"]

    @pytest.mark.asyncio
    async def test_hard_kill_state_blocks_recovery_inside_claim_window_then_recovers(self, home, monkeypatch):
        """Hard-kill is SIMULATED: A's turn is left 'processing' (as a SIGKILL would leave it) and a
        second adapter scans while A is still alive. The row is an explicit 'processing' claim."""
        thread = _Thread()
        msg = synthetic_message(7102, thread)
        thread.history_messages = [msg]
        stuck = Handler()
        stuck.gates["7102"] = asyncio.Event()
        a = make_adapter(stuck)
        await a._dispatch_discord_message(msg)
        for _ in range(200):
            if stuck.calls:
                break
            await asyncio.sleep(0.01)

        ok = Handler()
        b = make_adapter(ok)
        self._wire_backfill(b, thread, monkeypatch)
        try:
            await b._run_missed_message_backfill()
            await settle(b)
            assert ok.calls == []  # fresh 'processing' claim (<10 min) is respected

            age_ledger_claim(b, "7102")  # claim window elapsed with no completion
            await b._run_missed_message_backfill()
            await settle(b)
            assert ok.calls == ["7102"]
            assert ledger_rows(home)["7102"][0] == "responded"
        finally:
            for task in list(a._session_tasks.values()):
                a._expected_cancelled_tasks.add(task)
                task.cancel()
            await asyncio.gather(*a._session_tasks.values(), return_exceptions=True)


# ---------------------------------------------------------------------------
# Scenario 3 -- output transport failure, then retry => one completion, one work invocation
# ---------------------------------------------------------------------------

class TestScenario3TransportFailureRetry:

    @pytest.mark.asyncio
    async def test_adapter_send_failure_retried_by_real_send_with_retry(self, home, monkeypatch):
        """Real ``_send_with_retry`` + real final-delivery ledger; ``send`` (network) fails once with a
        retryable error, then succeeds. Back-off sleep is patched out."""
        sleeps = []

        real_sleep = asyncio.sleep

        async def fast_sleep(delay):
            if delay >= 1:  # retry back-off only; short polls keep their real timing
                sleeps.append(delay)
                delay = 0
            await real_sleep(delay)

        import gateway.platforms.base as base_mod
        monkeypatch.setattr(base_mod.asyncio, "sleep", fast_sleep)

        handler = Handler()
        send = AsyncMock(side_effect=[
            SendResult(success=False, error="connection reset by peer", retryable=True),
            SendResult(success=True, message_id="out-2"),
        ])
        adapter = make_adapter(handler, send=send)
        msg = synthetic_message(7201, _Thread())
        await adapter._dispatch_discord_message(msg)
        await settle(adapter)

        assert handler.calls == ["7201"]                       # one work invocation
        assert send.await_count == 2                            # failed attempt + one retry
        assert {c.kwargs["content"] for c in send.await_args_list} == {"synthetic reply to 7201"}
        assert len(sleeps) >= 1                                 # the retry path (not a rerun) did the waiting
        status, replied, _ = ledger_rows(home)["7201"]
        assert (status, replied) == ("responded", 1)            # exactly one completion recorded

    def test_kanban_notifier_transport_failure_rewinds_then_delivers_once(self, home, monkeypatch):
        """Real kanban DB + real notifier loop (``_kanban_notifier_watcher``). The task is completed
        ONCE (the work); the platform ``send`` fails on tick 1 and succeeds on tick 2."""
        conn = kbc.connect()
        try:
            tid = kb.create_task(conn, title="notify once", assignee="worker")
            kbn.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="chat-1")
            kb.complete_task(conn, tid, summary="work done once")
        finally:
            conn.close()

        class FlakyAdapter(RecordingAdapter):
            def __init__(self):
                super().__init__()
                self.attempts = 0

            async def send(self, chat_id, text, metadata=None):
                self.attempts += 1
                if self.attempts == 1:
                    raise ConnectionError("synthetic transport failure")
                return await super().send(chat_id, text, metadata)

        adapter = FlakyAdapter()
        runner = _make_notifier_runner(adapter)
        for _ in range(3):  # tick1 fails+rewinds, tick2 delivers+advances, tick3 must be silent
            runner._running = True
            asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

        assert adapter.attempts == 2
        assert len(adapter.sent) == 1 and tid in adapter.sent[0]["text"]
        conn = kbc.connect()
        try:
            completed = conn.execute(
                "SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='completed'", (tid,)).fetchone()[0]
        finally:
            conn.close()
        assert completed == 1  # one logical completion event backs the one delivered notice


# ---------------------------------------------------------------------------
# Scenario 4 -- two distinct events, same thread => order preserved by the real queue
# ---------------------------------------------------------------------------

class TestScenario4OrderingInThread:

    @pytest.mark.asyncio
    async def test_second_event_queues_behind_first_and_runs_after_it(self, home):
        thread = _Thread()
        handler = Handler()
        handler.gates["8001"] = asyncio.Event()
        adapter = make_adapter(handler)

        first = synthetic_message(8001, thread, "first")
        second = synthetic_message(8002, thread, "second")
        await adapter._dispatch_discord_message(first)
        for _ in range(200):
            if handler.calls:
                break
            await asyncio.sleep(0.01)
        await adapter._dispatch_discord_message(second)

        assert handler.calls == ["8001"]          # second must NOT start while first is running
        assert len(adapter._pending_messages) == 1  # held in the real pending queue

        handler.gates["8001"].set()
        await settle(adapter)

        assert handler.calls == ["8001", "8002"]
        replies = [c.kwargs["content"] for c in adapter.send.await_args_list]
        assert replies == ["synthetic reply to 8001", "synthetic reply to 8002"]
