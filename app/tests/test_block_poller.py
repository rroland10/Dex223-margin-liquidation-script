"""
Regression tests for BlockPoller.

The poller is the only thing feeding the liquidation worker. If it dies, no positions are ever
liquidated again for the lifetime of the process - main.py runs it under asyncio.gather() with no
return_exceptions, so an escaping error takes the whole bot down rather than restarting the loop.
"""
import os

os.environ.setdefault("HTTP_RPC_URL", "http://localhost:8545")
os.environ.setdefault(
    "PRIVATE_KEY", "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
)
os.environ.setdefault("MARGIN_MODULE_ADDRESS", "0x5D63230470AB553195dfaf794de3e94C69d150f9")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

import asyncio  # noqa: E402
import pytest  # noqa: E402
from services.block_poller import BlockPoller  # noqa: E402


class FakeEth:
    """get_block("latest") walks a script; numbered lookups return a stub block."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = 0

    async def get_block(self, which, full_transactions=False):
        if which == "latest":
            item = self.script[min(self.calls, len(self.script) - 1)]
            self.calls += 1
            if isinstance(item, Exception):
                raise item
            return {"number": item}
        return {"number": which}


class FakeW3:
    def __init__(self, eth):
        self.eth = eth


async def drain(poller, queue, expected, timeout=5.0):
    task = asyncio.create_task(poller.run())
    got = []
    try:
        for _ in range(expected):
            got.append(await asyncio.wait_for(queue.get(), timeout=timeout))
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    return got


@pytest.mark.asyncio
async def test_survives_an_rpc_error_and_keeps_polling():
    """A JSON-RPC error must not end the poller."""
    queue: asyncio.Queue = asyncio.Queue()
    # first poll establishes the baseline, second raises, third advances
    eth = FakeEth([100, RuntimeError("rate limited"), 101])
    poller = BlockPoller(FakeW3(eth), queue, poll_interval=0.01)

    blocks = await drain(poller, queue, expected=1)
    assert [b["number"] for b in blocks] == [101], "poller died on the RPC error"


@pytest.mark.asyncio
async def test_does_not_skip_blocks_when_catching_up():
    queue: asyncio.Queue = asyncio.Queue()
    eth = FakeEth([100, 104])  # jump of 4
    poller = BlockPoller(FakeW3(eth), queue, poll_interval=0.01)

    blocks = await drain(poller, queue, expected=4)
    assert [b["number"] for b in blocks] == [101, 102, 103, 104]


@pytest.mark.asyncio
async def test_does_not_replay_blocks_already_seen():
    queue: asyncio.Queue = asyncio.Queue()
    eth = FakeEth([100, 101, 101, 102])
    poller = BlockPoller(FakeW3(eth), queue, poll_interval=0.01)

    blocks = await drain(poller, queue, expected=2)
    assert [b["number"] for b in blocks] == [101, 102]


@pytest.mark.asyncio
async def test_first_poll_only_sets_the_baseline():
    """Startup must not replay history; StartupInitializer handles existing positions."""
    queue: asyncio.Queue = asyncio.Queue()
    eth = FakeEth([500, 500])
    poller = BlockPoller(FakeW3(eth), queue, poll_interval=0.01)

    task = asyncio.create_task(poller.run())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert queue.empty()
    assert poller.last == 500
