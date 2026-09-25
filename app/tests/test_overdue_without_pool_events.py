"""
A position can become liquidatable from interest alone, with no Swap/Mint/Burn on its pools.

The overdue check on predict_check_timestamp used to run only inside the per-pool loop, so a block
without pool events never reached it. On a quiet network that is almost every block: a live Sepolia
position went underwater and the bot never looked at it.
"""
import os

os.environ.setdefault("HTTP_RPC_URL", "http://localhost:8545")
os.environ.setdefault(
    "PRIVATE_KEY", "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
)
os.environ.setdefault("MARGIN_MODULE_ADDRESS", "0x5D63230470AB553195dfaf794de3e94C69d150f9")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

from services.event_orchestrator import EventOrchestrator  # noqa: E402


class _Eth:
    async def get_logs(self, _):
        return []


class _W3:
    eth = _Eth()


class _Topics:
    all_topics = ["0x" + "00" * 32]


class _Pools:
    def __init__(self, overdue, by_pool=None):
        self.overdue, self.by_pool = overdue, by_pool or {}

    async def affected_positions(self, pool, ts):
        return self.by_pool.get(pool, [])

    async def overdue_positions(self, ts):
        return self.overdue


def _orchestrator(pools):
    return EventOrchestrator(w3=_W3(), topics=_Topics(), mm=None, positions=None, pools=pools)


async def test_block_without_logs_still_yields_overdue_positions():
    orch = _orchestrator(_Pools(overdue=[7]))
    assert [p async for p in orch.handle_block({"number": 1, "timestamp": 100})] == [7]


async def test_overdue_position_already_found_through_a_pool_is_yielded_once():
    orch = _orchestrator(_Pools(overdue=[7, 8], by_pool={"0xpool": [7]}))

    async def logs(_):
        return [{"address": "0xpool"}]

    orch._get_logs = logs
    orch.topics = type("T", (), {
        "all_topics": ["x"],
        "topic0_of": staticmethod(lambda log: "swap"),
        "is_asset_topic": staticmethod(lambda t: False),
        "is_pool_topic": staticmethod(lambda t: True),
    })()
    assert [p async for p in orch.handle_block({"number": 1, "timestamp": 100})] == [7, 8]
