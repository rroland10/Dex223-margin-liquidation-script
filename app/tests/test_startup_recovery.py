"""
A freeze this bot paid for must survive a restart.

LiquidatorService keeps frozen positions in memory only, and the schema records no freeze state.
StartupInitializer.run() initialises just the positions *missing* from the DB, so before this fix an
already-known frozen position was never revisited: the bot held liquidator rights on chain, paid the
gas for them, and then never liquidated.
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
from services.startup_initializer import StartupInitializer  # noqa: E402
from services.liquidator import SubjectToLiquidation  # noqa: E402

US = "0x0000000000000000000000000000000000000009"
THEM = "0x000000000000000000000000000000000000000B"


class FakeMM:
    def __init__(self, subjects):
        self.subjects = subjects

    async def subject_to_liquidation(self, pid):
        return self.subjects[pid]


class FakeLiquidator:
    def __init__(self):
        self.frozen_positions = {}
        self.liquidate_address = US
        self.freeze_now_calls = []

    async def freeze_now(self, pid):
        self.freeze_now_calls.append(pid)


def build(subjects):
    init = StartupInitializer.__new__(StartupInitializer)
    init.mm = FakeMM(subjects)
    init.sem = asyncio.Semaphore(4)
    init.liquidator = FakeLiquidator()
    return init


@pytest.mark.asyncio
async def test_recovers_a_freeze_we_still_hold():
    subjects = {7: SubjectToLiquidation(True, US, 1234, None, False)}
    init = build(subjects)
    assert await init._recover_frozen(7) is True
    assert 7 in init.liquidator.frozen_positions, "frozen position was not re-armed"


@pytest.mark.asyncio
async def test_ignores_a_freeze_held_by_someone_else():
    subjects = {7: SubjectToLiquidation(True, THEM, 1234, None, False)}
    init = build(subjects)
    assert await init._recover_frozen(7) is False
    assert init.liquidator.frozen_positions == {}


@pytest.mark.asyncio
async def test_ignores_an_unfrozen_position():
    subjects = {7: SubjectToLiquidation(True, None, None, 99, False)}
    init = build(subjects)
    assert await init._recover_frozen(7) is False
    assert init.liquidator.frozen_positions == {}


@pytest.mark.asyncio
async def test_ignores_an_already_liquidated_position():
    subjects = {7: SubjectToLiquidation(False, US, 1234, None, True)}
    init = build(subjects)
    assert await init._recover_frozen(7) is False
    assert init.liquidator.frozen_positions == {}


@pytest.mark.asyncio
async def test_matches_the_liquidator_address_case_insensitively():
    subjects = {7: SubjectToLiquidation(True, US.upper(), 1234, None, False)}
    init = build(subjects)
    assert await init._recover_frozen(7) is True


@pytest.mark.asyncio
async def test_an_rpc_failure_does_not_abort_startup():
    class Boom:
        async def subject_to_liquidation(self, pid):
            raise RuntimeError("rpc down")

    init = build({})
    init.mm = Boom()
    assert await init._recover_frozen(7) is False  # logged and skipped, not raised


@pytest.mark.asyncio
async def test_freezes_a_known_position_that_went_underwater_unclaimed():
    subjects = {7: SubjectToLiquidation(True, None, None, None, False)}
    init = build(subjects)
    assert await init._freeze_if_eligible(7) is True
    assert init.liquidator.freeze_now_calls == [7]


@pytest.mark.asyncio
@pytest.mark.parametrize("subject", [
    SubjectToLiquidation(False, None, None, 99, False),   # healthy
    SubjectToLiquidation(True, THEM, 1234, None, False),  # someone else froze it
    SubjectToLiquidation(True, None, None, None, True),   # already liquidated
])
async def test_does_not_freeze_a_position_that_is_not_ours_to_take(subject):
    init = build({7: subject})
    assert await init._freeze_if_eligible(7) is False
    assert init.liquidator.freeze_now_calls == []
