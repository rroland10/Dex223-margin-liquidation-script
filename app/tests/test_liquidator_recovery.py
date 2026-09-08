"""
Regression tests for LiquidatorService failure handling.

Each test here corresponds to a bug that could lose money or corrupt state:
  * a freeze whose receipt wait raised left the position in _freeze_txs forever, so it was frozen
    on chain (gas paid, liquidator rights held) but never liquidated again for the process lifetime;
  * liquidate() removed the position from frozen_positions before the tx succeeded, so any error
    dropped it permanently with no retry;
  * liquidate() wrote is_liquidated=True straight after send_raw_transaction, so a reverted or
    dropped tx still recorded a liquidation in the database.
"""
import os

os.environ.setdefault("HTTP_RPC_URL", "http://localhost:8545")
os.environ.setdefault(
    "PRIVATE_KEY", "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
)
os.environ.setdefault("MARGIN_MODULE_ADDRESS", "0x5D63230470AB553195dfaf794de3e94C69d150f9")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

import pytest  # noqa: E402
from services import liquidator as liq_mod  # noqa: E402
from services.liquidator import LiquidatorService, SubjectToLiquidation  # noqa: E402


class FakeEth:
    def __init__(self, receipt=None, raise_on_receipt=None):
        self._receipt = receipt
        self._raise = raise_on_receipt
        self.chain_id = 11155111

    async def wait_for_transaction_receipt(self, tx_hash, timeout=600):
        if self._raise:
            raise self._raise
        return self._receipt


class FakeW3:
    def __init__(self, eth):
        self.eth = eth


class FakeHash(bytes):
    def to_0x_hex(self):
        return "0x" + self.hex()


class FakeRepo:
    calls: list = []

    def __init__(self, session_factory=None):
        pass

    async def set_liquidated(self, position_id):
        FakeRepo.calls.append(("set_liquidated", position_id))

    async def set_predict_timestamp(self, position_id, ts=None):
        FakeRepo.calls.append(("set_predict_timestamp", position_id))


def build(monkeypatch, eth, subject, send_raises=None):
    monkeypatch.setattr(liq_mod, "PositionRepository", FakeRepo)
    FakeRepo.calls = []

    svc = LiquidatorService.__new__(LiquidatorService)  # bypass __init__ (no real account/db needed)
    svc.w3 = FakeW3(eth)
    svc.sf = None
    svc.from_address = "0x0000000000000000000000000000000000000009"
    svc.liquidate_address = "0x0000000000000000000000000000000000000009"
    svc._freeze_txs = {}
    svc.frozen_positions = {}
    svc._liquidate_attempts = {}

    class FakeMM:
        async def subject_to_liquidation(self, pid):
            return subject

    svc.mm = FakeMM()

    async def _send(pid):
        if send_raises:
            raise send_raises
        return FakeHash(b"\x11" * 32)

    svc._send_liquidate_tx = _send
    return svc


ELIGIBLE = SubjectToLiquidation(
    status=True,
    liquidator="0x0000000000000000000000000000000000000009",
    frozen_ts=1,
    predict_timestamp=1,
    liquidated=False,
)


@pytest.mark.asyncio
async def test_freeze_receipt_timeout_does_not_strand_the_position(monkeypatch):
    """A raising receipt wait must not leave the position stuck in _freeze_txs."""
    eth = FakeEth(raise_on_receipt=TimeoutError("receipt timeout"))
    svc = build(monkeypatch, eth, SubjectToLiquidation(True, None, 1, 1, False))

    with pytest.raises(TimeoutError):
        await svc.frozen(42, {"number": 100})

    assert 42 not in svc._freeze_txs, "position stranded: it would never be retried"


@pytest.mark.asyncio
async def test_liquidate_failure_keeps_position_for_retry(monkeypatch):
    """A failed send must not silently drop an already-frozen position."""
    svc = build(monkeypatch, FakeEth(), ELIGIBLE, send_raises=RuntimeError("rpc down"))
    svc.frozen_positions[7] = 50

    await svc.liquidate(7, {"number": 51})

    assert 7 in svc.frozen_positions, "frozen position dropped after a transient failure"
    assert ("set_liquidated", 7) not in FakeRepo.calls, "DB marked liquidated despite failure"


@pytest.mark.asyncio
async def test_liquidate_does_not_mark_db_when_tx_reverts(monkeypatch):
    """status != 1 must not be recorded as a successful liquidation."""
    svc = build(monkeypatch, FakeEth(receipt={"status": 0, "blockNumber": 52}), ELIGIBLE)
    svc.frozen_positions[8] = 50

    await svc.liquidate(8, {"number": 51})

    assert ("set_liquidated", 8) not in FakeRepo.calls, "reverted tx recorded as liquidated"


@pytest.mark.asyncio
async def test_liquidate_success_marks_db_and_clears_state(monkeypatch):
    svc = build(monkeypatch, FakeEth(receipt={"status": 1, "blockNumber": 52}), ELIGIBLE)
    svc.frozen_positions[9] = 50

    await svc.liquidate(9, {"number": 51})

    assert ("set_liquidated", 9) in FakeRepo.calls
    assert 9 not in svc.frozen_positions
    assert 9 not in svc._liquidate_attempts


@pytest.mark.asyncio
async def test_liquidate_gives_up_after_max_attempts(monkeypatch):
    """Retries are capped so a permanently reverting position cannot burn gas forever."""
    svc = build(monkeypatch, FakeEth(), ELIGIBLE, send_raises=RuntimeError("always fails"))
    svc.frozen_positions[10] = 50

    for _ in range(LiquidatorService.MAX_LIQUIDATE_ATTEMPTS):
        await svc.liquidate(10, {"number": 51})

    assert 10 not in svc.frozen_positions, "should stop retrying after the cap"
    assert ("set_liquidated", 10) not in FakeRepo.calls


@pytest.mark.asyncio
async def test_ineligible_position_is_dropped_not_retried(monkeypatch):
    ineligible = SubjectToLiquidation(False, None, 1, 1, False)
    svc = build(monkeypatch, FakeEth(), ineligible)
    svc.frozen_positions[11] = 50

    await svc.liquidate(11, {"number": 51})

    assert 11 not in svc.frozen_positions
    assert ("set_liquidated", 11) not in FakeRepo.calls
