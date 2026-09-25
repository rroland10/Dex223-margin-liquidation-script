"""
The freeze/liquidate transaction must carry an estimated gas limit.

It was a fixed 50_000_000. That is above the per-transaction cap (EIP-7825, 2**24), and the node also
requires 50M * maxFeePerGas up front, so on Sepolia every freeze was rejected with "gas limit too high".
"""
import asyncio
import os

os.environ.setdefault("HTTP_RPC_URL", "http://localhost:8545")
os.environ.setdefault(
    "PRIVATE_KEY", "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
)
os.environ.setdefault("MARGIN_MODULE_ADDRESS", "0x5D63230470AB553195dfaf794de3e94C69d150f9")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

import pytest  # noqa: E402
from services.liquidator import LiquidatorService  # noqa: E402


class _Call:
    def __init__(self, estimate, sent):
        self.estimate, self.sent = estimate, sent

    async def estimate_gas(self, params):
        return self.estimate

    async def build_transaction(self, params):
        self.sent.append(params)
        return {**params, "to": "0x" + "11" * 20, "data": "0x", "value": 0, "type": 2}


class _Functions:
    def __init__(self, call):
        self.call = call

    def liquidate(self, position_id, receiver):
        return self.call


class _Eth:
    account = type("A", (), {"sign_transaction": staticmethod(
        lambda tx, private_key: type("S", (), {"raw_transaction": b"\x01"})()
    )})()

    async def get_transaction_count(self, addr, block):
        return 3

    async def _value(self, v):
        return v

    @property
    def chain_id(self):
        return self._value(11155111)

    @property
    def max_priority_fee(self):
        return self._value(1_000_000_000)

    async def fee_history(self, n, block, pct):
        return {"baseFeePerGas": [1_000_000_000]}

    async def send_raw_transaction(self, raw):
        return b"\xaa" * 32


def _service(estimate):
    sent = []
    svc = LiquidatorService.__new__(LiquidatorService)
    svc.w3 = type("W", (), {"eth": _Eth()})()
    svc.mm = type("M", (), {"contract": type("C", (), {"functions": _Functions(_Call(estimate, sent))})()})()
    svc.from_address = "0x" + "22" * 20
    svc.liquidate_address = svc.from_address
    svc._nonce, svc._nonce_lock, svc._chain_id = 0, asyncio.Lock(), None
    return svc, sent


@pytest.mark.parametrize("estimate, expected", [(137_000, 178_100), (20_000_000, LiquidatorService.MAX_TX_GAS)])
async def test_gas_limit_is_the_estimate_plus_buffer_capped(estimate, expected):
    svc, sent = _service(estimate)
    await svc._send_liquidate_tx(0)
    assert sent[0]["gas"] == expected
    assert sent[0]["gas"] <= 2 ** 24
