"""
Guards against ABI drift.

The bot subscribes by topic0 computed from its bundled ABIs. If an event is renamed or removed the
subscription silently covers nothing (or, before the fix, the process died at startup with a bare
StopIteration). ABI drift has already caused problems elsewhere in this stack, so it is worth a test.
"""
import json
import os
from pathlib import Path

os.environ.setdefault("HTTP_RPC_URL", "http://localhost:8545")
os.environ.setdefault(
    "PRIVATE_KEY", "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
)
os.environ.setdefault("MARGIN_MODULE_ADDRESS", "0x5D63230470AB553195dfaf794de3e94C69d150f9")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

import pytest  # noqa: E402
from eth_utils import keccak, to_hex  # noqa: E402
from core.topics import TopicsRegistry  # noqa: E402

ABI_DIR = Path(__file__).resolve().parent.parent / "src" / "abi"
ASSET_EVENTS = ["NewAsset", "AssetRemoved"]
POOL_EVENTS = ["Initialize", "Swap", "Mint", "Burn", "Collect"]

MM_ABI = json.loads((ABI_DIR / "margin_module.json").read_text())
POOL_ABI = json.loads((ABI_DIR / "pool.json").read_text())


@pytest.fixture
def registry():
    return TopicsRegistry(
        mm_abi=MM_ABI, pool_abi=POOL_ABI,
        asset_events=ASSET_EVENTS, pool_events=POOL_EVENTS,
    )


def test_every_subscribed_event_exists_in_the_shipped_abis():
    """main.py's ASSET_EVENTS / POOL_EVENTS must all resolve, or the bot subscribes to nothing."""
    TopicsRegistry(
        mm_abi=MM_ABI, pool_abi=POOL_ABI,
        asset_events=ASSET_EVENTS, pool_events=POOL_EVENTS,
    )


def test_a_missing_event_raises_a_clear_error():
    with pytest.raises(ValueError, match="not present in the ABI"):
        TopicsRegistry(
            mm_abi=MM_ABI, pool_abi=POOL_ABI,
            asset_events=["NoSuchEvent"], pool_events=POOL_EVENTS,
        )


def test_topic0_matches_the_real_event_signature(registry):
    expected = to_hex(keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)"))
    assert registry.is_pool_topic(expected), "Swap topic0 does not match the canonical signature"


def test_asset_and_pool_topics_are_distinguished(registry):
    new_asset = to_hex(keccak(text="NewAsset(uint256,address)"))
    assert registry.is_asset_topic(new_asset)
    assert not registry.is_pool_topic(new_asset)
    assert registry.asset_event_name(new_asset) == "NewAsset"


def test_all_topics_is_deduplicated_and_complete(registry):
    assert len(registry.all_topics) == len(set(registry.all_topics))
    assert len(registry.all_topics) == len(ASSET_EVENTS) + len(POOL_EVENTS)


def test_unknown_topic_is_neither(registry):
    junk = to_hex(keccak(text="Nonsense()"))
    assert not registry.is_asset_topic(junk)
    assert not registry.is_pool_topic(junk)
    assert registry.asset_event_name(junk) is None
