"""
The worker's handle_block retry must be bounded.

It was `while True`: a deterministic failure (an undecodable log, a persistent DB error) pinned the
worker to one block forever. The poller keeps enqueuing, so the queue grows without limit and no
position is ever liquidated again. Skipping the block is recoverable - find_positions() also unions an
overdue check on predict_check_timestamp - so bounded retries are strictly safer than wedging.
"""
import asyncio
import re
from pathlib import Path

import pytest

MAIN = (Path(__file__).resolve().parent.parent / "src" / "main.py").read_text()


def test_retry_is_bounded_in_source():
    assert "HANDLE_BLOCK_ATTEMPTS" in MAIN, "no retry bound defined"
    # the unbounded `while True:` immediately wrapping handle_block must be gone
    assert not re.search(
        r"while True:\s*\n\s*try:\s*\n\s*ids = \[pid async for pid in orchestrator\.handle_block",
        MAIN,
    ), "handle_block is still retried in an unbounded while True loop"
    assert re.search(r"for attempt in range\(1, HANDLE_BLOCK_ATTEMPTS \+ 1\)", MAIN)


@pytest.mark.asyncio
async def test_bounded_retry_gives_up_and_continues():
    """Model of the worker loop: a permanently failing block must not block later ones."""
    ATTEMPTS = 5
    processed, attempts = [], {"count": 0}

    async def handle_block(block):
        if block == "bad":
            attempts["count"] += 1
            raise RuntimeError("undecodable log")
        return [block]

    queue = asyncio.Queue()
    for b in ("bad", "good"):
        queue.put_nowait(b)

    while not queue.empty():
        block = queue.get_nowait()
        ids = []
        for attempt in range(1, ATTEMPTS + 1):
            try:
                ids = await handle_block(block)
                break
            except Exception:
                if attempt == ATTEMPTS:
                    ids = []
        processed.extend(ids)

    assert attempts["count"] == ATTEMPTS, "should stop after the bound, not retry forever"
    assert processed == ["good"], "a later block must still be processed"
