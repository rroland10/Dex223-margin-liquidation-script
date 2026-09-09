import asyncio
from web3 import AsyncWeb3
from web3.types import BlockData
from loguru import logger


class BlockPoller:
    def __init__(
        self,
        w3: AsyncWeb3,
        queue: asyncio.Queue[BlockData],
        poll_interval: float = 1.0,
    ):
        self.w3 = w3
        self.queue = queue
        self.last: int | None = None
        self.poll_interval = poll_interval

    async def run(self):
        while True:
            # RetryingHTTPProvider only retries transport failures. Anything web3 raises after a
            # response lands here instead - a JSON-RPC error object (public nodes return these when
            # rate limiting), BlockNotFound during a reorg, malformed payloads. Without this guard the
            # exception escaped run(), and main.py's asyncio.gather() has no return_exceptions, so a
            # single bad poll permanently killed the poller and with it the whole liquidation bot.
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"Block poll failed, continuing: {e}")

            await asyncio.sleep(self.poll_interval)

    async def _poll_once(self) -> None:
        head = await self.w3.eth.get_block("latest", full_transactions=False)
        current = head["number"]

        if self.last is None:
            # we start with the last known
            self.last = current

        if self.last < current:
            gap = current - self.last
            if gap > 1:
                logger.warning(f"⏳ Behind by {gap} blocks: {self.last+1}..{current} (catching up)")

            # we are consistently catching up: [last+1 .. current]
            for n in range(self.last + 1, current + 1):
                # to avoid making an extra rpc for the very last one, use the already received head
                blk = head if n == current else await self.w3.eth.get_block(n, full_transactions=False)
                await self.queue.put(blk)
                logger.info(f"🗓 New block {blk['number']}")
                # only advance after the block is queued, so a failure mid-catch-up is retried
                self.last = n
