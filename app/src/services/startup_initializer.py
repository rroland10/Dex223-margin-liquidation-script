from __future__ import annotations
import asyncio
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.contracts import MarginModuleClient
from repositories import PositionRepository
from services.positions import PositionService
from services.liquidator import LiquidatorService


class StartupInitializer:
    def __init__(
            self,
            mm: MarginModuleClient,
            session_factory: async_sessionmaker[AsyncSession],
            pos_service: PositionService,
            liquidator: LiquidatorService,
            *,
            concurrency: int = 8,
    ):
        self.mm = mm
        self.sf = session_factory
        self.pos_service = pos_service
        self.liquidator = liquidator
        self.sem = asyncio.Semaphore(concurrency)

    async def _init_one(self, pid: int):
        async with self.sem:
            dto = await self.mm.subject_to_liquidation(pid)

            # upsert
            await PositionRepository(session_factory=self.sf).upsert_position(
                id=pid,
                is_liquidated=dto.liquidated,
                predict_check_timestamp=dto.predict_timestamp,
            )

            # sync pools if not liquidated
            if not dto.liquidated:
                await self.pos_service.sync_pools(pid)

            # ⛓️ freeze immediately if eligible and nobody froze yet
            if dto.status is True and dto.liquidator is None:
                await self.liquidator.freeze_now(pid)

    async def _recover_frozen(self, pid: int) -> bool:
        """Re-arm a position this bot already froze on chain.

        LiquidatorService tracks frozen positions only in memory (`frozen_positions`), and nothing in
        the schema records the freeze. On restart that state is gone, and because run() below only
        initialises positions *missing* from the DB, an already-known frozen position was never looked
        at again: the bot had paid gas to claim liquidator rights and then never liquidated it.
        """
        async with self.sem:
            try:
                dto = await self.mm.subject_to_liquidation(pid)
            except Exception as e:
                logger.warning(f"StartupInitializer: could not read position {pid}: {e}")
                return False

            if dto.liquidated or not dto.frozen_ts:
                return False
            if dto.liquidator is None:
                return False
            if dto.liquidator.lower() != self.liquidator.liquidate_address.lower():
                return False  # somebody else holds the freeze

            # block 0 so to_liquidate() releases it on the very next block; the contract's own
            # "frozenTime < block.timestamp" check still enforces the one-block delay.
            self.liquidator.frozen_positions[pid] = 0
            logger.warning(f"StartupInitializer: recovered frozen position {pid} for liquidation")
            return True

    async def _freeze_if_eligible(self, pid: int) -> bool:
        """Freeze a known position that is liquidatable and unclaimed.

        Known positions are skipped by the initialisation below, and the per-block checks only reach
        a position through pool events or its predicted insolvency time. A position that went
        underwater while the bot was down has neither, so without this it was never frozen.
        """
        async with self.sem:
            try:
                dto = await self.mm.subject_to_liquidation(pid)
            except Exception as e:
                logger.warning(f"StartupInitializer: could not read position {pid}: {e}")
                return False
        if dto.liquidated or dto.status is not True or dto.liquidator is not None:
            return False
        await self.liquidator.freeze_now(pid)
        return True

    async def run(self):
        total = await self.mm.position_index()
        existing = set(await PositionRepository(session_factory=self.sf).get_skip_position_ids())
        missing = [pid for pid in range(total) if pid not in existing]

        # Positions already in the DB are not re-initialised, so recover any freeze we still hold.
        known = [pid for pid in range(total) if pid in existing]
        if known:
            recovered = await asyncio.gather(
                *(self._recover_frozen(pid) for pid in known), return_exceptions=True
            )
            n = sum(1 for r in recovered if r is True)
            for r in recovered:
                if isinstance(r, Exception):
                    logger.exception("StartupInitializer recovery task failed: {}", r)
            if n:
                logger.warning(f"StartupInitializer: re-armed {n} frozen position(s) after restart")

            unclaimed = [pid for pid, r in zip(known, recovered) if r is not True]
            frozen = await asyncio.gather(
                *(self._freeze_if_eligible(pid) for pid in unclaimed), return_exceptions=True
            )
            for r in frozen:
                if isinstance(r, Exception):
                    logger.exception("StartupInitializer freeze task failed: {}", r)

        if not missing:
            logger.info("StartupInitializer: no missing items")
            return

        logger.warning(f"StartupInitializer: initialization {len(missing)} positions (from {total})")



        results = await asyncio.gather(*(self._init_one(pid) for pid in missing), return_exceptions=True)
        for e in results:
            if isinstance(e, Exception):
                logger.exception("StartupInitializer task failed: {}", e)

        logger.info("StartupInitializer: completed")
