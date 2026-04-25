"""Quant Trading System — Instrument Metadata Manager.

Syncs instrument definitions from config/instruments.yaml into the database,
manages exchange token mappings, and provides lookup utilities.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
import yaml

from src.core.enums import AssetClass, Exchange
from src.core.exceptions import DatabaseReadError, DatabaseWriteError
from src.core.logging import get_logger
from src.core.models import Instrument
from src.storage.database import Database

log = get_logger(__name__)

# Default path to instruments config
INSTRUMENTS_CONFIG = Path(__file__).resolve().parent.parent.parent / "config" / "instruments.yaml"


class InstrumentMetadataManager:
    """Manages instrument metadata in the database.

    Responsibilities:
    - Load instrument definitions from YAML config
    - Sync them to the database (upsert)
    - Resolve exchange-specific tokens (e.g., Angel One numeric tokens)
    - Provide lookup by symbol, exchange, or token
    """

    # Exchange string → Exchange enum mapping for config parsing
    _EXCHANGE_MAP = {
        "NSE": Exchange.ANGEL_ONE,
        "BSE": Exchange.ANGEL_ONE,
        "BINANCE": Exchange.BINANCE,
    }

    _ASSET_CLASS_MAP = {
        "EQUITY": AssetClass.EQUITY,
        "EQUITY_FNO": AssetClass.EQUITY_FNO,
        "CRYPTO_FUTURES": AssetClass.CRYPTO_FUTURES,
    }

    def __init__(self, db: Database):
        self._db = db
        # In-memory caches (populated after sync)
        self._by_symbol: dict[str, Instrument] = {}       # "angel_one:RELIANCE" → Instrument
        self._by_token: dict[str, Instrument] = {}        # "12345" → Instrument

    # -------------------------------------------------------------------------
    # Config Loading
    # -------------------------------------------------------------------------

    @staticmethod
    def load_from_yaml(config_path: Path | None = None) -> list[Instrument]:
        """Parse instruments.yaml and return a list of Instrument models.

        Args:
            config_path: Path to instruments.yaml. Defaults to config/instruments.yaml.

        Returns:
            List of Instrument objects.
        """
        config_path = config_path or INSTRUMENTS_CONFIG

        if not config_path.exists():
            log.warning("instruments_config_not_found", path=str(config_path))
            return []

        with open(config_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        instruments: list[Instrument] = []

        for _section_key, section in raw.items():
            exchange_str = section.get("exchange", "")
            asset_class_str = section.get("asset_class", "")

            exchange = InstrumentMetadataManager._EXCHANGE_MAP.get(exchange_str)
            asset_class = InstrumentMetadataManager._ASSET_CLASS_MAP.get(asset_class_str)

            if not exchange or not asset_class:
                log.warning(
                    "instruments_config_unknown_exchange_or_class",
                    exchange=exchange_str,
                    asset_class=asset_class_str,
                )
                continue

            for item in section.get("symbols", []):
                instruments.append(
                    Instrument(
                        symbol=item["symbol"],
                        exchange=exchange,
                        asset_class=asset_class,
                        name=item.get("name", ""),
                    )
                )

        log.info("instruments_loaded_from_config", count=len(instruments))
        return instruments

    # -------------------------------------------------------------------------
    # Database Sync
    # -------------------------------------------------------------------------

    async def sync_to_db(self, instruments: list[Instrument] | None = None) -> int:
        """Upsert instrument metadata into the database.

        If no instruments are provided, loads from config/instruments.yaml.

        Args:
            instruments: Optional list of Instrument objects.

        Returns:
            Number of instruments upserted.
        """
        if instruments is None:
            instruments = self.load_from_yaml()

        if not instruments:
            log.warning("no_instruments_to_sync")
            return 0

        query = """
            INSERT INTO instruments (symbol, exchange, asset_class, name, lot_size, tick_size,
                                     expiry, is_active, exchange_token, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (symbol, exchange)
            DO UPDATE SET
                asset_class    = EXCLUDED.asset_class,
                name           = EXCLUDED.name,
                lot_size       = EXCLUDED.lot_size,
                tick_size       = EXCLUDED.tick_size,
                expiry         = EXCLUDED.expiry,
                is_active      = EXCLUDED.is_active,
                exchange_token = COALESCE(EXCLUDED.exchange_token, instruments.exchange_token),
                metadata       = instruments.metadata || EXCLUDED.metadata
        """

        args_list = []
        for inst in instruments:
            import json
            args_list.append((
                inst.symbol,
                inst.exchange.value,
                inst.asset_class.value,
                inst.name,
                inst.lot_size,
                inst.tick_size,
                inst.expiry,
                inst.is_active,
                inst.exchange_token,
                json.dumps(inst.metadata),
            ))

        try:
            await self._db.execute_many(query, args_list)
            log.info("instruments_synced_to_db", count=len(instruments))

            # Refresh in-memory cache
            await self.refresh_cache()
            return len(instruments)

        except asyncpg.PostgresError as e:
            raise DatabaseWriteError(
                f"Failed to sync {len(instruments)} instruments: {e}",
                details={"error": str(e)},
            ) from e

    # -------------------------------------------------------------------------
    # Token Updates
    # -------------------------------------------------------------------------

    async def update_exchange_token(
        self,
        symbol: str,
        exchange: Exchange,
        token: str,
        extra_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Update the exchange-specific token for an instrument.

        Angel One uses numeric instrument tokens for their WebSocket API.
        This method stores them after downloading the master contract file.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            token: Exchange-specific instrument token/ID.
            extra_metadata: Additional fields to merge into metadata JSONB.
        """
        import json

        if extra_metadata:
            query = """
                UPDATE instruments
                SET exchange_token = $1, metadata = metadata || $4::jsonb
                WHERE symbol = $2 AND exchange = $3
            """
            await self._db.execute(
                query, token, symbol, exchange.value, json.dumps(extra_metadata)
            )
        else:
            query = """
                UPDATE instruments
                SET exchange_token = $1
                WHERE symbol = $2 AND exchange = $3
            """
            await self._db.execute(query, token, symbol, exchange.value)

        log.debug(
            "exchange_token_updated",
            symbol=symbol,
            exchange=exchange.value,
            token=token,
        )

    async def bulk_update_tokens(
        self,
        token_map: dict[str, str],
        exchange: Exchange,
    ) -> int:
        """Bulk update exchange tokens for multiple instruments.

        Args:
            token_map: Dict of {symbol: token}.
            exchange: Exchange these tokens belong to.

        Returns:
            Number of instruments updated.
        """
        query = """
            UPDATE instruments
            SET exchange_token = $1
            WHERE symbol = $2 AND exchange = $3
        """
        args_list = [
            (token, symbol, exchange.value)
            for symbol, token in token_map.items()
        ]

        try:
            await self._db.execute_many(query, args_list)
            log.info(
                "exchange_tokens_bulk_updated",
                count=len(token_map),
                exchange=exchange.value,
            )
            await self.refresh_cache()
            return len(token_map)
        except asyncpg.PostgresError as e:
            raise DatabaseWriteError(
                f"Failed to bulk update tokens: {e}",
            ) from e

    # -------------------------------------------------------------------------
    # Lookups
    # -------------------------------------------------------------------------

    async def refresh_cache(self) -> None:
        """Reload all instruments from DB into in-memory caches."""
        rows = await self._db.fetch(
            "SELECT * FROM instruments WHERE is_active = TRUE ORDER BY symbol"
        )
        self._by_symbol.clear()
        self._by_token.clear()

        for row in rows:
            inst = Instrument(
                symbol=row["symbol"],
                exchange=Exchange(row["exchange"]),
                asset_class=AssetClass(row["asset_class"]),
                name=row["name"],
                lot_size=row["lot_size"],
                tick_size=row["tick_size"],
                expiry=row["expiry"],
                is_active=row["is_active"],
                exchange_token=row["exchange_token"],
                metadata=row["metadata"] if isinstance(row["metadata"], dict) else {},
            )
            self._by_symbol[inst.unique_key] = inst
            if inst.exchange_token:
                self._by_token[inst.exchange_token] = inst

        log.debug("instrument_cache_refreshed", count=len(self._by_symbol))

    def get_by_symbol(self, symbol: str, exchange: Exchange) -> Instrument | None:
        """Look up an instrument by symbol and exchange (from cache).

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.

        Returns:
            Instrument or None if not found.
        """
        return self._by_symbol.get(f"{exchange.value}:{symbol}")

    def get_by_token(self, token: str) -> Instrument | None:
        """Look up an instrument by its exchange token (from cache).

        Useful for resolving Angel One WebSocket messages which use numeric tokens.

        Args:
            token: Exchange-specific instrument token.

        Returns:
            Instrument or None if not found.
        """
        return self._by_token.get(token)

    def get_all(self, exchange: Exchange | None = None) -> list[Instrument]:
        """Get all cached instruments, optionally filtered by exchange.

        Args:
            exchange: Optional exchange filter.

        Returns:
            List of Instrument objects.
        """
        instruments = list(self._by_symbol.values())
        if exchange:
            instruments = [i for i in instruments if i.exchange == exchange]
        return instruments

    def get_symbols(self, exchange: Exchange | None = None) -> list[str]:
        """Get all symbol strings, optionally filtered by exchange.

        Args:
            exchange: Optional exchange filter.

        Returns:
            List of symbol strings.
        """
        return [i.symbol for i in self.get_all(exchange)]

    async def get_from_db(
        self,
        symbol: str,
        exchange: Exchange,
    ) -> Instrument | None:
        """Fetch a single instrument directly from the database (bypassing cache).

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.

        Returns:
            Instrument or None.
        """
        try:
            row = await self._db.fetchrow(
                "SELECT * FROM instruments WHERE symbol = $1 AND exchange = $2",
                symbol,
                exchange.value,
            )
            if not row:
                return None

            return Instrument(
                symbol=row["symbol"],
                exchange=Exchange(row["exchange"]),
                asset_class=AssetClass(row["asset_class"]),
                name=row["name"],
                lot_size=row["lot_size"],
                tick_size=row["tick_size"],
                expiry=row["expiry"],
                is_active=row["is_active"],
                exchange_token=row["exchange_token"],
                metadata=row["metadata"] if isinstance(row["metadata"], dict) else {},
            )
        except asyncpg.PostgresError as e:
            raise DatabaseReadError(
                f"Failed to fetch instrument {symbol}: {e}",
            ) from e

    async def deactivate(self, symbol: str, exchange: Exchange) -> None:
        """Mark an instrument as inactive (soft delete).

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
        """
        await self._db.execute(
            "UPDATE instruments SET is_active = FALSE WHERE symbol = $1 AND exchange = $2",
            symbol,
            exchange.value,
        )
        log.info(
            "instrument_deactivated",
            symbol=symbol,
            exchange=exchange.value,
        )
        # Remove from cache
        key = f"{exchange.value}:{symbol}"
        self._by_symbol.pop(key, None)
