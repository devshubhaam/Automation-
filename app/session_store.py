"""Persistent Telegram session storage backed by MongoDB."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient

logger = logging.getLogger("app.session_store")

__all__ = ["MongoSessionStore"]


class MongoSessionStore:
    """Store Telethon StringSession values in MongoDB."""

    def __init__(
        self,
        uri: str,
        database: str = "telegram_media_processor",
        collection: str = "sessions",
    ) -> None:
        self._client = MongoClient(
            uri,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
        )

        self._collection = (
            self._client[database][collection]
        )

    def ping(self) -> None:
        """Verify that MongoDB is reachable."""
        self._client.admin.command("ping")

    def load(
        self,
        session_name: str,
    ) -> Optional[str]:
        """Load a saved StringSession."""
        document = self._collection.find_one(
            {"_id": session_name},
            {"session": 1},
        )

        if not document:
            return None

        value = document.get("session")

        return str(value) if value else None

    def save(
        self,
        session_name: str,
        session_string: str,
    ) -> None:
        """Save or replace a StringSession."""
        self._collection.update_one(
            {"_id": session_name},
            {
                "$set": {
                    "session": session_string,
                    "updated_at": datetime.now(
                        timezone.utc
                    ),
                }
            },
            upsert=True,
        )

    def close(self) -> None:
        """Close the MongoDB client."""
        self._client.close()
