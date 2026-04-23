from motor.motor_asyncio import AsyncIOMotorClient

import config


class Database:
    def __init__(self) -> None:
        self._client = AsyncIOMotorClient(config.MONGO_URI)
        self._db = self._client[config.MONGO_DB]
        self.users = self._db["users"]

    async def add_user(self, user_id: int) -> bool:
        """
        Insert the user if they don't exist yet.
        Returns True if this is a new user, False if already known.
        """
        existing = await self.users.find_one({"_id": user_id})
        if existing:
            return False
        await self.users.insert_one({"_id": user_id})
        return True

    async def user_count(self) -> int:
        """Return total number of unique users."""
        return await self.users.count_documents({})

    async def all_user_ids(self) -> list[int]:
        """Return a list of all stored user IDs."""
        cursor = self.users.find({}, {"_id": 1})
        return [doc["_id"] async for doc in cursor]


# Singleton instance imported by other modules
db = Database()
