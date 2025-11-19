import time
import asyncio
from loggers.logger_setup import get_logger
from database.DatabaseManager import DatabaseManager, get_collection, DatabaseOperationError

class ActivitySystem:
    """
    Manages and records user activity (messages, voice) for tracking active members.
    """

    def __init__(self, db_manager: DatabaseManager, database_name: str = "Activity"):
        """
        Initialize the ActivitySystem.

        Args:
            db_manager: An instance of the DatabaseManager.
            database_name: The name of the database to use.
        """
        self.db_manager = db_manager
        self.database_name = database_name
        self.collection_name = "LastSeen"
        self.collection = None
        self.logger = get_logger("ActivitySystem")

    async def initialize(self):
        """
        Initializes the system by getting the collection and ensuring indexes are created.
        This should be called after the DatabaseManager is initialized.
        """
        self.logger.info("Initializing ActivitySystem...")
        try:
            self.collection = get_collection(self.database_name, self.collection_name)
            await self._create_indexes()
            self.logger.info("✅ ActivitySystem initialized successfully.")
        except DatabaseOperationError as e:
            self.logger.error(f"❌ Could not get collection '{self.database_name}.{self.collection_name}'. "
                             f"Please ensure the database is running and accessible. Error: {e}", exc_info=True)
            # Depending on desired behavior, you might want to raise this
            # to prevent the bot from starting without this system.
            raise
        except Exception as e:
            self.logger.error(f"❌ An unexpected error occurred during ActivitySystem initialization: {e}", exc_info=True)
            raise

    async def _create_indexes(self):
        """
        Create necessary indexes on the user_activity collection for efficient querying.
        """
        if self.collection is None:
            self.logger.warning("Collection is not initialized, skipping index creation.")
            return

        self.logger.info("Ensuring database indexes are created for user_activity...")
        try:
            # Index for quickly finding a user's activity record
            await self.collection.create_index(
                [("user_id", 1), ("guild_id", 1)],
                name="user_guild_activity_idx",
                unique=True
            )
            # Index for querying by last seen time (for DAU, WAU, etc.)
            await self.collection.create_index(
                [("last_seen_timestamp", -1)],
                name="last_seen_idx"
            )
            self.logger.info("✅ Database indexes for user_activity are up to date.")
        except Exception as e:
            self.logger.error(f"❌ Error creating indexes for user_activity: {e}", exc_info=True)

    async def record_activity(self, user_id: str, guild_id: str, activity_type: str):
        """
        Record a user's activity, updating their last-seen timestamp.

        Args:
            user_id: The ID of the user.
            guild_id: The ID of the guild where the activity occurred.
            activity_type: The type of activity (e.g., 'message', 'voice').
        """
        if self.collection is None:
            self.logger.error("Cannot record activity: collection is not initialized.")
            return

        current_timestamp = int(time.time())

        try:
            await self.collection.update_one(
                {"user_id": user_id, "guild_id": guild_id},
                {
                    "$set": {
                        "last_seen_timestamp": current_timestamp,
                        "activity_type": activity_type,
                    },
                    "$setOnInsert": {
                        "user_id": user_id,
                        "guild_id": guild_id,
                    }
                },
                upsert=True
            )
            self.logger.debug(f"Recorded activity for user {user_id} in guild {guild_id}: {activity_type}")
        except Exception as e:
            self.logger.error(f"❌ Failed to record activity for user {user_id}: {e}", exc_info=True)

