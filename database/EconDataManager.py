import logging
from datetime import datetime, timedelta
from typing import Optional

from .DatabaseManager import get_collection

logger = logging.getLogger(__name__)


class EconDataManager:
    """
    Manages all database operations for the economy system,
    including user settings, data resets, and deletions.
    """

    def __init__(self, db_manager_instance):
        self.db = db_manager_instance

    # --- Collection Getters ---

    @property
    def user_settings_collection(self):
        # Collection to store user-specific settings like opt-out status
        return get_collection("Users", "Settings")

    @property
    def user_stats_collection(self):
        return get_collection("Users", "Stats")

    @property
    def achievement_progress_collection(self):
        # Note the consistent typo from the existing codebase
        return get_collection("Users", "AcheievementProgress")

    @property
    def activity_events_collection(self):
        return get_collection("Activity", "Events")
    
    @property
    def guild_settings_collection(self):
        return get_collection("Guilds", "Settings")


    # --- Opt-Out Management ---

    async def get_user_opt_out_status(self, user_id: str, guild_id: str) -> bool:
        """
        Checks if a user has opted out in a specific guild.
        """
        query = {"user_id": user_id, "guild_id": guild_id}
        settings = await self.user_settings_collection.find_one(query)
        return settings.get("opted_out", False) if settings else False

    async def set_user_opt_out(self, user_id: str, guild_id: str, retain_data: bool):
        """
        Sets a user's opt-out status and schedules data deletion if requested.
        """
        now = datetime.utcnow()
        update_data = {
            "opted_out": True,
            "opt_out_timestamp": now,
        }

        if retain_data:
            update_data["data_deletion_date"] = now + timedelta(days=90)
            logger.info(f"User {user_id} opted out in {guild_id}. Data retained for 90 days.")
        else:
            # Mark for immediate deletion and perform it
            update_data["data_deletion_date"] = now
            await self.delete_all_user_data(user_id=user_id, guild_id=guild_id)
            logger.info(f"User {user_id} opted out in {guild_id} and requested immediate data deletion.")

        await self.user_settings_collection.update_one(
            {"user_id": user_id, "guild_id": guild_id},
            {"$set": update_data},
            upsert=True
        )

    async def set_user_opt_in(self, user_id: str, guild_id: str):
        """
        Opts a user back into the system.
        """
        update_data = {
            "$set": {
                "opted_out": False
            },
            "$unset": {
                "opt_out_timestamp": "",
                "data_deletion_date": ""
            }
        }
        await self.user_settings_collection.update_one(
            {"user_id": user_id, "guild_id": guild_id},
            update_data,
            upsert=True
        )
        logger.info(f"User {user_id} opted back into the system in guild {guild_id}.")

    # --- Data Deletion ('Nuke') Operations ---

    async def delete_all_user_data(self, user_id: str, guild_id: Optional[str] = None):
        """
        Completely removes all data for a specific user across all or a specific guild from MongoDB.
        """
        log_msg = f"Initiating Nuke for user {user_id}" + (f" in guild {guild_id}." if guild_id else " across all guilds.")
        logger.warning(log_msg)

        collections_to_clean = [
            self.user_stats_collection,
            self.achievement_progress_collection,
            self.activity_events_collection,
            self.user_settings_collection
        ]

        for collection in collections_to_clean:
            query = {"user_id": user_id}
            if guild_id:
                query["guild_id"] = guild_id
            
            if collection is not None:
                result = await collection.delete_many(query)
                logger.info(f"Deleted {result.deleted_count} documents from {collection.database.name}.{collection.name} for user {user_id}.")
            else:
                logger.warning(f"Could not get a collection to clean for user {user_id}.")
        
        await self._delete_local_user_activity(user_id, guild_id)


    async def delete_all_guild_data(self, guild_id: str):
        """
        Completely removes all data for a specific guild from MongoDB.
        """
        logger.warning(f"Initiating Nuke for all data in guild {guild_id}.")
        
        collections_to_clean = [
            self.user_stats_collection,
            self.achievement_progress_collection,
            self.activity_events_collection,
            self.user_settings_collection,
            self.guild_settings_collection
        ]
        
        for collection in collections_to_clean:
            query = {"guild_id": guild_id}
            if collection is not None:
                result = await collection.delete_many(query)
                logger.info(f"Deleted {result.deleted_count} documents from {collection.database.name}.{collection.name} for guild {guild_id}.")
            else:
                 logger.warning(f"Could not get a collection to clean for guild {guild_id}.")
        
        await self._delete_local_user_activity(guild_id=guild_id)

    async def _delete_local_user_activity(self, user_id: Optional[str] = None, guild_id: Optional[str] = None):
        """
        Deletes user activity from the local SQLite database.
        Placeholder for full implementation.
        """
        # This functionality depends on having access to the local SQLite DB path,
        # which is managed by the activity buffer. A more robust solution would involve
        # the EconDataManager having a way to access this path or by using an event system.
        logger.warning("Placeholder: _delete_local_user_activity is not fully implemented. "
                       "It needs access to the local SQLite database to clear user activity records.")
        # Example of what the implementation could look like:
        # try:
        #     async with aiosqlite.connect(LOCAL_DB_PATH) as db:
        #         if user_id and guild_id:
        #             await db.execute("DELETE FROM user_activities WHERE user_id = ? AND guild_id = ?", (user_id, guild_id))
        #         elif guild_id:
        #             await db.execute("DELETE FROM user_activities WHERE guild_id = ?", (guild_id,))
        #         await db.commit()
        #         logger.info("Successfully deleted records from local activity cache.")
        # except Exception as e:
        #     logger.error(f"Failed to delete from local SQLite DB: {e}")
        pass

    # --- Data Reset Operations ---

    async def reset_user_stats(self, user_id: str, guild_id: str):
        """
        Resets a user's stats (e.g., XP, level) in a guild to default values.
        This only resets the core stats, not all data.
        """
        logger.info(f"Resetting stats for user {user_id} in guild {guild_id}.")
        await self.user_stats_collection.update_one(
            {"user_id": user_id, "guild_id": guild_id},
            {"$set": {"xp": 0, "level": 1, "messages": 0}},  # Reset to default values
            upsert=False  # Don't create a new document if it doesn't exist
        )

    async def reset_user_achievements(self, user_id: str, guild_id: str):
        """
        Resets a user's achievements in a guild by deleting their progress.
        """
        logger.info(f"Resetting achievements for user {user_id} in guild {guild_id}.")
        await self.achievement_progress_collection.delete_many(
            {"user_id": user_id, "guild_id": guild_id}
        )

    async def reset_guild_achievements(self, guild_id: str):
        """
        Resets all achievements for an entire guild by deleting all progress documents.
        """
        logger.info(f"Resetting all achievements for guild {guild_id}.")
        await self.achievement_progress_collection.delete_many(
            {"guild_id": guild_id}
        )

# This assumes db_manager is the initialized instance from DatabaseManager.py
from .DatabaseManager import db_manager
econ_db_manager = EconDataManager(db_manager)